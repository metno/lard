use std::panic::AssertUnwindSafe;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use chrono::{DateTime, Duration, DurationRound, TimeDelta, TimeZone, Utc};
use futures::{Future, FutureExt};
use serde::Deserialize;
use test_case::test_case;
use tokio_postgres::NoTls;

use lard_ingestion::permissions::{
    timeseries_is_open, ParamPermit, ParamPermitTable, StationPermitTable,
};

const CONNECT_STRING: &str = "host=localhost user=postgres dbname=postgres password=postgres";
const PARAMCONV_CSV: &str = "../ingestion/resources/paramconversions.csv";

// TODO: should directly use the structs already defined in the different packages?
#[derive(Debug, Deserialize)]
pub struct IngestorResponse {
    pub message: String,
    pub message_id: usize,
    pub res: u8,
    pub retry: bool,
}

#[derive(Debug, Deserialize)]
pub struct StationsResponse {
    pub tseries: Vec<StationElem>,
}

#[derive(Debug, Deserialize)]
pub struct StationElem {
    pub regularity: String,
    pub data: Vec<f64>,
    // header: ...
}

#[derive(Debug, Deserialize)]
pub struct LatestResponse {
    pub data: Vec<LatestElem>,
}

#[derive(Debug, Deserialize)]
pub struct LatestElem {
    // TODO: Missing param_id here?
    pub value: f64,
    pub timestamp: DateTime<Utc>,
    pub station_id: i32,
    // loc: {lat, lon, hamsl, hag}
}

#[derive(Debug, Deserialize)]
pub struct TimesliceResponse {
    pub tslices: Vec<Tslice>,
}

#[derive(Debug, Deserialize)]
pub struct Tslice {
    pub timestamp: DateTime<Utc>,
    pub param_id: i32,
    pub data: Vec<SliceElem>,
}

#[derive(Debug, Deserialize)]
pub struct SliceElem {
    pub value: f64,
    pub station_id: i32,
    // loc: {lat, lon, hamsl, hag}
}

struct Param<'a> {
    id: i32,
    code: &'a str,
    sensor_level: Option<(i32, i32)>,
}

impl<'a> Param<'a> {
    fn new(id: i32, code: &'a str) -> Param {
        Param {
            id,
            code,
            sensor_level: None,
        }
    }
}

struct TestData<'a> {
    station_id: i32,
    type_id: i32,
    params: &'a [Param<'a>],
    start_time: DateTime<Utc>,
    period: Duration,
    len: usize,
}

impl<'a> TestData<'a> {
    // Creates a message with the following format:
    // ```
    // kldata/nationalnr=99999/type=501/messageid=23
    // param_1,param_2(0,0),...
    // 20240101000000,0.0,0.0,...
    // 20240101010000,0.0,0.0,...
    // ...
    // ```
    fn obsinn_message(&self) -> String {
        // TODO: assign different values?
        let val = 0.0;
        let values = vec![val.to_string(); self.params.len()].join(",");

        let mut msg = vec![self.obsinn_header(), self.param_header()];

        let mut time = self.start_time;
        while time < self.end_time() {
            msg.push(format!("{},{}", time.format("%Y%m%d%H%M%S"), values));
            time += self.period;
        }

        msg.join("\n")
    }

    fn obsinn_header(&self) -> String {
        format!(
            "kldata/nationalnr={}/type={}/messageid=23",
            self.station_id, self.type_id,
        )
    }

    fn param_header(&self) -> String {
        self.params
            .iter()
            .map(|param| match param.sensor_level {
                Some((sensor, level)) => format!("{}({},{})", param.code, sensor, level),
                None => param.code.to_string(),
            })
            .collect::<Vec<_>>()
            .join(",")
    }

    fn end_time(&self) -> DateTime<Utc> {
        self.start_time + self.period * self.len as i32
    }
}

pub fn mock_permit_tables() -> Arc<RwLock<(ParamPermitTable, StationPermitTable)>> {
    let param_permit = HashMap::from([
        // station_id -> (type_id, param_id, permit_id)
        (10000, vec![ParamPermit::new(0, 0, 0)]),
        (10001, vec![ParamPermit::new(0, 0, 1)]), // open
    ]);

    let station_permit = HashMap::from([
        // station_id -> permit_id
        (10000, 1), // potentially overidden by param_permit
        (10001, 0), // potentially overidden by param_permit
        (20000, 0),
        (20001, 1),
        (20002, 1),
    ]);

    Arc::new(RwLock::new((param_permit, station_permit)))
}

#[test_case(0, 0, 0 => false; "stationid not in permit_tables")]
#[test_case(10000, 0, 0 => false; "stationid in ParamPermitTable, timeseries closed")]
#[test_case(10001, 0, 0 => true; "stationid in ParamPermitTable, timeseries open")]
#[test_case(20000, 0, 0 => false; "stationid in StationPermitTable, timeseries closed")]
#[test_case(20001, 0, 1 => true; "stationid in StationPermitTable, timeseries open")]
fn test_timeseries_is_open(station_id: i32, type_id: i32, permit_id: i32) -> bool {
    let permit_tables = mock_permit_tables();
    timeseries_is_open(permit_tables, station_id, type_id, permit_id).unwrap()
}

pub async fn cleanup() {
    let (client, conn) = tokio_postgres::connect(CONNECT_STRING, NoTls)
        .await
        .unwrap();

    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("{}", e);
        }
    });

    client
        .batch_execute(
            // TODO: should clean public.timeseries_id_seq too? RESTART IDENTITY CASCADE?
            "TRUNCATE public.timeseries, labels.met, labels.obsinn CASCADE",
        )
        .await
        .unwrap();
}

async fn e2e_test_wrapper<T: Future<Output = ()>>(test: T) {
    let api_server = tokio::spawn(lard_api::run(CONNECT_STRING));
    let ingestor = tokio::spawn(lard_ingestion::run(
        CONNECT_STRING,
        PARAMCONV_CSV,
        mock_permit_tables(),
    ));

    tokio::select! {
        _ = api_server => { panic!("API server task terminated first") },
        _ = ingestor => { panic!("Ingestor server task terminated first") },
        // Clean up database even if test panics, to avoid test poisoning
        test_result = AssertUnwindSafe(test).catch_unwind() => {
            cleanup().await;
            assert!(test_result.is_ok())
        }
    }
}

async fn ingest_data(client: &reqwest::Client, obsinn_msg: String) -> IngestorResponse {
    let resp = client
        .post("http://localhost:3001/kldata")
        .body(obsinn_msg)
        .send()
        .await
        .unwrap();

    resp.json().await.unwrap()
}

#[tokio::test]
async fn test_stations_endpoint_irregular() {
    e2e_test_wrapper(async {
        let ts = TestData {
            station_id: 20001,
            params: &[Param::new(222, "TGM"), Param::new(225, "TGX")],
            start_time: Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
            period: Duration::hours(1),
            type_id: 501,
            len: 48,
        };

        let client = reqwest::Client::new();
        let ingestor_resp = ingest_data(&client, ts.obsinn_message()).await;
        assert_eq!(ingestor_resp.res, 0);

        for param in ts.params {
            let url = format!(
                "http://localhost:3000/stations/{}/params/{}",
                ts.station_id, param.id
            );
            let resp = reqwest::get(url).await.unwrap();
            assert!(resp.status().is_success());

            let json: StationsResponse = resp.json().await.unwrap();
            assert_eq!(json.tseries.len(), 1);

            let series = &json.tseries[0];
            assert_eq!(series.regularity, "Irregular");
            assert_eq!(series.data.len(), ts.len);
        }
    })
    .await
}

#[tokio::test]
async fn test_stations_endpoint_regular() {
    e2e_test_wrapper(async {
        let ts = TestData {
            station_id: 20001,
            params: &[Param::new(211, "TA"), Param::new(225, "TGX")],
            start_time: Utc::now().duration_trunc(TimeDelta::hours(1)).unwrap()
                - Duration::hours(11),
            period: Duration::hours(1),
            type_id: 501,
            len: 12,
        };

        let client = reqwest::Client::new();
        let ingestor_resp = ingest_data(&client, ts.obsinn_message()).await;
        assert_eq!(ingestor_resp.res, 0);

        let resolution = "PT1H";
        for param in ts.params {
            let url = format!(
                "http://localhost:3000/stations/{}/params/{}?time_resolution={}",
                ts.station_id, param.id, resolution
            );
            let resp = reqwest::get(url).await.unwrap();
            assert!(resp.status().is_success());

            let json: StationsResponse = resp.json().await.unwrap();
            assert_eq!(json.tseries.len(), 1);

            let series = &json.tseries[0];
            assert_eq!(series.regularity, "Regular");
            assert_eq!(series.data.len(), ts.len);
        }
    })
    .await
}

#[test_case(99999, 211; "missing station")]
#[test_case(20001, 999; "missing param")]
#[tokio::test]
async fn test_stations_endpoint_errors(station_id: i32, param_id: i32) {
    e2e_test_wrapper(async {
        let ts = TestData {
            station_id: 20001,
            params: &[Param::new(211, "TA")],
            start_time: Utc.with_ymd_and_hms(2024, 1, 1, 00, 00, 00).unwrap(),
            period: Duration::hours(1),
            type_id: 501,
            len: 48,
        };

        let client = reqwest::Client::new();
        let ingestor_resp = ingest_data(&client, ts.obsinn_message()).await;
        assert_eq!(ingestor_resp.res, 0);

        for _ in ts.params {
            let url = format!(
                "http://localhost:3000/stations/{}/params/{}",
                station_id, param_id
            );
            let resp = reqwest::get(url).await.unwrap();
            // TODO: resp.status() returns 500, maybe it should return 404?
            assert!(!resp.status().is_success());
        }
    })
    .await
}

// We insert 4 timeseries, 2 with new data (UTC::now()) and 2 with old data (2020)
#[test_case("", 2; "without query")]
#[test_case("?latest_max_age=2021-01-01T00:00:00Z", 2; "latest max age 1")]
#[test_case("?latest_max_age=2019-01-01T00:00:00Z", 4; "latest max age 2")]
#[tokio::test]
async fn test_latest_endpoint(query: &str, expected_len: usize) {
    e2e_test_wrapper(async {
        let test_data = [
            TestData {
                station_id: 20001,
                params: &[Param::new(211, "TA"), Param::new(225, "TGX")],
                start_time: Utc::now().duration_trunc(TimeDelta::minutes(1)).unwrap()
                    - Duration::hours(3),
                period: Duration::minutes(1),
                type_id: 508,
                len: 180,
            },
            TestData {
                station_id: 20002,
                params: &[Param::new(211, "TA"), Param::new(225, "TGX")],
                start_time: Utc.with_ymd_and_hms(2020, 1, 1, 0, 0, 0).unwrap(),
                period: Duration::minutes(1),
                type_id: 508,
                len: 180,
            },
        ];

        let client = reqwest::Client::new();
        for ts in test_data {
            let ingestor_resp = ingest_data(&client, ts.obsinn_message()).await;
            assert_eq!(ingestor_resp.res, 0);
        }

        let url = format!("http://localhost:3000/latest{}", query);
        let resp = reqwest::get(url).await.unwrap();
        assert!(resp.status().is_success());

        let json: LatestResponse = resp.json().await.unwrap();
        assert_eq!(json.data.len(), expected_len);
    })
    .await
}

#[tokio::test]
async fn test_timeslice_endpoint() {
    e2e_test_wrapper(async {
        // TODO: test multiple slices, can it take a sequence of timeslices?
        let timestamp = Utc.with_ymd_and_hms(2024, 1, 1, 1, 0, 0).unwrap();
        let param_id = 211;

        let test_data = [
            TestData {
                station_id: 20001,
                params: &[Param::new(211, "TA")],
                start_time: Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
                period: Duration::hours(1),
                type_id: 501,
                len: 2,
            },
            TestData {
                station_id: 20002,
                params: &[Param::new(211, "TA")],
                start_time: Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap(),
                period: Duration::minutes(1),
                type_id: 501,
                len: 120,
            },
        ];

        let client = reqwest::Client::new();
        for ts in &test_data {
            let ingestor_resp = ingest_data(&client, ts.obsinn_message()).await;
            assert_eq!(ingestor_resp.res, 0);
        }

        let url = format!(
            "http://localhost:3000/timeslices/{}/params/{}",
            timestamp, param_id
        );

        let resp = reqwest::get(url).await.unwrap();
        assert!(resp.status().is_success());

        let json: TimesliceResponse = resp.json().await.unwrap();
        assert!(json.tslices.len() == 1);

        let slice = &json.tslices[0];
        assert_eq!(slice.param_id, param_id);
        assert_eq!(slice.timestamp, timestamp);
        assert_eq!(slice.data.len(), test_data.len());

        for (data, ts) in slice.data.iter().zip(&test_data) {
            assert_eq!(data.station_id, ts.station_id);
        }
    })
    .await
}
