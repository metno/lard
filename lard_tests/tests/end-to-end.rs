use std::future::Future;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use chrono::{DateTime, Duration, DurationRound, TimeDelta, TimeZone, Utc};
use serde::Deserialize;
use tokio_postgres::NoTls;

use lard_ingestion::permissions::{ParamPermit, ParamPermitTable, StationPermitTable};

const CONNECT_STRING: &str = "host=localhost user=postgres dbname=postgres password=postgres";
const PARAMCONV_CSV: &str = "../ingestion/resources/paramconversions.csv";

#[derive(Debug, Deserialize)]
pub struct IngestorResponse {
    pub message: String,
    pub message_id: usize,
    pub res: u8,
    pub retry: bool,
}

#[derive(Debug, Deserialize)]
pub struct StationsResponse {
    pub tseries: Vec<Tseries>,
}

#[derive(Debug, Deserialize)]
pub struct Tseries {
    pub regularity: String,
    pub data: Vec<f64>,
    // header: ...
}

#[derive(Debug, Deserialize)]
pub struct LatestResponse {
    pub data: Vec<LatestData>,
}

#[derive(Debug, Deserialize)]
pub struct LatestData {
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
    pub data: Vec<f64>,
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
    params: Vec<Param<'a>>,
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

        let mut msg = vec![self.obsinn_header()];

        let mut time = self.start_time;
        while time < self.end_time() {
            msg.push(format!("{},{}", time.format("%Y%m%d%H%M%S"), values));
            time += self.period;
        }

        msg.join("\n")
    }

    fn obsinn_header(&self) -> String {
        format!(
            "kldata/nationalnr={}/type={}/messageid=23\n{}",
            self.station_id,
            self.type_id,
            self.param_header()
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
        (1, vec![ParamPermit::new(0, 0, 0)]),
        (2, vec![ParamPermit::new(0, 0, 1)]), // open
    ]);

    #[rustfmt::skip]
    let station_permit = HashMap::from([
        // station_id -> permit_id ... 1 = open, everything else is closed
        (20000, 1), (30000, 1), (11000, 1), (12000, 1),
        (12100, 1), (13000, 1), (40000, 1), (50000, 1),
    ]);

    Arc::new(RwLock::new((param_permit, station_permit)))
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
        .execute(
            // TODO: public.timeseries_id_seq? RESTART IDENTITY CASCADE?
            "TRUNCATE public.timeseries, labels.met, labels.obsinn CASCADE",
            &[],
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
        _ = api_server => {panic!("API server task terminated first")},
        _ = ingestor => {panic!("Ingestor server task terminated first")},
        _ = test => {cleanup().await}
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
            station_id: 11000,
            params: vec![Param::new(222, "TGM")], // mean(grass_temperature)
            start_time: Utc.with_ymd_and_hms(2012, 2, 14, 0, 0, 0).unwrap(),
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

            let resp: StationsResponse = resp.json().await.unwrap();
            assert_eq!(resp.tseries.len(), 1);

            let series = &resp.tseries[0];
            assert_eq!(series.regularity, "Irregular");
            assert_eq!(series.data.len(), ts.len);
            println!("{:?}", series.data)
        }
    })
    .await
}

#[tokio::test]
async fn test_stations_endpoint_regular() {
    e2e_test_wrapper(async {
        let ts = TestData {
            station_id: 30000,
            params: vec![Param::new(211, "TA")],
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

            let resp: StationsResponse = resp.json().await.unwrap();
            assert_eq!(resp.tseries.len(), 1);

            let series = &resp.tseries[0];
            println!("{:?}", series);
            assert_eq!(series.regularity, "Regular");
            assert_eq!(series.data.len(), ts.len);
        }
    })
    .await
}

#[tokio::test]
async fn test_latest_endpoint() {
    e2e_test_wrapper(async {
        let ts = TestData {
            station_id: 20000,
            params: vec![Param::new(211, "TA"), Param::new(255, "TGX")],
            start_time: Utc::now().duration_trunc(TimeDelta::minutes(1)).unwrap()
                - Duration::minutes(179),
            period: Duration::minutes(1),
            type_id: 508,
            len: 180,
        };

        let client = reqwest::Client::new();
        let ingestor_resp = ingest_data(&client, ts.obsinn_message()).await;
        assert_eq!(ingestor_resp.res, 0);

        let url = "http://localhost:3000/latest";
        // let expected_data_len = 180;

        let resp = reqwest::get(url).await.unwrap();
        assert!(resp.status().is_success());

        let json: LatestResponse = resp.json().await.unwrap();
        println!("{:?}", json);
        // assert_eq!(json.data.len(), expected_data_len);
    })
    .await
}

// #[test_case(40000; "new_station")]
// #[test_case(11000; "old_station")]
// #[tokio::test]
async fn create_timeseries(station_id: i32) {
    e2e_test_wrapper(async move {
        // 145, AGM, mean(air_gap PT10M)
        // 146, AGX, max(air_gap PT10M)
        let param_ids = [145, 146];
        let expected_len = 5;
        let obsinn_msg = format!(
            concat!(
                "kldata/nationalnr={}/type=506/messageid=23\n",
                "AGM(1,2),AGX(1,2)\n",
                "20240606000000,11.0,12.0\n",
                "20240606001000,12.0,19.0\n",
                "20240606002000,10.0,16.0\n",
                "20240606003000,12.0,16.0\n",
                "20240606004000,11.0,15.0",
            ),
            station_id
        );

        let client = reqwest::Client::new();
        let resp = client
            .post("http://localhost:3001/kldata")
            .body(obsinn_msg)
            .send()
            .await
            .unwrap();
        let json: IngestorResponse = resp.json().await.unwrap();
        assert_eq!(json.res, 0);

        for id in param_ids {
            let url = format!(
                "http://localhost:3000/stations/{}/params/{}",
                station_id, id
            );

            let resp = client.get(&url).send().await.unwrap();
            assert!(resp.status().is_success());

            let json: StationsResponse = resp.json().await.unwrap();
            assert_eq!(json.tseries[0].data.len(), expected_len)
        }
    })
    .await
}
