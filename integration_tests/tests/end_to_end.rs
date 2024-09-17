use std::panic::AssertUnwindSafe;
use std::sync::LazyLock;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use bb8_postgres::PostgresConnectionManager;
use chrono::{DateTime, Duration, DurationRound, TimeDelta, TimeZone, Utc};
use futures::{Future, FutureExt};
use test_case::test_case;
use tokio::sync::mpsc;
use tokio_postgres::NoTls;

use lard_api::timeseries::Timeseries;
use lard_api::{LatestResp, TimeseriesResp, TimesliceResp};
use lard_ingestion::kvkafka;
use lard_ingestion::permissions::{
    timeseries_is_open, ParamPermit, ParamPermitTable, StationPermitTable,
};
use lard_ingestion::KldataResp;

const CONNECT_STRING: &str = "host=localhost user=postgres dbname=postgres password=postgres";
const PARAMCONV_CSV: &str = "../ingestion/resources/paramconversions.csv";

static PARAMATERS: LazyLock<HashMap<String, (i32, ObsType)>> = LazyLock::new(|| {
    csv::Reader::from_path(PARAMCONV_CSV)
        .unwrap()
        .into_records()
        .map(|record_result| {
            let record = record_result.unwrap();
            (
                record.get(1).unwrap().to_owned(),
                (
                    record.get(0).unwrap().parse::<i32>().unwrap(),
                    match record.get(3).unwrap() {
                        "t" => ObsType::Scalar,
                        "f" => ObsType::NonScalar,
                        _ => unreachable!(),
                    },
                ),
            )
        })
        .collect()
});

#[derive(Clone, Copy)]
enum ObsType {
    Scalar,
    NonScalar,
}

// TODO: could probably have a LazyLock read-only hashmap that loads the paramconversions.csv instead of
// typing all params manually
#[derive(Clone)]
struct Param<'a> {
    id: i32,
    code: &'a str,
    sensor_level: Option<(i32, i32)>,
    obstype: ObsType,
}

impl<'a> Param<'a> {
    fn new(code: &str) -> Self {
        let (code, (id, obstype)) = PARAMATERS
            .get_key_value(code)
            .expect("Provided param code should be found in global hashmap");

        Self {
            id: *id,
            code,
            sensor_level: None,
            obstype: *obstype,
        }
    }

    fn with_sensor_level(code: &str, sensor_level: (i32, i32)) -> Self {
        let (code, (id, obstype)) = PARAMATERS
            .get_key_value(code)
            .expect("Provided param code should be found in global hashmap");

        Self {
            id: *id,
            code,
            sensor_level: Some(sensor_level),
            obstype: *obstype,
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
        let scalar_val = 0.0;
        let nonscalar_val = "test";

        let values = self
            .params
            .iter()
            .map(|param| match param.obstype {
                ObsType::Scalar => scalar_val.to_string(),
                ObsType::NonScalar => nonscalar_val.to_string(),
            })
            .collect::<Vec<String>>()
            .join(",");

        let mut msg = vec![self.obsinn_header(), self.param_header()];

        let end_time = self.end_time();
        let mut time = self.start_time;
        while time < end_time {
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

fn mock_permit_tables() -> Arc<RwLock<(ParamPermitTable, StationPermitTable)>> {
    let param_permit = HashMap::from([
        // station_id -> (type_id, param_id, permit_id)
        (10000, vec![ParamPermit::new(0, 0, 0)]),
        (10001, vec![ParamPermit::new(0, 0, 1)]), // open
    ]);

    let station_permit = HashMap::from([
        // station_id -> permit_id
        (10000, 1), // overridden by param_permit
        (10001, 0), // overridden by param_permit
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

async fn cleanup(client: &tokio_postgres::Client) {
    client
        .batch_execute(
            // TODO: should clean public.timeseries_id_seq too? RESTART IDENTITY CASCADE?
            "TRUNCATE public.timeseries, labels.met, labels.obsinn CASCADE",
        )
        .await
        .unwrap();
}

async fn e2e_test_wrapper<T: Future<Output = ()>>(test: T) {
    let manager = PostgresConnectionManager::new_from_stringlike(CONNECT_STRING, NoTls).unwrap();
    let db_pool = bb8::Pool::builder().build(manager).await.unwrap();

    let api_server = tokio::spawn(lard_api::run(db_pool.clone()));
    let ingestor = tokio::spawn(lard_ingestion::run(
        db_pool.clone(),
        PARAMCONV_CSV,
        mock_permit_tables(),
    ));

    tokio::select! {
        _ = api_server => panic!("API server task terminated first"),
        _ = ingestor => panic!("Ingestor server task terminated first"),
        // Clean up database even if test panics, to avoid test poisoning
        test_result = AssertUnwindSafe(test).catch_unwind() => {
            // For debugging a specific test, it might be useful to skip cleaning up
            #[cfg(not(feature = "debug"))]
            {
                let client = db_pool.get().await.unwrap();
                cleanup(&client).await;
            }
            assert!(test_result.is_ok())
        }
    }
}

async fn ingest_data(client: &reqwest::Client, obsinn_msg: String) -> KldataResp {
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
            params: &[Param::new("TGM"), Param::new("TGX")],
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

            let json: TimeseriesResp = resp.json().await.unwrap();
            assert_eq!(json.tseries.len(), 1);

            let Timeseries::Irregular(series) = &json.tseries[0] else {
                panic!("Expected irrregular timeseries")
            };

            assert_eq!(series.data.len(), ts.len);
        }
    })
    .await
}

#[test_case(
    TestData {
        station_id: 20001,
        params: &[Param::new("TA"), Param::new("TGX")],
        start_time: Utc::now().duration_trunc(TimeDelta::hours(1)).unwrap()
            - Duration::hours(11),
        period: Duration::hours(1),
        type_id: 501,
        len: 12,
    }; "Scalar params")
]
// TODO: probably write a separate test, so we can check actual sensor and level
#[test_case(
    TestData {
        station_id: 20001,
        params: &[Param::with_sensor_level("TA", (1, 1)), Param::new("TGX")],
        start_time: Utc::now().duration_trunc(TimeDelta::hours(1)).unwrap()
            - Duration::hours(11),
        period: Duration::hours(1),
        type_id: 501,
        len: 12,
    }; "With sensor and level")
]
#[test_case(
    TestData {
        station_id: 20001,
        params: &[Param::new("TA"), Param::new("KLOBS")],
        start_time: Utc::now().duration_trunc(TimeDelta::hours(1)).unwrap()
            - Duration::hours(11),
        period: Duration::hours(1),
        type_id: 501,
        len: 12,
    }; "Scalar and non-scalar")
]
#[tokio::test]
async fn test_stations_endpoint_regular(ts: TestData<'_>) {
    e2e_test_wrapper(async {
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

            let json: TimeseriesResp = resp.json().await.unwrap();
            assert_eq!(json.tseries.len(), 1);

            let Timeseries::Regular(series) = &json.tseries[0] else {
                panic!("Expected regular timeseries")
            };
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
            params: &[Param::new("TA")],
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
                params: &[Param::new("TA"), Param::new("TGX")],
                start_time: Utc::now().duration_trunc(TimeDelta::minutes(1)).unwrap()
                    - Duration::hours(3),
                period: Duration::minutes(1),
                type_id: 508,
                len: 180,
            },
            TestData {
                station_id: 20002,
                params: &[Param::new("TA"), Param::new("TGX")],
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

        let json: LatestResp = resp.json().await.unwrap();
        assert_eq!(json.data.len(), expected_len);
    })
    .await
}

#[tokio::test]
async fn test_timeslice_endpoint() {
    e2e_test_wrapper(async {
        let timestamp = Utc.with_ymd_and_hms(2024, 1, 1, 1, 0, 0).unwrap();
        let params = vec![Param::new("TA")];

        let test_data = [
            TestData {
                station_id: 20001,
                params: &params.clone(),
                start_time: timestamp - Duration::hours(1),
                period: Duration::hours(1),
                type_id: 501,
                len: 2,
            },
            TestData {
                station_id: 20002,
                params: &params.clone(),
                start_time: timestamp - Duration::hours(1),
                period: Duration::minutes(1),
                type_id: 508,
                len: 120,
            },
        ];

        let client = reqwest::Client::new();
        for ts in &test_data {
            let ingestor_resp = ingest_data(&client, ts.obsinn_message()).await;
            assert_eq!(ingestor_resp.res, 0);
        }

        for param in &params {
            let url = format!(
                "http://localhost:3000/timeslices/{}/params/{}",
                timestamp, param.id
            );

            let resp = reqwest::get(url).await.unwrap();
            assert!(resp.status().is_success());

            let json: TimesliceResp = resp.json().await.unwrap();
            assert!(json.tslices.len() == 1);

            let slice = &json.tslices[0];
            assert_eq!(slice.param_id, param.id);
            assert_eq!(slice.timestamp, timestamp);
            assert_eq!(slice.data.len(), test_data.len());

            for (data, ts) in slice.data.iter().zip(&test_data) {
                assert_eq!(data.station_id, ts.station_id);
            }
        }
    })
    .await
}

#[tokio::test]
async fn test_kafka() {
    e2e_test_wrapper(async {
        let (tx, mut rx) = mpsc::channel(10);

        let (pgclient, conn) = tokio_postgres::connect(CONNECT_STRING, NoTls)
            .await
            .unwrap();

        tokio::spawn(async move {
            if let Err(e) = conn.await {
                eprintln!("{}", e)
            }
        });

        // Spawn task to send message
        tokio::spawn(async move {
            let ts = TestData {
                station_id: 20001,
                params: &[Param::new(106, "RR_1")], // sum(precipitation_amount PT1H)
                start_time: Utc.with_ymd_and_hms(2024, 6, 5, 12, 0, 0).unwrap(),
                period: chrono::Duration::hours(1),
                type_id: -4,
                len: 24,
            };

            let client = reqwest::Client::new();
            let ingestor_resp = ingest_data(&client, ts.obsinn_message()).await;
            assert_eq!(ingestor_resp.res, 0);

            // This observation was 2.5 hours late??
            let kafka_xml = r#"<?xml?>
            <KvalobsData producer=\"kvqabase\" created=\"2024-06-06 08:30:43\">
                <station val=\"20001\">
                    <typeid val=\"-4\">
                        <obstime val=\"2024-06-06 06:00:00\">
                            <tbtime val=\"2024-06-06 08:30:42.943247\">
                                <sensor val=\"0\">
                                    <level val=\"0\">
                                        <kvdata paramid=\"106\">
                                            <original>10</original>
                                            <corrected>10</corrected>
                                            <controlinfo>1000000000000000</controlinfo>
                                            <useinfo>9000000000000000</useinfo>
                                            <cfailed></cfailed>
                                        </kvdata>
                                    </level>
                                </sensor>
                            </tbtime>
                        </obstime>
                    </typeid>
                </station>
            </KvalobsData>"#;

            kvkafka::parse_message(kafka_xml.as_bytes(), &tx)
                .await
                .unwrap();
        });

        //  wait for message
        if let Some(msg) = rx.recv().await {
            kvkafka::insert_kvdata(&pgclient, msg).await.unwrap()
        }

        // TODO: we do not have an API endpoint to query the flags.kvdata table
        assert!(pgclient
            .query_one("SELECT * FROM flags.kvdata", &[])
            .await
            .is_ok());
    })
    .await
}
