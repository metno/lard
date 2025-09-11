use std::{panic::AssertUnwindSafe, time::Instant};

use chrono::{DateTime, Duration, TimeZone, Utc};
use futures::FutureExt;
use rdkafka::producer::{FutureProducer, FutureRecord};
use reqwest::Client;
use reqwest::StatusCode;

use lard_egress::{
    patchwork::PatchworkTables,
    reports::{IdfEvent, IdfEventAvailability, IdfEventResp, DEFAULT_DURATIONS},
    PatchworkResp,
};

use lard_ingestion::get_conversions;
use util::{DbPools, PooledPgConn};

pub mod common;
use common::{Param, TestData};

use crate::common::update_patchwork_table;

const KAFKA_RAW_TOPIC: &str = "raw";
const KAFKA_CHECKED_TOPIC: &str = "checked";
const KAFKA_CHECKED_HIST_TOPIC: &str = "hist.checked";
const KAFKA_GROUP: &str = "lard_test";

/// Similar to e2e_test_wrapper, but adapted to use kvkafka ingestion instead of obsinn.
pub async fn e2e_test_wrapper_legacy(
    test: impl AsyncFnOnce(FutureProducer, DbPools, PatchworkTables) -> (),
) {
    let (db_pools, patchwork_tables, mut egress, cancel_token) = common::wrapper_setup().await;

    let mock_kafka_cluster = rdkafka::mocking::MockCluster::new(3).unwrap();
    mock_kafka_cluster
        .create_topic(KAFKA_RAW_TOPIC, 32, 3)
        .unwrap();
    mock_kafka_cluster
        .create_topic(KAFKA_CHECKED_TOPIC, 32, 3)
        .unwrap();
    let kafka_brokers = mock_kafka_cluster.bootstrap_servers();

    let kafka_producer: FutureProducer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", kafka_brokers.clone())
        .create()
        .unwrap();

    let param_conv_path = std::env::var("PARAMCONV_CSV").unwrap();
    let param_conversions =
        get_conversions(&param_conv_path).expect("failed to load param conversions");

    let (ingestion_pools, ingestion_token) = (db_pools.clone(), cancel_token.clone());
    let mut ingestion = tokio::spawn(lard_ingestion::legacy::run(
        ingestion_pools,
        kafka_brokers,
        KAFKA_GROUP.to_string(),
        KAFKA_RAW_TOPIC,
        KAFKA_CHECKED_TOPIC,
        KAFKA_CHECKED_HIST_TOPIC,
        ingestion_token,
        common::mock_permit_tables(),
        common::mock_level_table(),
        param_conversions,
    ));

    tokio::select! {
        _ = &mut egress => panic!("API server task terminated first"),
        _ = &mut ingestion => panic!("Ingestor server task terminated first"),
        // Clean up database even if test panics, to avoid test poisoning
        test_result = AssertUnwindSafe(test(kafka_producer, db_pools.clone(), patchwork_tables.clone())).catch_unwind() => {
            // For debugging a specific test, it might be useful to skip the cleanup process
            #[cfg(not(feature = "debug"))]
            common::db_cleanup(db_pools.clone()).await;

            assert!(test_result.is_ok())
        }
    }

    cancel_token.cancel();
    let (egress_result, ingestion_result) = tokio::join!(egress, ingestion);
    egress_result.unwrap();
    ingestion_result.unwrap().unwrap();
}

// As we have no way to sync with message processing in kvkafka ingestion, we just keep
// trying to fetch data with a timeout
async fn wait_for_db_readiness(conn: &PooledPgConn<'_>, expected_rows: usize) {
    let timeout = std::time::Duration::from_secs(10);
    let timeout_start = Instant::now();
    loop {
        if let Ok(rows) = conn.query("SELECT timeseries FROM legacy.data", &[]).await {
            if rows.len() == expected_rows {
                break;
            }
        };

        if timeout_start.elapsed() > timeout {
            panic!("Timed out waiting for data to appear")
        }
    }
}

#[tokio::test]
async fn test_kafka_checked() {
    e2e_test_wrapper_legacy(async |producer: FutureProducer, db_pools: DbPools, _| {
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

        producer
            .send_result(
                FutureRecord::to(KAFKA_CHECKED_TOPIC)
                    .key("")
                    .payload(kafka_xml),
            )
            .unwrap()
            .await
            .unwrap()
            .unwrap();

        // As we have no way to sync with message processing in kvkafka ingestion, we just keep
        // trying to fetch data with a timeout
        let expected_rows = 1;
        let open_conn = db_pools.open.get().await.unwrap();
        wait_for_db_readiness(&open_conn, expected_rows).await;

        // TODO: we do not have an API endpoint to query the flags.kvdata table
        let data_row = open_conn
            .query_one(
                "SELECT timeseries, obstime, original, corrected, \
                        quality_code, controlinfo, useinfo, cfailed \
                    FROM legacy.data",
                &[],
            )
            .await
            .unwrap();

        #[allow(clippy::type_complexity)]
                let (
                    timeseries,
                    obstime,
                    original,
                    corrected,
                    quality_code,
                    controlinfo,
                    useinfo,
                    cfailed,
                ): (
                    i64,
                    DateTime<Utc>,
                    Option<f64>,
                    Option<f64>,
                    Option<i32>,
                    Option<String>,
                    Option<String>,
                    Option<String>,
                ) = (
                    data_row.get(0),
                    data_row.get(1),
                    data_row.get(2),
                    data_row.get(3),
                    data_row.get(4),
                    data_row.get(5),
                    data_row.get(6),
                    data_row.get(7),
                );
        assert_eq!(obstime, Utc.with_ymd_and_hms(2024, 6, 6, 6, 0, 0).unwrap());
        assert_eq!(original, Some(10.));
        assert_eq!(corrected, Some(10.));
        assert_eq!(
            quality_code,
            lard_ingestion::util::quality_code::get_quality_code(useinfo.clone().unwrap().as_str())
        );
        assert_eq!(controlinfo, Some("1000000000000000".to_string()));
        assert_eq!(useinfo, Some("9000000000000000".to_string()));
        assert_eq!(cfailed, None);

        let label_row = open_conn
            .query_one(
                "SELECT  station_id, param_id, type_id, lvl, sensor \
                    FROM labels.kvalobs \
                    WHERE timeseries = $1",
                &[&timeseries],
            )
            .await
            .unwrap();

        #[allow(clippy::type_complexity)]
        let (station_id, param_id, type_id, lvl, sensor): (
            // should these really all be Option??
            Option<i32>,
            Option<i32>,
            Option<i32>,
            Option<i32>,
            Option<i32>,
        ) = (
            label_row.get(0),
            label_row.get(1),
            label_row.get(2),
            label_row.get(3),
            label_row.get(4),
        );

        assert_eq!(station_id, Some(20001));
        assert_eq!(param_id, Some(106));
        assert_eq!(type_id, Some(-4));
        assert_eq!(lvl, Some(0));
        assert_eq!(sensor, Some(0));
    })
    .await
}

#[tokio::test]
async fn test_kafka_raw() {
    e2e_test_wrapper_legacy(async |producer: FutureProducer, db_pools: DbPools, _| {
        let ts = TestData {
            station_id: 20001,
            params: vec![Param::new("TA")],
            start_time: Utc.with_ymd_and_hms(2024, 6, 6, 6, 0, 0).unwrap(),
            period: Duration::hours(1),
            type_id: 501,
            len: 1,
        };

        producer
            .send_result(
                FutureRecord::to(KAFKA_RAW_TOPIC)
                    .key("")
                    .payload(&ts.obsinn_zeros()),
            )
            .unwrap()
            .await
            .unwrap()
            .unwrap();

        // As we have no way to sync with message processing in kvkafka ingestion, we just keep
        // trying to fetch data with a timeout
        let expected_rows = 1;
        let open_conn = db_pools.open.get().await.unwrap();
        wait_for_db_readiness(&open_conn, expected_rows).await;

        // TODO: we do not have an API endpoint to query the flags.kvdata table
        let data_row = open_conn
            .query_one("SELECT timeseries, obstime, original FROM legacy.data", &[])
            .await
            .unwrap();

        let (timeseries, obstime, original): (i64, DateTime<Utc>, Option<f64>) =
            (data_row.get(0), data_row.get(1), data_row.get(2));
        assert_eq!(obstime, Utc.with_ymd_and_hms(2024, 6, 6, 6, 0, 0).unwrap());
        assert_eq!(original, Some(0.));

        let label_row = open_conn
            .query_one(
                "SELECT station_id, param_id, type_id, lvl, sensor \
                        FROM labels.kvalobs \
                        WHERE timeseries = $1",
                &[&timeseries],
            )
            .await
            .unwrap();

        #[allow(clippy::type_complexity)]
        let (station_id, param_id, type_id, lvl, sensor): (
            // should these really all be Option??
            Option<i32>,
            Option<i32>,
            Option<i32>,
            Option<i32>,
            Option<i32>,
        ) = (
            label_row.get(0),
            label_row.get(1),
            label_row.get(2),
            label_row.get(3),
            label_row.get(4),
        );

        assert_eq!(station_id, Some(20001));
        assert_eq!(param_id, Some(211));
        assert_eq!(type_id, Some(501));
        assert_eq!(lvl, Some(0));
        assert_eq!(sensor, Some(0));
    })
    .await
}

#[tokio::test]
async fn test_patchwork_endpoint() {
    // Use values present in the mocks
    let cases = vec![
        (
            "?stationids=10001\
            &paramids=211\
            &levels=200\
            &sensors=0\
            &from=2024-12-31T23:00:00Z\
            &to=2025-01-01T01:30:00Z",
            None,
            200,
            3,
        ),
        (
            "?stationids=10001,20001\
            &paramids=211,225\
            &levels=200\
            &sensors=0\
            &from=2024-12-31T23:00:00Z\
            &to=2025-01-01T01:30:00Z",
            None,
            200,
            3,
        ),
        // 99995 has permitid 5 in mock_permit_tables(), so is restricted
        (
            "?stationids=99995\
            &paramids=211\
            &levels=200\
            &sensors=0\
            &from=2024-12-31T23:00:00Z\
            &to=2025-01-01T01:30:00Z",
            None, // no token, no data access
            404, // just don't see it... 
            0,
        ),
        (
            "?stationids=99995\
            &paramids=211\
            &levels=200\
            &sensors=0\
            &from=2024-12-31T23:00:00Z\
            &to=2025-01-01T01:30:00Z",
            // fake token created with roles 9,5 so should be able to see data
            Some("eyJ0eXAiOiJKV1QiLCJhbGciOiJFUzM4NCJ9.eyJyZXNvdXJjZV9hY2Nlc3MiOnsiT0RBIjp7InJvbGVzIjpbInBlcm1pdGlkLTkiLCJwZXJtaXRpZC01Il19fSwiZXhwIjoyMDcxOTE2MTY2fQ.K9VSyzl583Ck5pAvWj1dBHZ57VPeG00XyZY686BCLEtpCXAgB2I1FunROt3Vl1sP2mohnhbb5GOZInx_y-RW1LBHEeZRK-expKC10ipYsqUbG8-P0fw8HFH7vedMExHO"),
            200,
            3,
        ),
    ];

    e2e_test_wrapper_legacy(
        async |producer: FutureProducer, db_pools: DbPools, tables: PatchworkTables| {
            let t1: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 12, 31, 20, 0, 0).unwrap();
            let test_data = [
                TestData {
                    station_id: 10001,
                    params: vec![Param::new("TA")],
                    start_time: t1,
                    period: Duration::hours(1),
                    type_id: 508,
                    len: 8,
                },
                TestData {
                    station_id: 10001,
                    params: vec![Param::new("TA")],
                    start_time: t1,
                    period: Duration::hours(1),
                    type_id: 501,
                    len: 8,
                },
                TestData {
                    station_id: 20001,
                    params: vec![Param::new("TGX")],
                    start_time: t1,
                    period: Duration::hours(1),
                    type_id: 501,
                    len: 8,
                },
                TestData {
                    station_id: 99995,
                    params: vec![Param::new("TA")],
                    start_time: t1,
                    period: Duration::hours(1),
                    type_id: 508,
                    len: 8,
                },
                TestData {
                    station_id: 99995,
                    params: vec![Param::new("TA")],
                    start_time: t1,
                    period: Duration::hours(1),
                    type_id: 501,
                    len: 8,
                },
            ];

            for ts in test_data {
                producer
                    .send_result(
                        FutureRecord::to(KAFKA_RAW_TOPIC)
                            .key("")
                            .payload(&ts.obsinn_zeros()),
                    )
                    .unwrap()
                    .await
                    .unwrap()
                    .unwrap();
            }

            let open_conn = db_pools.open.get().await.unwrap();
            let restricted_conn = db_pools.restricted.get().await.unwrap();

            let expected_open_rows = 24;
            let expected_restricted_rows = 16;
            tokio::join!(
                wait_for_db_readiness(&open_conn, expected_open_rows),
                wait_for_db_readiness(&restricted_conn, expected_restricted_rows),
            );

            // Update patchwork with the timeseries in the database
            update_patchwork_table(&open_conn, tables.open.clone()).await;

            for (query, token, status, n_data_found) in cases {
                let url = format!("http://localhost:3000/patchwork{query}");
                let client = Client::new();
                let request = match token {
                    Some(t) => client.get(url).bearer_auth(t),
                    None => client.get(url),
                };

                let resp = request.send().await.unwrap();
                assert!(resp.status() == status);

                if status == StatusCode::OK {
                    let json: Vec<PatchworkResp> = resp.json().await.unwrap();
                    for x in json {
                        assert_eq!(x.data.len(), n_data_found);
                    }
                }
            }
        },
    )
    .await
}

#[tokio::test]
async fn test_idf_event_availability() {
    e2e_test_wrapper_legacy(
        async |producer: FutureProducer, db_pools: DbPools, tables: PatchworkTables| {
            let start_time = Utc.with_ymd_and_hms(2024, 12, 31, 23, 50, 0).unwrap();
            let ts_len = 20;
            let test_data = [
                TestData {
                    station_id: 10001,
                    params: vec![Param::new("RR_01")],
                    start_time,
                    period: Duration::minutes(1),
                    type_id: 514,
                    len: ts_len,
                },
                TestData {
                    station_id: 20001,
                    params: vec![Param::new("RR_01")],
                    start_time,
                    period: Duration::minutes(1),
                    type_id: 508,
                    len: ts_len,
                },
            ];

            for ts in &test_data {
                producer
                    .send_result(
                        FutureRecord::to(KAFKA_RAW_TOPIC)
                            .key("")
                            .payload(&ts.obsinn_zeros()),
                    )
                    .unwrap()
                    .await
                    .unwrap()
                    .unwrap();
            }

            let open_conn = db_pools.open.get().await.unwrap();
            let expected_open_rows = ts_len * test_data.len();
            wait_for_db_readiness(&open_conn, expected_open_rows).await;

            update_patchwork_table(&open_conn, tables.open).await;

            let url = "http://localhost:3000/reports/idf/event";
            let resp = reqwest::get(url).await.unwrap();
            assert!(resp.status().is_success(), "{}", resp.text().await.unwrap());

            let json: IdfEventAvailability = resp.json().await.unwrap();
            assert_eq!(json.stations.len(), test_data.len(), "{json:?}");
        },
    )
    .await
}

#[tokio::test]
async fn test_idf_event() {
    let start_time = Utc.with_ymd_and_hms(2024, 12, 31, 23, 40, 0).unwrap();
    let end_first_ts = Utc.with_ymd_and_hms(2024, 12, 31, 23, 49, 0).unwrap();
    let end_second_ts = Utc.with_ymd_and_hms(2025, 1, 1, 0, 9, 0).unwrap();

    let ts_len = 30;
    let station_id = 10001;
    let test_data = [
        TestData {
            station_id,
            params: vec![Param::new("RR_01")],
            start_time,
            period: Duration::minutes(1),
            type_id: 514,
            len: ts_len,
        },
        TestData {
            station_id,
            params: vec![Param::new("RR_01")],
            start_time,
            period: Duration::minutes(1),
            type_id: 508,
            len: ts_len,
        },
    ];

    let default_end_time = start_time.checked_add_signed(Duration::minutes(1)).unwrap();
    let cases = [
        (
            // Only extract 2 observations for simplicity (painful)
            "default durations",
            start_time,
            start_time.checked_add_signed(Duration::minutes(2)).unwrap(),
            None,
            DEFAULT_DURATIONS
                .iter()
                .map(|d| {
                    let (intensity, end_time) = if *d == 1 {
                        (1.0, start_time)
                    } else {
                        (2.0, default_end_time)
                    };
                    IdfEvent::new(intensity, *d, start_time, end_time)
                })
                .collect(),
        ),
        (
            // Should only get the first timeseries
            "single duration",
            start_time,
            start_time
                .checked_add_signed(Duration::minutes(10))
                .unwrap(),
            Some(vec![10]),
            vec![IdfEvent::new(10.0, 10, start_time, end_first_ts)],
        ),
        (
            // Should get both timeseries
            "multiple durations",
            start_time,
            start_time
                .checked_add_signed(Duration::minutes(50))
                .unwrap(),
            Some(vec![10, 40]),
            vec![
                IdfEvent::new(10.0, 10, start_time, end_first_ts),
                IdfEvent::new(30.0, 40, start_time, end_second_ts),
            ],
        ),
    ];

    e2e_test_wrapper_legacy(
        async |producer: FutureProducer, db_pools: DbPools, tables: PatchworkTables| {
            for ts in &test_data {
                producer
                    .send_result(
                        FutureRecord::to(KAFKA_RAW_TOPIC)
                            .key("")
                            .payload(&ts.obsinn_ones()),
                    )
                    .unwrap()
                    .await
                    .unwrap()
                    .unwrap();
            }

            let open_conn = db_pools.open.get().await.unwrap();
            let expected_open_rows = ts_len * test_data.len();
            wait_for_db_readiness(&open_conn, expected_open_rows).await;
            update_patchwork_table(&open_conn, tables.open).await;

            // HACK: need to set corrected and quality_code
            open_conn
                .execute(
                    "UPDATE legacy.data SET corrected = 1.0, quality_code = 1",
                    &[],
                )
                .await
                .unwrap();

            for (title, from, to, durations, expected) in cases {
                let duration_query = durations
                    .map(|v| {
                        let joined = v
                            .iter()
                            .map(|d| d.to_string())
                            .collect::<Vec<_>>()
                            .join(",");
                        format!("&durations={joined}")
                    })
                    .unwrap_or("".to_string());

                let url = format!(
                    "http://localhost:3000/reports/idf/event/{station_id}\
                        ?fromtime={from}\
                        &totime={to}\
                        {duration_query}",
                );

                let resp = reqwest::get(url).await.unwrap();
                assert!(
                    resp.status().is_success(),
                    "{title}: {}",
                    resp.text().await.unwrap()
                );

                let json: IdfEventResp = resp.json().await.unwrap();
                assert_eq!(json.station_id, station_id, "{title}");
                assert_eq!(json.values.len(), expected.len(), "{title}");

                for (val, exp) in json.values.into_iter().zip(expected) {
                    assert_eq!(val, exp, "{title}")
                }
            }
        },
    )
    .await
}

#[tokio::test]
async fn test_idf_failure() {
    let start_time = Utc.with_ymd_and_hms(2024, 12, 31, 23, 40, 0).unwrap();
    let end_time = Utc.with_ymd_and_hms(2025, 1, 1, 0, 9, 0).unwrap();

    let ts_len = 30;
    let station_id = 10001;
    let test_data = [
        TestData {
            station_id,
            params: vec![Param::new("RR_01")],
            start_time,
            period: Duration::minutes(1),
            type_id: 514,
            len: ts_len,
        },
        TestData {
            station_id,
            params: vec![Param::new("RR_01")],
            start_time,
            period: Duration::minutes(1),
            type_id: 508,
            len: ts_len,
        },
    ];

    e2e_test_wrapper_legacy(
        async |producer: FutureProducer, db_pools: DbPools, tables: PatchworkTables| {
            for ts in &test_data {
                producer
                    .send_result(
                        FutureRecord::to(KAFKA_RAW_TOPIC)
                            .key("")
                            .payload(&ts.obsinn_ones()),
                    )
                    .unwrap()
                    .await
                    .unwrap()
                    .unwrap();
            }

            let open_conn = db_pools.open.get().await.unwrap();
            let expected_open_rows = ts_len * test_data.len();
            wait_for_db_readiness(&open_conn, expected_open_rows).await;
            update_patchwork_table(&open_conn, tables.open).await;

            let url = format!(
                "http://localhost:3000/reports/idf/event/{station_id}\
                        ?fromtime={start_time}\
                        &totime={end_time}"
            );

            let resp = reqwest::get(url).await.unwrap();

            // Since the data is not QCed we won't get a positive response
            assert!(resp.status().is_client_error(),);
        },
    )
    .await
}
