use std::{panic::AssertUnwindSafe, time::Instant};

use chrono::{DateTime, Duration, TimeZone, Utc};
use futures::FutureExt;
use rdkafka::producer::{FutureProducer, FutureRecord};
use reqwest::Client;
use reqwest::{header::AUTHORIZATION, StatusCode};

use lard_egress::PatchworkResp;

use lard_ingestion::get_conversions;
use util::DbPools;

pub mod common;
use common::{Param, TestData};

const KAFKA_RAW_TOPIC: &str = "raw";
const KAFKA_CHECKED_TOPIC: &str = "checked";
const KAFKA_CHECKED_HIST_TOPIC: &str = "hist.checked";
const KAFKA_GROUP: &str = "lard_test";

/// Similar to e2e_test_wrapper, but adapted to use kvkafka ingestion instead of obsinn.
pub async fn e2e_test_wrapper_legacy(test: impl AsyncFnOnce(FutureProducer, DbPools) -> ()) {
    let (db_pools, mut egress, cancel_token) = common::wrapper_setup().await;

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
        test_result = AssertUnwindSafe(test(kafka_producer, db_pools.clone())).catch_unwind() => {
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

#[tokio::test]
async fn test_kafka_checked() {
    e2e_test_wrapper_legacy(async |producer: FutureProducer, db_pools: DbPools| {
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

        // TODO: we do not have an API endpoint to query the flags.kvdata table
        let open_conn = db_pools.open.get().await.unwrap();

        // As we have no way to sync with message processing in kvkafka ingestion, we just keep
        // trying to fetch data with a timeout
        let timeout = std::time::Duration::from_secs(10);
        let timeout_start = Instant::now();
        loop {
            if let Ok(data_row) = open_conn
                .query_one(
                    r#"
                        SELECT
                            timeseries,
                            obstime,
                            original,
                            corrected,
                            quality_code,
                            controlinfo,
                            useinfo,
                            cfailed
                        FROM legacy.data
                    "#,
                    &[],
                )
                .await
            {
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
                    lard_ingestion::util::quality_code::get_quality_code(
                        useinfo.clone().unwrap().as_str()
                    )
                );
                assert_eq!(controlinfo, Some("1000000000000000".to_string()));
                assert_eq!(useinfo, Some("9000000000000000".to_string()));
                assert_eq!(cfailed, None);

                let label_row = open_conn
                    .query_one(
                        r#"
                        SELECT
                            station_id,
                            param_id,
                            type_id,
                            lvl,
                            sensor
                        FROM labels.kvalobs
                        WHERE timeseries = $1
                    "#,
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

                break;
            }

            if timeout_start.elapsed() > timeout {
                panic!("Timed out waiting for data to appear")
            }
        }
    })
    .await
}

#[tokio::test]
async fn test_kafka_raw() {
    e2e_test_wrapper_legacy(async |producer: FutureProducer, db_pools: DbPools| {
        let ts = TestData {
            station_id: 20001,
            params: vec![Param::new("TA")],
            // start_time: Utc::now().duration_trunc(TimeDelta::hours(1)).unwrap()
            //     - Duration::hours(11),
            start_time: Utc.with_ymd_and_hms(2024, 6, 6, 6, 0, 0).unwrap(),
            period: Duration::hours(1),
            type_id: 501,
            len: 1,
        };

        producer
            .send_result(
                FutureRecord::to(KAFKA_RAW_TOPIC)
                    .key("")
                    .payload(&ts.obsinn_message()),
            )
            .unwrap()
            .await
            .unwrap()
            .unwrap();

        // TODO: we do not have an API endpoint to query the flags.kvdata table
        let open_conn = db_pools.open.get().await.unwrap();

        // As we have no way to sync with message processing in kvkafka ingestion, we just keep
        // trying to fetch data with a timeout
        let timeout = std::time::Duration::from_secs(10);
        let timeout_start = Instant::now();
        loop {
            if let Ok(data_row) = open_conn
                .query_one(
                    r#"
                        SELECT
                            timeseries,
                            obstime,
                            original
                        FROM legacy.data
                    "#,
                    &[],
                )
                .await
            {
                let (timeseries, obstime, original): (i64, DateTime<Utc>, Option<f64>) =
                    (data_row.get(0), data_row.get(1), data_row.get(2));
                assert_eq!(obstime, Utc.with_ymd_and_hms(2024, 6, 6, 6, 0, 0).unwrap());
                assert_eq!(original, Some(0.));

                let label_row = open_conn
                    .query_one(
                        r#"
                        SELECT
                            station_id,
                            param_id,
                            type_id,
                            lvl,
                            sensor
                        FROM labels.kvalobs
                        WHERE timeseries = $1
                    "#,
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

                break;
            }

            if timeout_start.elapsed() > timeout {
                panic!("Timed out waiting for data to appear")
            }
        }
    })
    .await
}

// test patchwork...
#[tokio::test]
async fn test_patchwork_endpoint() {
    // find an example from the mock table...
    let cases = vec![
        (
            "?stationids=10001&paramids=211&levels=0&sensors=0&from=2024-12-31T23:00:00Z&to=2025-01-01T01:30:00Z",
            None,
            200,
            3,
        ),
        (
            "?stationids=10001,20001&paramids=211,225&levels=0&sensors=0&from=2024-12-31T23:00:00Z&to=2025-01-01T01:30:00Z",
            None,
            200,
            3,
        ),
        // 99995 has permitid 5 in mock_permit_tables(), so is restricted
        (
            "?stationids=99995&paramids=211&levels=0&sensors=0&from=2024-12-31T23:00:00Z&to=2025-01-01T01:30:00Z",
            None, // no token, no data access
            404, // just don't see it... 
            0,
        ),
        (
            "?stationids=99995&paramids=211&levels=0&sensors=0&from=2024-12-31T23:00:00Z&to=2025-01-01T01:30:00Z",
            // fake token created with roles 9,5 so should be able to see data
            Some("eyJ0eXAiOiJKV1QiLCJhbGciOiJFUzM4NCJ9.eyJyZXNvdXJjZV9hY2Nlc3MiOnsiT0RBIjp7InJvbGVzIjpbInBlcm1pdGlkLTkiLCJwZXJtaXRpZC01Il19fSwiZXhwIjoyMDcxOTE2MTY2fQ.K9VSyzl583Ck5pAvWj1dBHZ57VPeG00XyZY686BCLEtpCXAgB2I1FunROt3Vl1sP2mohnhbb5GOZInx_y-RW1LBHEeZRK-expKC10ipYsqUbG8-P0fw8HFH7vedMExHO"),
            200,
            3,
        ),
    ];

    e2e_test_wrapper_legacy(async |producer: FutureProducer, db_pools: DbPools| {
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
                        .payload(&ts.obsinn_message()),
                )
                .unwrap()
                .await
                .unwrap()
                .unwrap();
        }

        let open_conn = db_pools.open.get().await.unwrap();
        let restricted_conn = db_pools.restricted.get().await.unwrap();

        // As we have no way to sync with message processing in kvkafka ingestion, we just keep
        // trying to fetch data with a timeout
        let timeout = std::time::Duration::from_secs(10);
        let timeout_start = Instant::now();
        loop {
            if let Ok(data_rows) = open_conn
                .query(
                    r#"
                        SELECT
                            timeseries,
                            obstime,
                            original
                        FROM legacy.data
                    "#,
                    &[],
                )
                .await
            {
                if data_rows.len() == 24 {
                    // have the open data, but also check restricted
                    if let Ok(data_rows_restricted) = restricted_conn
                        .query(
                            r#"
                        SELECT
                            timeseries,
                            obstime,
                            original
                        FROM legacy.data
                    "#,
                            &[],
                        )
                        .await
                    {
                        if data_rows_restricted.len() == 16 {
                            break;
                        }
                    }
                }
            }
            // or else keep looping since no data has been written

            if timeout_start.elapsed() > timeout {
                panic!("Timed out waiting for data to appear")
            }
        }
        for (query, token, status, n_data_found) in cases {
            let url = format!("http://localhost:3000/patchwork{query}");
            let client = Client::new();
            let request = match token {
                Some(t) => client.get(url).bearer_auth(t),
                None => client.get(url),
            };;
            
            let resp = request.send().await.unwrap();
            assert!(resp.status() == status);
            
            if status == StatusCode::OK {
                let json: Vec<PatchworkResp> = resp.json().await.unwrap();
                for x in json {
                    assert_eq!(x.data.len(), n_data_found);
                }
            }
        }
            }
        }
    })
    .await
}
