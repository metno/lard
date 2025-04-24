use std::{panic::AssertUnwindSafe, time::Instant};

use chrono::{DateTime, TimeZone, Utc};
use futures::FutureExt;
use rdkafka::producer::{FutureProducer, FutureRecord};

use lard_ingestion::DbPools;
pub mod common;

const KAFKA_TOPIC: &str = "checked";
const KAFKA_GROUP: &str = "lard_test";

/// Similar to e2e_test_wrapper, but adapted to use kvkafka ingestion instead of obsinn.
pub async fn e2e_test_wrapper_kafka(test: impl AsyncFnOnce(FutureProducer, DbPools) -> ()) {
    let (db_pools, mut egress, cancel_token) = common::wrapper_setup().await;

    let mock_kafka_cluster = rdkafka::mocking::MockCluster::new(3).unwrap();
    mock_kafka_cluster.create_topic(KAFKA_TOPIC, 32, 3).unwrap();
    let kafka_brokers = mock_kafka_cluster.bootstrap_servers();

    let kafka_producer: FutureProducer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", kafka_brokers.clone())
        .create()
        .unwrap();

    let (ingestion_pools, ingestion_token) = (db_pools.clone(), cancel_token.clone());
    let mut ingestion = tokio::spawn(async move {
        let kafka_brokers = kafka_brokers;
        lard_ingestion::kvkafka::ingest_kvkafka(
            ingestion_pools,
            &kafka_brokers,
            KAFKA_GROUP,
            KAFKA_TOPIC,
            ingestion_token,
            common::mock_permit_tables(),
            common::mock_level_table(),
        )
        .await
    });

    tokio::select! {
        _ = &mut egress => panic!("API server task terminated first"),
        _ = &mut ingestion => panic!("Ingestor server task terminated first"),
        // Clean up database even if test panics, to avoid test poisoning
        test_result = AssertUnwindSafe(test(kafka_producer, db_pools.clone())).catch_unwind() => {
            // For debugging a specific test, it might be useful to skip the cleanup process
            #[cfg(not(feature = "debug"))]
            if test_result.is_err() {
                common::db_cleanup(db_pools.clone()).await;
            }

            assert!(test_result.is_ok())
        }
    }

    cancel_token.cancel();
    let (egress_result, ingestion_result) = tokio::join!(egress, ingestion);
    egress_result.unwrap();
    ingestion_result.unwrap().unwrap();
}

#[tokio::test]
async fn test_kafka() {
    e2e_test_wrapper_kafka(async |producer: FutureProducer, db_pools: DbPools| {
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
            .send_result(FutureRecord::to(KAFKA_TOPIC).key("").payload(kafka_xml))
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
                    lard_ingestion::kvkafka::get_quality_code(useinfo.clone().unwrap().as_str())
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
