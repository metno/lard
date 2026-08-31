use std::{panic::AssertUnwindSafe, time::Instant};

use futures::FutureExt;
use rdkafka::producer::{FutureProducer, FutureRecord};

use lard_egress::patchwork::PatchworkTables;
use util::{
    DbPools, PooledPgConn,
    mock::metadata::{mock_level_table, mock_permit_tables},
    stinfofacade::{self, permissions::timeseries_get_permit},
};

#[cfg(not(feature = "debug"))]
use crate::common::db_cleanup;

use crate::common::{TestData, update_patchwork_table, wrapper_setup};

pub const KAFKA_CHECKED_TOPIC: &str = "checked";
const KAFKA_RAW_TOPIC: &str = "raw";
const KAFKA_CHECKED_HIST_TOPIC: &str = "hist.checked";
const KAFKA_GROUP: &str = "lard_test";

pub struct IngestData<'a> {
    timeseries: Vec<TestData<'a>>,
    expected_open: usize,
    expected_restricted: usize,
}

impl<'a> IngestData<'a> {
    pub fn new(data: Vec<TestData<'a>>) -> Self {
        let mut expected_open = 0;
        let mut expected_restricted = 0;

        // Calculate expected rows to be found in the database after ingestion
        // To be honest this feels like another hack
        for ts in &data {
            for param in &ts.params {
                let permit = timeseries_get_permit(
                    mock_permit_tables(),
                    ts.station_id,
                    ts.type_id,
                    Some(param.id),
                )
                .unwrap();
                if permit == Some(1) {
                    expected_open += ts.len;
                } else {
                    expected_restricted += ts.len
                }
            }
        }

        Self {
            timeseries: data,
            expected_open,
            expected_restricted,
        }
    }
}

// Helper function that waits for data to be available
pub async fn wait_for_db_readiness(conn: &PooledPgConn<'_>, expected_rows: usize) {
    let timeout = std::time::Duration::from_secs(10);
    let timeout_start = Instant::now();
    loop {
        let rows_scalar = conn.query("SELECT timeseries FROM legacy.data", &[]).await;
        let rows_nonscalar = conn
            .query("SELECT timeseries FROM public.nonscalar_data", &[])
            .await;

        if let (Ok(scalar), Ok(nonscalar)) = (rows_scalar, rows_nonscalar)
            && scalar.len() + nonscalar.len() == expected_rows
        {
            break;
        };

        if timeout_start.elapsed() > timeout {
            panic!("Timed out waiting for data to appear")
        }
    }
}

/// Helper function that ingests data into the raw queue, waits for it to be available, and updates
/// the patchwork tables
pub async fn ingest_raw(
    data: &IngestData<'_>,
    producer: FutureProducer,
    pools: DbPools,
    tables: PatchworkTables,
) {
    for ts in &data.timeseries {
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

    let open_conn = pools.open.get().await.unwrap();
    let restricted_conn = pools.restricted.get().await.unwrap();

    // As we have no way to sync with message processing in kvkafka ingestion, we just keep
    // trying to fetch data with a timeout
    tokio::join!(
        wait_for_db_readiness(&open_conn, data.expected_open),
        wait_for_db_readiness(&restricted_conn, data.expected_restricted),
    );

    tokio::join!(
        update_patchwork_table(&open_conn, tables.open),
        update_patchwork_table(&restricted_conn, tables.restricted)
    );
}

/// Similar to e2e_test_wrapper, but adapted to use kvkafka ingestion instead of obsinn.
pub async fn e2e_test_wrapper_legacy(
    params: &[&str],
    test: impl AsyncFnOnce(FutureProducer, DbPools, PatchworkTables) -> (),
) {
    let (db_pools, patchwork_tables, mut egress, cancel_token) = wrapper_setup().await;

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

    let param_tables = stinfofacade::param::from_codes(params);

    let (ingestion_pools, ingestion_token) = (db_pools.clone(), cancel_token.clone());
    let mut ingestion = tokio::spawn(lard_ingestion::legacy::run(
        ingestion_pools,
        kafka_brokers,
        KAFKA_GROUP.to_string(),
        KAFKA_RAW_TOPIC,
        KAFKA_CHECKED_TOPIC,
        KAFKA_CHECKED_HIST_TOPIC,
        ingestion_token,
        mock_permit_tables(),
        mock_level_table(),
        param_tables,
    ));

    tokio::select! {
        _ = &mut egress => panic!("API server task terminated first"),
        _ = &mut ingestion => panic!("Ingestor server task terminated first"),
        // Clean up database even if test panics, to avoid test poisoning
        test_result = AssertUnwindSafe(test(kafka_producer, db_pools.clone(), patchwork_tables.clone())).catch_unwind() => {
            // For debugging a specific test, it might be useful to skip the cleanup process
            #[cfg(not(feature = "debug"))]
            db_cleanup(db_pools.clone()).await;

            assert!(test_result.is_ok())
        }
    }

    cancel_token.cancel();
    let (egress_result, ingestion_result) = tokio::join!(egress, ingestion);
    egress_result.unwrap();
    ingestion_result.unwrap().unwrap();
}
