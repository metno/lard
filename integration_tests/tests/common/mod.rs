use std::{
    collections::HashMap,
    future::Future,
    panic::AssertUnwindSafe,
    sync::{Arc, LazyLock, RwLock},
};

use bb8_postgres::PostgresConnectionManager;
use futures::FutureExt;
use rove_connector::Connector;
use tokio::task::JoinHandle;
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;

use lard_ingestion::{
    levels::{Level, ParamLevelTable},
    permissions::{ParamPermit, ParamPermitTable, StationPermitTable},
    qc_pipelines::load_pipelines,
    DbPools,
};

#[derive(Clone, Copy)]
pub enum TestObsType {
    Scalar,
    NonScalar,
}

// TODO: make API and ingestor global static as well? So we don't have to recreate them for each test?
pub static PARAMETERS: LazyLock<HashMap<String, (i32, TestObsType)>> = LazyLock::new(|| {
    let path = std::env::var("PARAMCONV_CSV").unwrap();

    csv::Reader::from_path(path)
        .unwrap()
        .into_records()
        .map(|record_result| {
            let record = record_result.unwrap();
            (
                record.get(1).unwrap().to_owned(),
                (
                    record.get(0).unwrap().parse::<i32>().unwrap(),
                    match record.get(3).unwrap() {
                        "t" => TestObsType::Scalar,
                        "f" => TestObsType::NonScalar,
                        _ => unreachable!(),
                    },
                ),
            )
        })
        .collect()
});

pub fn mock_permit_tables() -> Arc<RwLock<(ParamPermitTable, StationPermitTable)>> {
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
        (20001, 1), // open
        (20002, 1), // open
    ]);

    Arc::new(RwLock::new((param_permit, station_permit)))
}

pub fn mock_level_table() -> Arc<RwLock<ParamLevelTable>> {
    let param_level = HashMap::from([
        (211, Level::new(2, 0, "above".to_string())),
        (81, Level::new(10, 0, "above".to_string())),
        (3, Level::new(20, -2, "below".to_string())),
    ]);

    Arc::new(RwLock::new(param_level))
}

pub async fn create_db_pools() -> DbPools {
    let open_manager = PostgresConnectionManager::new_from_stringlike(
        std::env::var("LARD_CONN_STRING").unwrap(),
        NoTls,
    )
    .unwrap();
    let open_db_pool = bb8::Pool::builder().build(open_manager).await.unwrap();

    let restricted_manager = PostgresConnectionManager::new_from_stringlike(
        std::env::var("LARD_CONN_STRING_RESTRICTED").unwrap(),
        NoTls,
    )
    .unwrap();
    let restricted_db_pool = bb8::Pool::builder()
        .build(restricted_manager)
        .await
        .unwrap();

    DbPools {
        open: open_db_pool,
        restricted: restricted_db_pool,
    }
}

pub async fn wrapper_setup() -> (DbPools, JoinHandle<()>, CancellationToken) {
    let db_pools = create_db_pools().await;

    let s3_bucket = Arc::from(
        s3::Bucket::new(
            &std::env::var("S3_BUCKET_NAME").unwrap(),
            s3::Region::from_env("AWS_REGION", Some("S3_ENDPOINT_URL")).unwrap(),
            // Requires "AWS_ACCESS_KEY_ID" and "AWS_SECRET_ACCESS_KEY" to be set
            s3::creds::Credentials::from_env().unwrap(),
        )
        .unwrap()
        .with_path_style(),
    );

    // set up cancellation token and signal catcher to detect premature shutdown
    let cancel_token = CancellationToken::new();

    let egress = tokio::spawn(lard_egress::run(
        db_pools.open.clone(),
        s3_bucket,
        cancel_token.clone(),
    ));

    (db_pools, egress, cancel_token)
}

pub async fn db_cleanup(db_pools: DbPools) {
    for db_pool in [db_pools.open, db_pools.restricted] {
        let client = db_pool.get().await.unwrap();
        client
            .batch_execute(
                // TODO: should clean public.timeseries_id_seq too? RESTART IDENTITY CASCADE?
                "TRUNCATE public.timeseries, labels.met, labels.obsinn CASCADE",
            )
            .await
            .unwrap();
    }
}

pub async fn e2e_test_wrapper<T: Future<Output = ()>>(test: T) {
    let (db_pools, mut egress, cancel_token) = wrapper_setup().await;

    let rove_connector = Connector {
        pool: db_pools.open.clone(),
    };
    let qc_pipelines = load_pipelines("mock_qc_pipelines/fresh").expect("failed to load pipelines");

    let param_conv_path = std::env::var("PARAMCONV_CSV").unwrap();
    let ingestor_pools = db_pools.clone();
    let ingestor_token = cancel_token.clone();
    let mut ingestion = tokio::spawn(async move {
        lard_ingestion::run(
            ingestor_pools,
            &param_conv_path,
            mock_permit_tables(),
            mock_level_table(),
            rove_connector,
            qc_pipelines,
            ingestor_token,
        )
        .await
    });

    tokio::select! {
        _ = &mut egress => panic!("API server task terminated first"),
        _ = &mut ingestion => panic!("Ingestor server task terminated first"),
        // Clean up database even if test panics, to avoid test poisoning
        test_result = AssertUnwindSafe(test).catch_unwind() => {
            // For debugging a specific test, it might be useful to skip the cleanup process
            #[cfg(not(feature = "debug"))]
            db_cleanup(db_pools).await;

            assert!(test_result.is_ok())
        }
    }

    cancel_token.cancel();
    let (egress_result, ingestion_result) = tokio::join!(egress, ingestion);
    egress_result.unwrap();
    ingestion_result.unwrap().unwrap()
}
