use chrono::{DateTime, Utc};
use std::{
    collections::HashMap,
    panic::AssertUnwindSafe,
    sync::{Arc, LazyLock, RwLock},
};

use bb8_postgres::PostgresConnectionManager;
use chrono::Duration;
use futures::FutureExt;
use rove_connector::Connector;
use tokio::task::JoinHandle;
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;

use crate::common::mocks::mock_auth_certs;
use lard_egress::patchwork::{
    create_patchwork_timeseries_table, fetch_timeseries_list_from_database, PatchworkTables,
    PatchworkTimeseriesTable,
};
use lard_ingestion::{get_conversions, util::qc_pipelines::load_pipelines};
use util::{DbPools, PooledPgConn};

pub mod legacy;
pub mod mocks;

// fake token created with roles 9,5 and station 1234 so should be able to see extra data
pub const RESTRICTED_TOKEN: &str = "eyJ0eXAiOiJKV1QiLCJhbGciOiJFUzM4NCJ9.\
eyJyZXNvdXJjZV9hY2Nlc3MiOnsiT0RBIjp7InJvbGVzIjpbInJlYWQtcGVybWl0aWQtOSIsInJ\
lYWQtcGVybWl0aWQtNSIsInJlYWQtc3RhdGlvbmlkLTEyMzQiXX19LCJleHAiOjIwODUyMjYyMDl9.\
wjYbORedpBs6VlK44V_4lWVUh0KyiK71jDzIKAhEDU7UQCM4nraGg3AAoOse4wWHoT7SCAqoscDZke\
GqIXjfqKs1A6dU3n5UwmlXuROZc3vfzQq6O1PXReEleYEhXyH4";

#[derive(Clone, Copy)]
pub enum TestObsType {
    Scalar,
    NonScalar,
}

#[derive(Clone)]
pub struct Param<'a> {
    pub id: i32,
    pub code: &'a str,
    pub sensor_level: Option<(i32, i32)>,
    pub obstype: TestObsType,
    values: Option<Vec<f64>>,
}

impl Param<'_> {
    pub fn new(code: &str) -> Self {
        let (code, (id, obstype)) = PARAMETERS
            .get_key_value(code)
            .expect("Provided param code should be present in global params hashmap");

        Self {
            id: *id,
            code,
            sensor_level: None,
            obstype: *obstype,
            values: None,
        }
    }

    pub fn with_sensor_level(code: &str, sensor_level: (i32, i32)) -> Self {
        let (code, (id, obstype)) = PARAMETERS
            .get_key_value(code)
            .expect("Provided param code should be present in global params hashmap");

        Self {
            id: *id,
            code,
            sensor_level: Some(sensor_level),
            obstype: *obstype,
            values: None,
        }
    }

    pub fn with_values(mut self, values: Vec<f64>) -> Self {
        self.values = Some(values);
        self
    }
}

pub struct TestData<'a> {
    pub station_id: i32,
    pub type_id: i32,
    pub params: Vec<Param<'a>>,
    pub start_time: DateTime<Utc>,
    pub period: Duration,
    pub len: usize,
}

impl TestData<'_> {
    // Creates a message with the following format:
    // ```
    // kldata/nationalnr=99999/type=501/messageid=23
    // param_1,param_2(0,0),...
    // 20240101000000,0.0,0.0,...
    // 20240101010000,0.0,0.0,...
    // ...
    // ```
    pub fn obsinn_message(&self, scalar_val: f64) -> String {
        let nonscalar_val = "test";

        // Either all params don't have values,
        // otherwise all the values match the `len` field
        assert!(self
            .params
            .iter()
            .map(|p| &p.values)
            .all(|v| v.as_ref().is_none_or(|y| y.len() == self.len)));

        let mut idx = 0;
        let mut time = self.start_time;
        let mut msg = vec![self.obsinn_header(), self.param_header()];

        while idx < self.len {
            let mut values = vec![];

            for param in &self.params {
                if let Some(vals) = &param.values {
                    values.push(vals[idx].to_string());
                } else {
                    values.push(match param.obstype {
                        TestObsType::Scalar => scalar_val.to_string(),
                        TestObsType::NonScalar => nonscalar_val.to_string(),
                    })
                }
            }

            let row = values.join(",");
            msg.push(format!("{},{}", time.format("%Y%m%d%H%M%S"), row));

            idx += 1;
            time += self.period;
        }

        msg.join("\n")
    }

    // Creates an obsimm message where all values are zeros
    pub fn obsinn_zeros(&self) -> String {
        self.obsinn_message(0.0)
    }

    // Creates an obsimm message where all values are ones
    pub fn obsinn_ones(&self) -> String {
        self.obsinn_message(1.0)
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

pub async fn create_db_pools() -> DbPools {
    let open_manager = PostgresConnectionManager::new_from_stringlike(
        std::env::var("LARD_CONN_STRING").unwrap(),
        NoTls,
    )
    .unwrap();
    let open_db_pool = bb8::Pool::builder().build(open_manager).await.unwrap();

    let restricted_manager = PostgresConnectionManager::new_from_stringlike(
        std::env::var("LARD_RESTRICTED_CONN_STRING").unwrap(),
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

// Create empty patchwork tables, these must be updated inside the tests that need the
// patchwork timeseries, since they require knowledge of the timeseries present in the database
pub fn empty_patchwork_tables() -> PatchworkTables {
    PatchworkTables::new(HashMap::new(), HashMap::new())
}

pub async fn update_patchwork_table(
    conn: &PooledPgConn<'_>,
    table: Arc<RwLock<PatchworkTimeseriesTable>>,
) {
    let db_list = fetch_timeseries_list_from_database(conn).await.unwrap();
    let message_priority = mocks::mock_message_priority();
    // Empty exceptions, could mock them in the future
    let exceptions = HashMap::new();

    let new_table =
        create_patchwork_timeseries_table(db_list, message_priority, exceptions).unwrap();

    let mut writer = table.write().unwrap();
    *writer = new_table;
}

pub async fn wrapper_setup() -> (DbPools, PatchworkTables, JoinHandle<()>, CancellationToken) {
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

    let patchwork_tables = empty_patchwork_tables();

    let egress = tokio::spawn(lard_egress::run(
        db_pools.clone(),
        s3_bucket,
        patchwork_tables.clone(),
        mocks::mock_auth_certs(),
        cancel_token.clone(),
    ));

    (db_pools, patchwork_tables, egress, cancel_token)
}

pub async fn db_cleanup(db_pools: DbPools) {
    for db_pool in [db_pools.open, db_pools.restricted] {
        let client = db_pool.get().await.unwrap();
        client
            .batch_execute(
                "TRUNCATE public.timeseries, labels.met, labels.obsinn RESTART IDENTITY CASCADE",
            )
            .await
            .unwrap();
    }
}

pub async fn e2e_test_wrapper(test: impl AsyncFnOnce(DbPools)) {
    let (db_pools, _, mut egress, cancel_token) = wrapper_setup().await;

    let rove_connector = Connector {
        pool: db_pools.open.clone(),
    };
    let qc_pipelines = load_pipelines("mock_qc_pipelines/fresh").expect("failed to load pipelines");

    let param_conv_path = std::env::var("PARAMCONV_CSV").unwrap();
    let param_conversions =
        get_conversions(&param_conv_path).expect("failed to load param conversions");

    let ingestor_pools = db_pools.clone();
    let ingestor_token = cancel_token.clone();
    let mut ingestion = tokio::spawn(async move {
        lard_ingestion::run(
            ingestor_pools,
            param_conversions,
            mocks::mock_permit_tables(),
            mocks::mock_level_table(),
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
        test_result = AssertUnwindSafe(test(db_pools.clone())).catch_unwind() => {
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

pub async fn s3_test_wrapper(
    (base, path, content): (&str, &str, &str),
    test: impl AsyncFnOnce() -> (),
) {
    let db_pools = create_db_pools().await;

    // set up cancellation token and signal catcher to detect premature shutdown
    let cancel_token = CancellationToken::new();
    let bucket: Arc<s3::Bucket> = Arc::from(
        s3::Bucket::new(
            &std::env::var("S3_BUCKET_NAME").unwrap(),
            s3::Region::from_env("AWS_REGION", Some("S3_ENDPOINT_URL")).unwrap(),
            // Requires "AWS_ACCESS_KEY_ID" and "AWS_SECRET_ACCESS_KEY" to be set
            s3::creds::Credentials::from_env().unwrap(),
        )
        .unwrap()
        // TODO: not sure what the path would be otherwise
        .with_path_style(),
    );
    let s3path = format!("{base}{path}");
    if let Err(e) = bucket.put_object(s3path, content.as_bytes()).await {
        panic!("{e}")
    };

    let mut egress = tokio::spawn(lard_egress::run(
        db_pools.clone(),
        bucket,
        empty_patchwork_tables(),
        mock_auth_certs(),
        cancel_token.clone(),
    ));

    tokio::select! {
        _ = &mut egress => panic!("API server task terminated first"),
        // Clean up database even if test panics, to avoid test poisoning
        test_result = AssertUnwindSafe(test()).catch_unwind() => {
            // For debugging a specific test, it might be useful to skip the cleanup process
            #[cfg(not(feature = "debug"))]
            db_cleanup(db_pools).await;

            assert!(test_result.is_ok())
        }
    }

    cancel_token.cancel();
    egress.await.unwrap()
}
