use chrono::{DateTime, Duration, Utc};
use std::{
    collections::HashMap,
    panic::AssertUnwindSafe,
    sync::{Arc, RwLock},
};

use bb8_postgres::PostgresConnectionManager;
use futures::FutureExt;
use tokio::task::JoinHandle;
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;

use lard_egress::patchwork::{
    PatchworkTables, PatchworkTimeseriesTable, create_patchwork_timeseries_table,
    fetch_timeseries_list_from_database,
};
use util::{DbPools, PgPool, PooledPgConn, stinfofacade};

pub mod legacy;
pub mod mocks;

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

impl<'a> Param<'a> {
    pub fn new(code: &'a str) -> Self {
        let (id, obstype) = match code {
            "TA" => (211, TestObsType::Scalar),
            "KLOBS" => (1022, TestObsType::NonScalar),
            "RR_1" => (106, TestObsType::Scalar),
            "RR_01" => (105, TestObsType::Scalar),
            "TGM" => (222, TestObsType::Scalar),
            "TGX" => (225, TestObsType::Scalar),
            "FF" => (81, TestObsType::Scalar),
            "DD" => (61, TestObsType::Scalar),
            &_ => panic!("undefined param"),
        };

        Self {
            id,
            code,
            sensor_level: None,
            obstype,
            values: None,
        }
    }

    pub fn with_sensor_level(mut self, sensor_level: (i32, i32)) -> Self {
        self.sensor_level = Some(sensor_level);
        self
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
        assert!(
            self.params
                .iter()
                .map(|p| &p.values)
                .all(|v| v.as_ref().is_none_or(|y| y.len() == self.len))
        );

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

async fn create_db_pool(conn_var: &str) -> PgPool {
    let manager =
        PostgresConnectionManager::new_from_stringlike(std::env::var(conn_var).unwrap(), NoTls)
            .unwrap();
    bb8::Pool::builder().build(manager).await.unwrap()
}

pub async fn create_db_pools() -> (DbPools, DbPools) {
    let open_db_pool = create_db_pool("LARD_CONN_STRING").await;
    let restricted_db_pool = create_db_pool("LARD_RESTRICTED_CONN_STRING").await;
    let open_readonly_db_pool = create_db_pool("LARD_READONLY_CONN_STRING").await;
    let restricted_readonly_db_pool = create_db_pool("LARD_READONLY_RESTRICTED_CONN_STRING").await;

    (
        DbPools {
            open: open_db_pool,
            restricted: restricted_db_pool,
        },
        DbPools {
            open: open_readonly_db_pool,
            restricted: restricted_readonly_db_pool,
        },
    )
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
    let (db_pools, db_readonly_pools) = create_db_pools().await;

    // set up cancellation token and signal catcher to detect premature shutdown
    let cancel_token = CancellationToken::new();

    let patchwork_tables = empty_patchwork_tables();

    let egress = tokio::spawn(lard_egress::run(
        db_readonly_pools,
        None,
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

pub async fn e2e_test_wrapper(params: &[&str], test: impl AsyncFnOnce(DbPools)) {
    let (db_pools, _, mut egress, cancel_token) = wrapper_setup().await;

    let param_tables = stinfofacade::param::from_codes(params);

    let ingestor_pools = db_pools.clone();
    let ingestor_token = cancel_token.clone();
    let mut ingestion = tokio::spawn(async move {
        lard_ingestion::run(
            ingestor_pools,
            param_tables,
            mocks::mock_permit_tables(),
            mocks::mock_level_table(),
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
