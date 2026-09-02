use std::sync::{Arc, RwLock};

use bb8_postgres::PostgresConnectionManager;
use chrono::{DateTime, Duration, Utc};
use rdkafka::producer::FutureProducer;
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;
use tower_sessions::{MemoryStore, SessionManagerLayer};

use lard_egress::patchwork::{
    PatchworkTables, create_patchwork_timeseries_table, fetch_timeseries_list_from_database,
};
use util::{
    DbPools, PgPool,
    mock::auth::bearer::mock_auth_certs,
    stinfofacade::{self, permissions::PermitTables},
};

pub mod legacy;
pub mod next;
pub mod patchwork;

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
            "UU" => (262, TestObsType::Scalar),
            "PA" => (173, TestObsType::Scalar),
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

pub async fn create_patchwork_tables(pools: DbPools) -> PatchworkTables {
    let open_conn = pools.open.get().await.unwrap();
    let restricted_conn = pools.restricted.get().await.unwrap();

    // Empty exceptions, could mock them in the future
    let (defaults, exceptions) = stinfofacade::persistence::message_priority::load_persisted()
        .await
        .unwrap();

    let open_db_list = fetch_timeseries_list_from_database(&open_conn)
        .await
        .unwrap();
    let restricted_db_list = fetch_timeseries_list_from_database(&restricted_conn)
        .await
        .unwrap();
    let open_table =
        create_patchwork_timeseries_table(open_db_list, &defaults, &exceptions).unwrap();
    let restricted_table =
        create_patchwork_timeseries_table(restricted_db_list, &defaults, &exceptions).unwrap();

    PatchworkTables::new(open_table, restricted_table)
}

pub async fn e2e_test_setup() -> (FutureProducer, DbPools, PermitTables) {
    let (db_pools, db_readonly_pools) = create_db_pools().await;

    // set up cancellation token and signal catcher to detect premature shutdown
    let cancel_token = CancellationToken::new();

    let patchwork_tables = create_patchwork_tables(db_pools.clone()).await;
    let level_table = Arc::new(RwLock::new(
        stinfofacade::persistence::level::load_persisted()
            .await
            .unwrap(),
    ));

    let mock_kafka_cluster = rdkafka::mocking::MockCluster::new(3).unwrap();
    mock_kafka_cluster
        .create_topic(legacy::KAFKA_RAW_TOPIC, 32, 3)
        .unwrap();
    mock_kafka_cluster
        .create_topic(legacy::KAFKA_CHECKED_TOPIC, 32, 3)
        .unwrap();
    let kafka_brokers = mock_kafka_cluster.bootstrap_servers();
    // if we don't leak this it will stop working once it goes out of scope and is
    // dropped.
    Box::leak(Box::new(mock_kafka_cluster));

    let kafka_producer: FutureProducer = rdkafka::ClientConfig::new()
        .set("bootstrap.servers", kafka_brokers.clone())
        .create()
        .unwrap();

    let param_table = Arc::new(RwLock::new(
        stinfofacade::persistence::param::load_persisted()
            .await
            .unwrap(),
    ));
    let permit_tables = Arc::new(RwLock::new(
        stinfofacade::persistence::permissions::load_persisted()
            .await
            .unwrap(),
    ));

    util::auth::bearer::JWKS_CERTS.get_or_init(mock_auth_certs);

    let _mock_oidc_provider = tokio::spawn(util::mock::auth::oidc::run());
    let oidc_client = util::auth::oidc::create_oidc_client(
        "http://localhost:3008".to_string(),
        "lard_integration_testing".to_string(),
        None,
        "http://localhost:3001/oidc_redirect".to_string(),
    )
    .await
    .unwrap();
    _ = util::auth::oidc::CLIENT.set(oidc_client);

    let session_store_egress = MemoryStore::default();
    let session_store_ingestion = MemoryStore::default();
    let session_layer_egress = SessionManagerLayer::new(session_store_egress);
    let session_layer_ingestion = SessionManagerLayer::new(session_store_ingestion);

    let _egress = tokio::spawn(lard_egress::run(
        db_readonly_pools,
        None,
        patchwork_tables.clone(),
        level_table.clone(),
        cancel_token.clone(),
        session_layer_egress,
    ));

    let (legacy_pools, legacy_cancel_token) = (db_pools.clone(), cancel_token.clone());
    let _legacy_ingestion = tokio::spawn(lard_ingestion::legacy::run(
        legacy_pools,
        kafka_brokers,
        legacy::KAFKA_GROUP.to_string(),
        legacy::KAFKA_RAW_TOPIC,
        legacy::KAFKA_CHECKED_TOPIC,
        legacy::KAFKA_CHECKED_HIST_TOPIC,
        legacy_cancel_token,
        permit_tables.clone(),
        level_table.clone(),
        param_table.clone(),
    ));

    let (next_pools, next_cancel_token, next_permit_tables) = (
        db_pools.clone(),
        cancel_token.clone(),
        permit_tables.clone(),
    );
    let _next_ingestion = tokio::spawn(async move {
        lard_ingestion::run(
            next_pools,
            param_table,
            "resources/assets".to_string(),
            next_permit_tables,
            level_table,
            next_cancel_token,
            session_layer_ingestion,
        )
        .await
    });

    (kafka_producer, db_pools, permit_tables)
}
