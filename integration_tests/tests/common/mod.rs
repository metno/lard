use chrono::{DateTime, Utc};
use jsonwebtoken::DecodingKey;
use std::{
    collections::HashMap,
    future::Future,
    panic::AssertUnwindSafe,
    sync::{Arc, LazyLock, RwLock},
};

use bb8_postgres::PostgresConnectionManager;
use chrono::{Duration, TimeZone};
use futures::FutureExt;
use rove_connector::Connector;
use tokio::task::JoinHandle;
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;

use lard_egress::patchwork::{
    create_patchwork_timeseries_table, fetch_timeseries_list_from_database, MessagePriority,
    MessagePriorityDefaultTable, PatchworkTables, PatchworkTimeseriesTable, Timerange,
};
use lard_ingestion::{
    get_conversions,
    util::{
        levels::{self, Level, LevelTable},
        permissions::{ParamPermit, ParamPermitTable, StationPermitTable},
        qc_pipelines::load_pipelines,
    },
};
use util::{DbPools, PooledPgConn};

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
        }
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

        let values = self
            .params
            .iter()
            .map(|param| match param.obstype {
                TestObsType::Scalar => scalar_val.to_string(),
                TestObsType::NonScalar => nonscalar_val.to_string(),
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

    fn end_time(&self) -> DateTime<Utc> {
        self.start_time + self.period * self.len as i32
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
        (99995, 5), // restricted
    ]);

    Arc::new(RwLock::new((param_permit, station_permit)))
}

pub fn mock_level_table() -> LevelTable {
    let param_level = HashMap::from([
        (211, Level::new(2, levels::Unit::M, levels::Direction::Up)),
        (81, Level::new(10, levels::Unit::M, levels::Direction::Up)),
        (3, Level::new(20, levels::Unit::Cm, levels::Direction::Down)),
        // Needed for IDF event
        (105, Level::new(2, levels::Unit::M, levels::Direction::Up)),
    ]);

    Arc::new(RwLock::new(param_level))
}

pub fn mock_auth_certs() -> DecodingKey {
    jsonwebtoken::DecodingKey::from_ec_pem(
        b"-----BEGIN PUBLIC KEY-----
MHYwEAYHKoZIzj0CAQYFK4EEACIDYgAETz7rFlJZ8IM7r53QKr7hF6GitWKpY3FN
tqdj2gL4EFqYX459/hpSh7w5hIW8k8mmftDz0Pm12CmV9MyvD1Lv1pucYyoJLobR
wARDennWSrMRamnmbyLO6jno3N9mNFtq
-----END PUBLIC KEY-----",
    )
    .unwrap()
}

pub fn mock_message_priority() -> MessagePriorityDefaultTable {
    let from: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 12, 31, 23, 0, 0).unwrap();
    let to: DateTime<Utc> = Utc.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap();

    MessagePriorityDefaultTable::from([
        (
            (508, 211),
            MessagePriority::new(9000, Timerange::new(Some(from), Some(to))),
        ),
        (
            (501, 211),
            MessagePriority::new(9000, Timerange::new(Some(to), None)),
        ),
        (
            (501, 225),
            MessagePriority::new(9000, Timerange::new(Some(from), None)),
        ),
        // The next ones are needed for IDF event
        (
            (514, 105),
            MessagePriority::new(100, Timerange::new(Some(from), Some(to))),
        ),
        (
            // This is needed to check that our patches are sorted
            (501, 105),
            MessagePriority::new(
                200,
                Timerange::new(Some(from - Duration::hours(1)), Some(from)),
            ),
        ),
        (
            (508, 105),
            MessagePriority::new(300, Timerange::new(Some(to), None)),
        ),
    ])
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
    let message_priority = mock_message_priority();
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
        mock_auth_certs(),
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

pub async fn e2e_test_wrapper<T: Future<Output = ()>>(test: T) {
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
