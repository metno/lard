use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use tokio::task::JoinHandle;
use tokio_postgres::NoTls;

use lard_ingestion::permissions::{ParamPermit, ParamPermitTable, StationPermitTable};
use lard_ingestion::{PgConnectionPool, PooledPgConn};

const CONNECT_STRING: &str = "host=localhost user=postgres dbname=postgres password=postgres";
pub const PARAMCONV_CSV: &str = "../ingestion/resources/paramconversions.csv";

#[derive(Debug, Deserialize)]
pub struct IngestorResponse {
    pub message: String,
    pub message_id: usize,
    pub res: u8,
    pub retry: bool,
}

#[derive(Debug, Deserialize)]
pub struct StationsResponse {
    pub tseries: Vec<Tseries>,
}

#[derive(Debug, Deserialize)]
pub struct Tseries {
    pub regularity: String,
    pub data: Vec<f64>,
    // header: ...
}

#[derive(Debug, Deserialize)]
pub struct LatestResponse {
    pub data: Vec<Data>,
}

#[derive(Debug, Deserialize)]
pub struct Data {
    // TODO: Missing param_id here?
    pub value: f64,
    pub timestamp: DateTime<Utc>,
    pub station_id: i32,
    // loc: {lat, lon, hamsl, hag}
}

#[derive(Debug, Deserialize)]
pub struct TimesliceResponse {
    pub tslices: Vec<Tslice>,
}

#[derive(Debug, Deserialize)]
pub struct Tslice {
    pub timestamp: DateTime<Utc>,
    pub param_id: i32,
    pub data: Vec<f64>,
}

pub async fn init_api_server() -> JoinHandle<()> {
    tokio::spawn(lard_api::run(CONNECT_STRING))
}

pub async fn init_db_pool() -> Result<PgConnectionPool, tokio_postgres::Error> {
    let manager = PostgresConnectionManager::new_from_stringlike(CONNECT_STRING, NoTls)?;
    let pool = Pool::builder().build(manager).await?;
    Ok(pool)
}

pub fn mock_permit_tables() -> Arc<RwLock<(ParamPermitTable, StationPermitTable)>> {
    let param_permit = HashMap::from([
        // station_id -> (type_id, param_id, permit_id)
        (1, vec![ParamPermit::new(0, 0, 0)]),
        (2, vec![ParamPermit::new(0, 0, 1)]), // open
    ]);

    let station_permit = HashMap::from([
        // station_id -> permit_id
        (1, 0),
        (2, 0),
        (3, 0),
        (4, 1), // open
        // used in e2e tests
        (20000, 1),
        (30000, 1),
        (11000, 1),
        (12000, 1),
        (12100, 1),
        (13000, 1),
        (40000, 1),
    ]);

    Arc::new(RwLock::new((param_permit, station_permit)))
}

pub async fn number_of_data_rows(conn: &PooledPgConn<'_>, ts_id: i32) -> usize {
    let rows = conn
        .query(
            "SELECT * FROM public.data
                WHERE timeseries = $1",
            &[&ts_id],
        )
        .await
        .unwrap();

    rows.len()
}

pub async fn init_ingestion_server(
) -> JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>> {
    tokio::spawn(lard_ingestion::run(
        CONNECT_STRING,
        PARAMCONV_CSV,
        mock_permit_tables(),
    ))
}
