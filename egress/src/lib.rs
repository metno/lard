use std::sync::{Arc, PoisonError};

use axum::extract::FromRef;
use thiserror::Error;
use tokio::task::JoinError;
use tokio_util::sync::CancellationToken;
use tower_http::compression::CompressionLayer;
use tower_sessions::{SessionManagerLayer, SessionStore};

use ::util::{
    DbPools, EnvError, auth,
    stinfofacade::{self, level::LevelTable},
};
use aggregations::{Aggregation, AggregationParams, get_aggregation};

pub mod aggregations;
pub mod calculations;
pub mod http;
pub mod latest;
pub mod patchwork;
pub mod reports;
pub mod timeseries;
pub mod timeslice;
pub mod util;

pub const PATCHWORK_HTTP_REQUESTS_DURATION_SECONDS: &str =
    "patchwork_http_requests_duration_seconds";
pub const PATCHWORK_REQUESTS_RECEIVED: &str = "patchwork_requests_received";
pub const PATCHWORK_AVAILABLE_REQUESTS_RECEIVED: &str = "patchwork_available_requests_received";
pub const AGGREGATIONS_REQUESTS_RECEIVED: &str = "aggregations_requests_received";

#[derive(Error, Debug)]
pub enum Error {
    #[error("no conn string was provided")]
    NoConnString,
    #[error("postgres returned an error: {0}")]
    Database(#[from] tokio_postgres::Error),
    #[error("database pool could not return a connection: {0}")]
    Pool(#[from] bb8::RunError<tokio_postgres::Error>),
    #[error("join error: {0}")]
    Join(#[from] JoinError),
    #[error(transparent)]
    Auth(#[from] auth::Error),
    #[error("parse int error: {0}")]
    Parse(#[from] std::num::ParseIntError),
    #[error("parse float error: {0}")]
    ParseFloat(#[from] std::num::ParseFloatError),
    #[error("csv parsing error: {0}")]
    Csv(#[from] csv::Error),
    #[error("json parsing error: {0}")]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Env(#[from] EnvError),
    #[error("S3 error: {0}")]
    S3(#[from] s3::error::S3Error),
    #[error("RwLock was poisoned")]
    Lock,
    #[error("Utf8 error: {0}")]
    Utf8(#[from] std::str::Utf8Error),
    #[error("metadata cache error: {0}")]
    Stinfo(#[from] stinfofacade::Error),
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_: PoisonError<T>) -> Self {
        Self::Lock
    }
}

// TODO: move to utils?
type S3Bucket = Option<Arc<s3::Bucket>>;

#[derive(Clone, Debug)]
pub struct EgressState {
    db_pools: DbPools,
    // pub s3_client: S3Client,
    s3_bucket: S3Bucket,
    // patchwork table(s) - open and restricted
    patchwork_tables: patchwork::Tables,
    // level table - for looking up default levels for params
    level_table: LevelTable,
}

impl FromRef<EgressState> for DbPools {
    fn from_ref(state: &EgressState) -> Self {
        state.db_pools.clone() // the pool is internally reference counted, so no Arc needed
    }
}

impl FromRef<EgressState> for S3Bucket {
    fn from_ref(state: &EgressState) -> Self {
        state.s3_bucket.clone()
    }
}

impl FromRef<EgressState> for patchwork::Tables {
    fn from_ref(state: &EgressState) -> patchwork::Tables {
        state.patchwork_tables.clone()
    }
}

impl FromRef<EgressState> for LevelTable {
    fn from_ref(state: &EgressState) -> LevelTable {
        state.level_table.clone()
    }
}

pub async fn run<S: SessionStore + Clone>(
    db_pools: DbPools,
    s3_bucket: S3Bucket,
    patchwork_tables: patchwork::Tables,
    level_table: LevelTable,
    cancel_token: CancellationToken,
    session_layer: SessionManagerLayer<S>,
) {
    let router = http::router()
        .with_state(EgressState {
            db_pools,
            s3_bucket,
            patchwork_tables,
            level_table,
        })
        .layer(session_layer)
        .layer(CompressionLayer::new());

    // run it with hyper on localhost:3000
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, router)
        .with_graceful_shutdown(async move { cancel_token.cancelled().await })
        .await
        .unwrap();
}
