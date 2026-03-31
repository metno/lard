use std::sync::PoisonError;

use axum::{
    Router,
    extract::{FromRef, MatchedPath, Request, State},
    middleware::{self, Next},
    response::{IntoResponse, Json},
    routing::post,
};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

pub mod legacy;
pub mod util;
use ::util::{
    DbPools, EnvError, PooledPgConn,
    stinfofacade::{
        self,
        level::LevelTable,
        param::ParamTables,
        permissions::{self, PermitTables},
    },
};

#[derive(Error, Debug)]
pub enum Error {
    #[error("postgres returned an error: {0}")]
    Database(#[from] tokio_postgres::Error),
    #[error("database pool could not return a connection: {0}")]
    Pool(#[from] bb8::RunError<tokio_postgres::Error>),
    #[error("parse error: {0}")]
    Parse(#[from] kldata::ParseError),
    #[error("RwLock was poisoned")]
    Lock,
    #[error(transparent)]
    Env(#[from] EnvError),
    #[error("metadata cache error: {0}")]
    Stinfo(#[from] stinfofacade::Error),
    #[error("Failed to join tasks: {0}")]
    Join(#[from] tokio::task::JoinError),
    #[error(transparent)]
    Csv(#[from] csv::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Legacy(#[from] legacy::Error),
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_: PoisonError<T>) -> Self {
        Self::Lock
    }
}

pub const HTTP_REQUESTS_DURATION_SECONDS: &str = "http_requests_duration_seconds";
pub const KLDATA_MESSAGES_RECEIVED: &str = "kldata_messages_received";
pub const KLDATA_FAILURES: &str = "kldata_failures";
pub const QC_FAILURES: &str = "qc_failures";
pub const KAFKA_RAW_MESSAGES_RECEIVED: &str = "kafka_raw_messages_received";
pub const KAFKA_RAW_FAILURES: &str = "kafka_raw_failures";
pub const KAFKA_CHECKED_MESSAGES_RECEIVED: &str = "kafka_checked_messages_received";
pub const KAFKA_CHECKED_FAILURES: &str = "kafka_checked_failures";
pub const SCALAR_DATAPOINTS: &str = "scalar_datapoints";
pub const NONSCALAR_DATAPOINTS: &str = "nonscalar_datapoints";
pub use ::util::FROM_TO_FUTURES_FAILURES;

impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        use Error::*;

        match (self, other) {
            (Database(a), Database(b)) => a.to_string() == b.to_string(),
            (Pool(a), Pool(b)) => a.to_string() == b.to_string(),
            (Parse(a), Parse(b)) => a == b,
            (Lock, Lock) => true,
            (Env(a), Env(b)) => a == b,
            _ => false,
        }
    }
}

#[derive(Clone, Debug)]
struct IngestorState {
    db_pools: DbPools,
    param_tables: ParamTables,
    permit_tables: PermitTables,
    level_table: LevelTable,
}

impl FromRef<IngestorState> for DbPools {
    fn from_ref(state: &IngestorState) -> DbPools {
        state.db_pools.clone() // the pool is internally reference counted, so no Arc needed
    }
}

impl FromRef<IngestorState> for ParamTables {
    fn from_ref(state: &IngestorState) -> ParamTables {
        state.param_tables.clone()
    }
}

impl FromRef<IngestorState> for PermitTables {
    fn from_ref(state: &IngestorState) -> PermitTables {
        state.permit_tables.clone()
    }
}

impl FromRef<IngestorState> for LevelTable {
    fn from_ref(state: &IngestorState) -> LevelTable {
        state.level_table.clone()
    }
}

/// Represents the different Data types observation can have
#[derive(Clone, Debug, PartialEq)]
pub enum ObsType {
    Scalar(Option<f64>),
    NonScalar(Option<String>),
}

pub struct Datum {
    timeseries_id: i64,
    // needed for QC
    _param_id: Option<i32>,
    value: ObsType,
    // FIXME: currently not set usefully since we removed ROVE
    qc_usable: bool,
}

/// Generic container for a piece of data ready to be inserted into the DB
pub struct DataChunk {
    timestamp: DateTime<Utc>,
    _time_resolution: Option<chronoutil::RelativeDuration>,
    data: Vec<Datum>,
}

// TODO: benchmark insertion of scalar and non-scalar together vs separately?
pub async fn insert_data(
    chunks: &Vec<DataChunk>,
    conn: &mut PooledPgConn<'_>,
) -> Result<(), Error> {
    // TODO: the conflict resolution on this query is an imperfect solution, and needs improvement
    //
    // ---
    //
    // I learned from Søren that obsinn and kvalobs organise updates and deletions by sending new
    // messages that overwrite previous messages. The catch is that the new message does not need
    // to contain all the params of the old message (or indeed any of them), and any that are left
    // out should be deleted.
    //
    // We either need to scan for and delete matching data for every request obsinn sends us, or
    // get obsinn to adopt and use a new endpoint or message format to signify deletion. The latter
    // option seems to me the much better solution, and Søren seemed receptive when I spoke to him,
    // but we would need to hash out the details of such and endpoint/format with him before we can
    // implement it here.
    let query_scalar = conn
        .prepare(
            "INSERT INTO public.data (timeseries, obstime, obsvalue, qc_usable) \
                VALUES ($1, $2, $3, $4) \
                ON CONFLICT ON CONSTRAINT data_pkey \
                    DO UPDATE SET obsvalue = EXCLUDED.obsvalue, \
                    qc_usable = public.data.qc_usable AND EXCLUDED.qc_usable",
        )
        .await?;

    let query_nonscalar = conn
        .prepare(
            "INSERT INTO public.nonscalar_data (timeseries, obstime, obsvalue, qc_usable) \
                VALUES ($1, $2, $3, $4) \
                ON CONFLICT ON CONSTRAINT nonscalar_data_pkey \
                    DO UPDATE SET obsvalue = EXCLUDED.obsvalue, \
                    qc_usable = public.nonscalar_data.qc_usable AND EXCLUDED.qc_usable",
        )
        .await?;

    // TODO: should we flat map into one FuturesUnordered instead of for looping?
    for chunk in chunks {
        let mut futures = chunk
            .data
            .iter()
            .map(|datum| async {
                match &datum.value {
                    ObsType::Scalar(val) => {
                        conn.execute(
                            &query_scalar,
                            &[
                                &datum.timeseries_id,
                                &chunk.timestamp,
                                &val,
                                &datum.qc_usable,
                            ],
                        )
                        .await
                    }
                    ObsType::NonScalar(val) => {
                        conn.execute(
                            &query_nonscalar,
                            &[
                                &datum.timeseries_id,
                                &chunk.timestamp,
                                &val,
                                &datum.qc_usable,
                            ],
                        )
                        .await
                    }
                }
            })
            .collect::<FuturesUnordered<_>>();

        while let Some(res) = futures.next().await {
            res?;
        }
    }

    Ok(())
}

pub mod kldata;
use kldata::{filter_and_label_kldata, parse_kldata};

/// Format of response Obsinn expects from this API
#[derive(Debug, Serialize, Deserialize)]
pub struct KldataResp {
    /// Optional message indicating what happened to the data
    pub message: String,
    /// Should be the same message_id we received in the request
    pub message_id: usize,
    /// Result indicator, 0 means success, anything else means fail.
    // Kvalobs uses some specific numbers to denote specific errors with this, I don't much see
    // the point, the only information Obsinn can really action on as far as I can tell, is whether
    // we failed and whether it can retry
    pub res: u8, // TODO: Should be an enum?
    /// Indicates whether Obsinn should try to send the message again
    pub retry: bool,
}

async fn handle_kldata(
    State(pools): State<DbPools>,
    State(param_tables): State<ParamTables>,
    State(permit_table): State<PermitTables>,
    State(level_table): State<LevelTable>,
    body: String,
) -> Json<KldataResp> {
    metrics::counter!(KLDATA_MESSAGES_RECEIVED).increment(1);

    let result: Result<usize, Error> = async {
        let mut open_conn = pools.open.get().await?;
        let mut restricted_conn = pools.restricted.get().await?;

        let (message_id, obsinn_chunk) = parse_kldata(&body, param_tables.clone())?;

        let (open_data, restricted_data) = filter_and_label_kldata(
            obsinn_chunk,
            &mut open_conn,
            &mut restricted_conn,
            param_tables,
            permit_table,
            level_table,
        )
        .await?;

        insert_data(&open_data, &mut open_conn).await?;
        insert_data(&restricted_data, &mut restricted_conn).await?;

        Ok(message_id)
    }
    .await;

    match result {
        Ok(message_id) => Json(KldataResp {
            message: "".into(),
            message_id,
            res: 0,
            retry: false,
        }),
        Err(e) => {
            metrics::counter!(KLDATA_FAILURES).increment(1);
            error!("failed to ingest kldata message: {}, body: {}", e, body);
            Json(KldataResp {
                message: e.to_string(),
                message_id: 0, // TODO: some clever way to get the message id still if possible?
                res: 1,
                retry: !matches!(e, Error::Parse(_)),
            })
        }
    }
}

/// Middleware function that runs around a request, so we can record how long it took
async fn track_request_duration(req: Request, next: Next) -> impl IntoResponse {
    let start = std::time::Instant::now();
    let path = if let Some(matched_path) = req.extensions().get::<MatchedPath>() {
        matched_path.as_str().to_owned()
    } else {
        req.uri().path().to_owned()
    };
    let method = req.method().to_string();

    let response = next.run(req).await;

    let duration = start.elapsed().as_secs_f64();
    let status = response.status().as_u16().to_string();

    let labels = [("method", method), ("path", path), ("status", status)];

    metrics::histogram!("http_requests_duration_seconds", &labels).record(duration);

    response
}

pub async fn run(
    db_pools: DbPools,
    param_tables: ParamTables,
    permit_tables: PermitTables,
    level_table: LevelTable,
    cancel_token: CancellationToken,
) -> Result<(), Error> {
    // build our application with a single route
    let app = Router::new()
        .route("/kldata", post(handle_kldata))
        .route_layer(middleware::from_fn(track_request_duration))
        .with_state(IngestorState {
            db_pools,
            param_tables,
            permit_tables,
            level_table,
        });

    // run our app with hyper, listening globally on port 3001
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3001").await?;
    info!("Ingestion server started!");
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { cancel_token.cancelled().await })
        .await?;

    Ok(())
}
