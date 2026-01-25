use std::{
    collections::HashMap,
    sync::{Arc, PoisonError},
};

use axum::{
    extract::{FromRef, MatchedPath, Request, State},
    middleware::{self, Next},
    response::{IntoResponse, Json},
    routing::post,
    Router,
};
use chrono::{DateTime, Utc};
use chronoutil::RelativeDuration;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

pub mod cron;
pub mod legacy;
pub mod util;
use ::util::{
    stinfofacade::{
        self,
        level::LevelTable,
        param::ParamTables,
        permissions::{self, PermitTables},
    },
    DbPools, PooledPgConn,
};

#[derive(Error, Debug)]
pub enum Error {
    #[error("postgres returned an error: {0}")]
    Database(#[from] tokio_postgres::Error),
    #[error("database pool could not return a connection: {0}")]
    Pool(#[from] bb8::RunError<tokio_postgres::Error>),
    #[error("parse error: {0}")]
    Parse(#[from] kldata::ParseError),
    #[error("qc system returned an error: {0}")]
    Qc(#[from] rove::scheduler::Error),
    #[error("loading qc pipelines returned an error: {0}")]
    QcLoad(#[from] rove::pipeline::Error),
    #[error("rove connector returned an error: {0}")]
    Connector(#[from] rove::data_switch::Error),
    #[error("RwLock was poisoned")]
    Lock,
    #[error("Could not read environment variable: {0}")]
    Env(String),
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
pub const FROM_TO_FUTURES_FAILURES: &str = "from_to_futures_failures";

/// Gets an environment variable, providing more details than calling std::env::var() directly.
pub fn getenv(key: &str) -> Result<String, Error> {
    std::env::var(key).map_err(|e| Error::Env(format!("{e}: {key}")))
}

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
    rove_connector: Arc<rove_connector::Connector>,
    qc_pipelines: Arc<HashMap<(i32, RelativeDuration), rove::Pipeline>>,
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

impl FromRef<IngestorState> for Arc<rove_connector::Connector> {
    fn from_ref(state: &IngestorState) -> Arc<rove_connector::Connector> {
        state.rove_connector.clone()
    }
}

impl FromRef<IngestorState> for Arc<HashMap<(i32, RelativeDuration), rove::Pipeline>> {
    fn from_ref(state: &IngestorState) -> Arc<HashMap<(i32, RelativeDuration), rove::Pipeline>> {
        state.qc_pipelines.clone()
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
    param_id: Option<i32>,
    value: ObsType,
    qc_usable: bool,
}

/// Generic container for a piece of data ready to be inserted into the DB
pub struct DataChunk {
    timestamp: DateTime<Utc>,
    time_resolution: Option<chronoutil::RelativeDuration>,
    data: Vec<Datum>,
}

pub struct QcProvenance {
    timeseries_id: i64,
    timestamp: DateTime<Utc>,
    // TODO: possible to avoid heap-allocating this?
    pipeline: String,
    // TODO: correct type?
    flag: i32,
    fail_condition: Option<String>,
}

// TODO: benchmark insertion of scalar and non-scalar together vs separately?
pub async fn insert_data(
    chunks: &Vec<DataChunk>,
    provenance: &[QcProvenance],
    conn: &mut PooledPgConn<'_>,
) -> Result<(), Error> {
    // TODO: the conflict resolution on this query is an imperfect solution, and needs improvement
    //
    // ---
    //
    // On periodic or consistency QC pipelines, we should be checking the provenance table to
    // decide how to update usable on a conflict, but here it should be fine not to since this is
    // fresh data.
    // The `AND` in the `DO UPDATE SET` subexpression better handles the case of resent data where
    // periodic checks might already have been run by defaulting to false. If the existing data was
    // only fresh checked, and the replacement is different, this could result in a false positive.
    // I think this is OK though since it should be a rare occurence and will be quickly cleared up
    // by a periodic run regardless.
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
    let query_provenance = conn
        .prepare(
            "INSERT INTO flags.confident_provenance (timeseries, obstime, pipeline, flag, fail_condition) \
                VALUES ($1, $2, $3, $4, $5) \
                ON CONFLICT ON CONSTRAINT confident_provenance_pkey \
                    DO UPDATE SET flag = EXCLUDED.flag, fail_condition = EXCLUDED.fail_condition",
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

    let mut futures = provenance
        .iter()
        .map(|qc_result| async {
            conn.execute(
                &query_provenance,
                &[
                    &qc_result.timeseries_id,
                    &qc_result.timestamp,
                    &qc_result.pipeline,
                    &qc_result.flag,
                    &qc_result.fail_condition,
                ],
            )
            .await
        })
        .collect::<FuturesUnordered<_>>();

    while let Some(res) = futures.next().await {
        res?;
    }

    Ok(())
}

pub async fn qc_fresh_data(
    chunks: &mut Vec<DataChunk>,
    rove_connector: &rove_connector::Connector,
    pipelines: &HashMap<(i32, RelativeDuration), rove::Pipeline>,
) -> Result<Vec<QcProvenance>, Error> {
    let mut qc_results: Vec<QcProvenance> = Vec::new();
    for chunk in chunks {
        let time_resolution = match chunk.time_resolution {
            Some(time_resolution) => time_resolution,
            // if there's no time_resolution, we can't QC
            None => continue,
        };

        for datum in chunk.data.iter_mut() {
            let inner_datum = match datum.value {
                // TODO: should we continue if inner_datum is not Some?
                ObsType::Scalar(x) => x,
                ObsType::NonScalar(_) => continue,
            };
            let param_id = match datum.param_id {
                Some(id) => id,
                None => continue,
            };
            let pipeline = match pipelines.get(&(param_id, time_resolution)) {
                Some(pipeline) => pipeline,
                None => continue,
            };
            let data_cache = rove_connector
                .fetch_context(
                    datum.timeseries_id,
                    chunk.timestamp,
                    time_resolution,
                    pipeline.num_leading_required,
                    inner_datum,
                )
                .await?;
            let rove_output = rove::Scheduler::schedule_tests(pipeline, data_cache)?;

            let first_fail = rove_output.iter().find(|check_result| {
                // first here because there should only be one timeseries
                if let Some(result) = check_result.results.first() {
                    // first here because there should only be one qced datum in the timeseries
                    if let Some(flag) = result.values.first() {
                        return *flag == rove::Flag::Fail;
                    }
                }
                false
            });

            let (flag, fail_condition) = match first_fail {
                Some(check_result) => (1, Some(check_result.check.clone())),
                None => (0, None),
            };

            datum.qc_usable = flag == 0;

            qc_results.push(QcProvenance {
                timeseries_id: datum.timeseries_id,
                timestamp: chunk.timestamp,
                // TODO: should this encode more info? In theory the param/type can be deduced from the DB anyway
                pipeline: "fresh".to_string(),
                flag,
                fail_condition,
            });
        }
    }

    Ok(qc_results)
}

pub async fn qc_and_insert_data(
    chunks: &mut Vec<DataChunk>,
    rove_connector: &rove_connector::Connector,
    pipelines: &HashMap<(i32, RelativeDuration), rove::Pipeline>,
    conn: &mut PooledPgConn<'_>,
) -> Result<(), Error> {
    // TODO: handling of restricted data in QC? currently rove_connector only looks at the open db
    let provenance = match qc_fresh_data(chunks, rove_connector, pipelines).await {
        Ok(provenance) => provenance,
        Err(e) => {
            error!("Failed to qc data: {}", e);
            metrics::counter!(QC_FAILURES).increment(1);
            Vec::new()
        }
    };

    insert_data(chunks, &provenance, conn).await
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
    State(rove_connector): State<Arc<rove_connector::Connector>>,
    State(qc_pipelines): State<Arc<HashMap<(i32, RelativeDuration), rove::Pipeline>>>,
    body: String,
) -> Json<KldataResp> {
    metrics::counter!(KLDATA_MESSAGES_RECEIVED).increment(1);

    let result: Result<usize, Error> = async {
        let mut open_conn = pools.open.get().await?;
        let mut restricted_conn = pools.restricted.get().await?;

        let (message_id, obsinn_chunk) = parse_kldata(&body, param_tables.clone())?;

        let (mut open_data, mut restricted_data) = filter_and_label_kldata(
            obsinn_chunk,
            &mut open_conn,
            &mut restricted_conn,
            param_tables,
            permit_table,
            level_table,
        )
        .await?;

        qc_and_insert_data(
            &mut open_data,
            &rove_connector,
            &qc_pipelines,
            &mut open_conn,
        )
        .await?;
        qc_and_insert_data(
            &mut restricted_data,
            &rove_connector,
            &qc_pipelines,
            &mut restricted_conn,
        )
        .await?;

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
    rove_connector: rove_connector::Connector,
    qc_pipelines: HashMap<(i32, RelativeDuration), rove::Pipeline>,
    cancel_token: CancellationToken,
) -> Result<(), Error> {
    // TODO: This should be fine without Arc, we can just clone it as the internal db_pool is
    // already reference counted
    let rove_connector = Arc::new(rove_connector);
    let qc_pipelines = Arc::new(qc_pipelines);

    // build our application with a single route
    let app = Router::new()
        .route("/kldata", post(handle_kldata))
        .route_layer(middleware::from_fn(track_request_duration))
        .with_state(IngestorState {
            db_pools,
            param_tables,
            permit_tables,
            level_table,
            rove_connector,
            qc_pipelines,
        });

    // run our app with hyper, listening globally on port 3001
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3001").await?;
    info!("Ingestion server started!");
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { cancel_token.cancelled().await })
        .await?;

    Ok(())
}
