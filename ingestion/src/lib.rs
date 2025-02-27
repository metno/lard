use axum::{
    extract::{FromRef, MatchedPath, Request, State},
    middleware::{self, Next},
    response::{IntoResponse, Json},
    routing::post,
    Router,
};
use bb8::PooledConnection;
use bb8_postgres::PostgresConnectionManager;
use chrono::{DateTime, Utc};
use chronoutil::RelativeDuration;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};
use thiserror::Error;
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

#[cfg(feature = "kafka")]
pub mod kvkafka;
pub mod permissions;
pub mod qc_pipelines;
use permissions::{ParamPermitTable, StationPermitTable};

#[derive(Error, Debug)]
pub enum Error {
    #[error("postgres returned an error: {0}")]
    Database(#[from] tokio_postgres::Error),
    #[error("database pool could not return a connection: {0}")]
    Pool(#[from] bb8::RunError<tokio_postgres::Error>),
    #[error("parse error: {0}")]
    Parse(String),
    #[error("qc system returned an error: {0}")]
    Qc(#[from] rove::scheduler::Error),
    #[error("rove connector returned an error: {0}")]
    Connector(#[from] rove::data_switch::Error),
    #[error("RwLock was poisoned: {0}")]
    Lock(String),
    #[error("Could not read environment variable: {0}")]
    Env(String),
}

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
            (Lock(a), Lock(b)) => a == b,
            (Env(a), Env(b)) => a == b,
            _ => false,
        }
    }
}

pub type PgConnectionPool = bb8::Pool<PostgresConnectionManager<NoTls>>;

pub type PooledPgConn<'a> = PooledConnection<'a, PostgresConnectionManager<NoTls>>;

/// Type that maps a subset of columns from the Stinfosys 'param' table
#[derive(Clone, Debug)]
pub struct ReferenceParam {
    /// Numerical identifier of the parameter (e.g., 212)
    id: i32,
    /// Descriptive identifier of the paramater (e.g., 'air_temperature')
    element_id: String,
    /// Whether the parameter is marked as scalar in Stinfosys
    is_scalar: bool,
}

type ParamConversions = Arc<HashMap<String, ReferenceParam>>;

#[derive(Clone, Debug)]
struct IngestorState {
    db_pool: PgConnectionPool,
    param_conversions: ParamConversions, // converts param codes to element ids
    permit_tables: Arc<RwLock<(ParamPermitTable, StationPermitTable)>>,
    rove_connector: Arc<rove_connector::Connector>,
    qc_pipelines: Arc<HashMap<(i32, RelativeDuration), rove::Pipeline>>,
}

impl FromRef<IngestorState> for PgConnectionPool {
    fn from_ref(state: &IngestorState) -> PgConnectionPool {
        state.db_pool.clone() // the pool is internally reference counted, so no Arc needed
    }
}

impl FromRef<IngestorState> for ParamConversions {
    fn from_ref(state: &IngestorState) -> ParamConversions {
        state.param_conversions.clone()
    }
}

impl FromRef<IngestorState> for Arc<RwLock<(ParamPermitTable, StationPermitTable)>> {
    fn from_ref(state: &IngestorState) -> Arc<RwLock<(ParamPermitTable, StationPermitTable)>> {
        state.permit_tables.clone()
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
#[derive(Debug, PartialEq)]
pub enum ObsType<'a> {
    Scalar(f64),
    NonScalar(&'a str),
}

pub struct Datum<'a> {
    timeseries_id: i64,
    // needed for QC
    param_id: i32,
    value: ObsType<'a>,
    qc_usable: bool,
}

/// Generic container for a piece of data ready to be inserted into the DB
pub struct DataChunk<'a> {
    timestamp: DateTime<Utc>,
    time_resolution: Option<chronoutil::RelativeDuration>,
    data: Vec<Datum<'a>>,
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
    chunks: &Vec<DataChunk<'_>>,
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
                ON CONFLICT ON CONSTRAINT unique_data_timeseries_obstime \
                    DO UPDATE SET obsvalue = EXCLUDED.obsvalue, \
                    qc_usable = public.data.qc_usable AND EXCLUDED.qc_usable",
        )
        .await?;

    let query_nonscalar = conn
        .prepare(
            "INSERT INTO public.nonscalar_data (timeseries, obstime, obsvalue, qc_usable) \
                VALUES ($1, $2, $3, $4) \
                ON CONFLICT ON CONSTRAINT unique_nonscalar_data_timeseries_obstime \
                    DO UPDATE SET obsvalue = EXCLUDED.obsvalue, \
                    qc_usable = public.nonscalar_data.qc_usable AND EXCLUDED.qc_usable",
        )
        .await?;
    let query_provenance = conn
        .prepare(
            "INSERT INTO flags.confident_provenance (timeseries, obstime, pipeline, flag, fail_condition) \
                VALUES ($1, $2, $3, $4, $5) \
                ON CONFLICT ON CONSTRAINT unique_confident_provenance_timeseries_obstime_pipeline \
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
    chunks: &mut Vec<DataChunk<'_>>,
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
                ObsType::Scalar(x) => x,
                ObsType::NonScalar(_) => continue,
            };
            let pipeline = match pipelines.get(&(datum.param_id, time_resolution)) {
                Some(pipeline) => pipeline,
                None => continue,
            };
            let data_cache = rove_connector
                .fetch_context(
                    datum.timeseries_id,
                    chunk.timestamp,
                    time_resolution,
                    pipeline.num_leading_required,
                    Some(inner_datum),
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
    State(pool): State<PgConnectionPool>,
    State(param_conversions): State<ParamConversions>,
    State(permit_table): State<Arc<RwLock<(ParamPermitTable, StationPermitTable)>>>,
    State(rove_connector): State<Arc<rove_connector::Connector>>,
    State(qc_pipelines): State<Arc<HashMap<(i32, RelativeDuration), rove::Pipeline>>>,
    body: String,
) -> Json<KldataResp> {
    metrics::counter!("kldata_messages_received").increment(1);

    let result: Result<usize, Error> = async {
        let mut conn = pool.get().await?;

        let (message_id, obsinn_chunk) = parse_kldata(&body, param_conversions.clone())?;

        let mut data =
            filter_and_label_kldata(obsinn_chunk, &mut conn, param_conversions, permit_table)
                .await?;

        // TODO: should we tolerate failure here? Perhaps there should be metric for this?
        let provenance = qc_fresh_data(&mut data, &rove_connector, &qc_pipelines).await?;

        insert_data(&data, &provenance, &mut conn).await?;

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
            metrics::counter!("kldata_failures").increment(1);
            error!("failed to ingest kldata message: {}, body: {}", e, body);
            // TODO: log errors?
            Json(KldataResp {
                message: e.to_string(),
                message_id: 0, // TODO: some clever way to get the message id still if possible?
                res: 1,
                retry: !matches!(e, Error::Parse(_)),
            })
        }
    }
}

fn get_conversions(filename: &str) -> Result<ParamConversions, csv::Error> {
    Ok(Arc::new(
        csv::Reader::from_path(filename)
            .unwrap()
            .into_records()
            .map(|record_result| {
                record_result.map(|record| {
                    (
                        record.get(1).unwrap().to_owned(), // param code
                        (ReferenceParam {
                            id: record.get(0).unwrap().parse::<i32>().unwrap(),
                            element_id: record.get(2).unwrap().to_owned(),
                            is_scalar: match record.get(3).unwrap() {
                                "t" => true,
                                "f" => false,
                                _ => unreachable!(),
                            },
                        }),
                    )
                })
            })
            .collect::<Result<HashMap<String, ReferenceParam>, csv::Error>>()?,
    ))
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

    let latency = start.elapsed().as_secs_f64();
    let status = response.status().as_u16().to_string();

    let labels = [("method", method), ("path", path), ("status", status)];

    metrics::histogram!("http_requests_duration_seconds", &labels).record(latency);

    response
}

pub async fn run(
    db_pool: PgConnectionPool,
    param_conversion_path: &str,
    permit_tables: Arc<RwLock<(ParamPermitTable, StationPermitTable)>>,
    rove_connector: rove_connector::Connector,
    qc_pipelines: HashMap<(i32, RelativeDuration), rove::Pipeline>,
    cancel_token: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // set up param conversion map
    let param_conversions = get_conversions(param_conversion_path)?;

    // TODO: This should be fine without Arc, we can just clone it as the internal db_pool is
    // already reference counted
    let rove_connector = Arc::new(rove_connector);
    let qc_pipelines = Arc::new(qc_pipelines);

    // build our application with a single route
    let app = Router::new()
        .route("/kldata", post(handle_kldata))
        .route_layer(middleware::from_fn(track_request_duration))
        .with_state(IngestorState {
            db_pool,
            param_conversions,
            permit_tables,
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
