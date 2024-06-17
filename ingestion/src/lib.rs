use axum::{
    extract::{FromRef, State},
    response::Json,
    routing::post,
    Router,
};
use bb8::PooledConnection;
use bb8_postgres::PostgresConnectionManager;
use chrono::{DateTime, Utc};
use futures::{stream::FuturesUnordered, StreamExt};
use serde::Serialize;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};
use thiserror::Error;
use tokio_postgres::NoTls;

pub mod permissions;
use permissions::{ParamPermitTable, StationPermitTable};

#[derive(Error, Debug)]
pub enum Error {
    #[error("postgres returned an error: {0}")]
    Database(#[from] tokio_postgres::Error),
    #[error("database pool could not return a connection: {0}")]
    Pool(#[from] bb8::RunError<tokio_postgres::Error>),
    #[error("parse error: {0}")]
    Parse(String),
    #[error("RwLock was poisoned: {0}")]
    Lock(String),
    #[error("Could not read environment variable: {0}")]
    Env(#[from] std::env::VarError),
}

impl PartialEq for Error {
    fn eq(&self, other: &Self) -> bool {
        use Error::*;

        match (self, other) {
            (Database(a), Database(b)) => a.to_string() == b.to_string(),
            (Pool(a), Pool(b)) => a.to_string() == b.to_string(),
            (Parse(a), Parse(b)) => a == b,
            (Lock(a), Lock(b)) => a == b,
            (Env(a), Env(b)) => a.to_string() == b.to_string(),
            _ => false,
        }
    }
}

pub type PgConnectionPool = bb8::Pool<PostgresConnectionManager<NoTls>>;

pub type PooledPgConn<'a> = PooledConnection<'a, PostgresConnectionManager<NoTls>>;

type ParamConversions = Arc<HashMap<String, (String, i32)>>;

#[derive(Clone, Debug)]
struct IngestorState {
    db_pool: PgConnectionPool,
    param_conversions: ParamConversions, // converts param codes to element ids
    permit_tables: Arc<RwLock<(ParamPermitTable, StationPermitTable)>>,
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

/// Generic container for a piece of data ready to be inserted into the DB
pub struct Datum {
    timeseries_id: i32,
    timestamp: DateTime<Utc>,
    value: f32,
}

pub type Data = Vec<Datum>;

pub async fn insert_data(data: Data, conn: &mut PooledPgConn<'_>) -> Result<(), Error> {
    // TODO: the conflict resolution on this query is an imperfect solution, and needs improvement
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
    let query = conn
        .prepare(
            "INSERT INTO public.data (timeseries, obstime, obsvalue) \
                VALUES ($1, $2, $3) \
                ON CONFLICT ON CONSTRAINT unique_data_timeseries_obstime \
                    DO UPDATE SET obsvalue = EXCLUDED.obsvalue",
        )
        .await?;

    let mut futures = data
        .iter()
        .map(|datum| async {
            conn.execute(
                &query,
                &[&datum.timeseries_id, &datum.timestamp, &datum.value],
            )
            .await
        })
        .collect::<FuturesUnordered<_>>();

    while let Some(res) = futures.next().await {
        res?;
    }

    Ok(())
}

pub mod kldata;
use kldata::{filter_and_label_kldata, parse_kldata};

/// Format of response Obsinn expects from this API
#[derive(Debug, Serialize)]
struct KldataResp {
    /// Optional message indicating what happened to the data
    message: String,
    /// Should be the same message_id we received in the request
    message_id: usize,
    /// Result indicator, 0 means success, anything else means fail.
    // Kvalobs uses some specific numbers to denote specific errors with this, I don't much see
    // the point, the only information Obsinn can really action on as far as I can tell, is whether
    // we failed and whether it can retry
    res: u8, // TODO: Should be an enum?
    /// Indicates whether Obsinn should try to send the message again
    retry: bool,
}

async fn handle_kldata(
    State(pool): State<PgConnectionPool>,
    State(param_conversions): State<ParamConversions>,
    State(permit_table): State<Arc<RwLock<(ParamPermitTable, StationPermitTable)>>>,
    body: String,
) -> Json<KldataResp> {
    let result: Result<usize, Error> = async {
        let mut conn = pool.get().await?;

        let (message_id, obsinn_chunk) = parse_kldata(&body)?;

        let data =
            filter_and_label_kldata(obsinn_chunk, &mut conn, param_conversions, permit_table)
                .await?;

        insert_data(data, &mut conn).await?;

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
        Err(e) => Json(KldataResp {
            message: e.to_string(),
            message_id: 0, // TODO: some clever way to get the message id still if possible?
            res: 1,
            retry: !matches!(e, Error::Parse(_)),
        }),
    }
}

pub async fn run(
    connect_string: &str,
    param_conversion_path: &str,
    permit_tables: Arc<RwLock<(ParamPermitTable, StationPermitTable)>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // set up postgres connection pool
    let manager = PostgresConnectionManager::new_from_stringlike(connect_string, NoTls)?;
    let db_pool = bb8::Pool::builder().build(manager).await?;

    // set up param conversion map
    // TODO: extract to separate function?
    let param_conversions = Arc::new(
        csv::Reader::from_path(param_conversion_path)
            .unwrap()
            .into_records()
            .map(|record_result| {
                record_result.map(|record| {
                    (
                        record.get(1).unwrap().to_owned(), // param code
                        (
                            record.get(2).unwrap().to_owned(),              // element id
                            record.get(0).unwrap().parse::<i32>().unwrap(), // param id
                        ),
                    )
                })
            })
            .collect::<Result<HashMap<String, (String, i32)>, csv::Error>>()?,
    );

    // build our application with a single route
    let app = Router::new()
        .route("/kldata", post(handle_kldata))
        .with_state(IngestorState {
            db_pool,
            param_conversions,
            permit_tables,
        });

    // run our app with hyper, listening globally on port 3001
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3001").await?;
    axum::serve(listener, app).await?;

    Ok(())
}