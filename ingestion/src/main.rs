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
use permissions::{fetch_open_permits, PermitTable};

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
}

type PgConnectionPool = bb8::Pool<PostgresConnectionManager<NoTls>>;

pub type PooledPgConn<'a> = PooledConnection<'a, PostgresConnectionManager<NoTls>>;

type ParamConversions = Arc<HashMap<String, (String, i32)>>;

#[derive(Clone, Debug)]
struct IngestorState {
    db_pool: PgConnectionPool,
    param_conversions: ParamConversions, // converts param codes to element ids
    permit_table: Arc<RwLock<PermitTable>>,
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

impl FromRef<IngestorState> for Arc<RwLock<PermitTable>> {
    fn from_ref(state: &IngestorState) -> Arc<RwLock<PermitTable>> {
        state.permit_table.clone()
    }
}

pub struct Datum {
    timeseries_id: i32,
    timestamp: DateTime<Utc>,
    value: f32,
}
pub type Data = Vec<Datum>;

async fn insert_data(data: Data, conn: &mut PooledPgConn<'_>) -> Result<(), Error> {
    let query = conn
        .prepare(
            "INSERT INTO public.data (timeseries, obstime, obsvalue) \
                VALUES ($1, $2, $3) \
                ON CONFLICT DO NOTHING", // TODO: figure out whether this should be nothing or update
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

#[derive(Debug, Serialize)]
struct KldataResp {
    message: String,
    message_id: usize,
    res: u8, // TODO: Should be an enum?
    retry: bool,
}

async fn handle_kldata(
    State(pool): State<PgConnectionPool>,
    State(param_conversions): State<ParamConversions>,
    State(permit_table): State<Arc<RwLock<PermitTable>>>,
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

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // TODO: use clap for argument parsing
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 4 {
        panic!("not enough args passed in, at least host, user, dbname needed, optionally password")
    }

    let mut connect_string = format!("host={} user={} dbname={}", &args[1], &args[2], &args[3]);
    if args.len() > 4 {
        connect_string.push_str(" password=");
        connect_string.push_str(&args[4])
    }

    // set up postgres connection pool
    let manager = PostgresConnectionManager::new_from_stringlike(connect_string, NoTls)?;
    let db_pool = bb8::Pool::builder().build(manager).await?;

    // set up param conversion map
    let param_conversions = Arc::new(
        csv::Reader::from_path("resources/paramconversions.csv")
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

    let permit_table = Arc::new(RwLock::new(fetch_open_permits().await?));
    let background_permit_table = permit_table.clone();

    // background task to refresh permit table every 30 mins
    tokio::task::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30 * 60));

        loop {
            interval.tick().await;
            async {
                // TODO: better error handling here? Nothing is listening to what returns on this task
                // but we could surface failures in metrics. Also we maybe don't want to bork the task
                // forever if these functions fail
                let new_table = fetch_open_permits().await.unwrap();
                let mut table = background_permit_table.write().unwrap();
                *table = new_table;
            }
            .await;
        }
    });

    // build our application with a single route
    let app = Router::new()
        .route("/kldata", post(handle_kldata))
        .with_state(IngestorState {
            db_pool,
            param_conversions,
            permit_table,
        });

    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3001").await?;
    axum::serve(listener, app).await?;

    Ok(())
}
