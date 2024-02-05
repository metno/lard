use axum::{extract::State, response::Json, routing::post, Router};
use bb8::PooledConnection;
use bb8_postgres::PostgresConnectionManager;
use chrono::{DateTime, Utc};
use futures::{stream::FuturesUnordered, StreamExt};
use serde::Serialize;
use tokio_postgres::NoTls;

type PgConnectionPool = bb8::Pool<PostgresConnectionManager<NoTls>>;

pub type PooledPgConn<'a> = PooledConnection<'a, PostgresConnectionManager<NoTls>>;

pub struct Datum {
    timeseries_id: i32,
    timestamp: DateTime<Utc>,
    value: f32,
}
pub type Data = Vec<Datum>;

async fn insert_data(
    data: Data,
    conn: &mut PooledPgConn<'_>,
) -> Result<(), Box<dyn std::error::Error>> {
    let query = conn
        .prepare(
            "INSERT INTO public.data (timeseries, obstime, obsvalue) \
                VALUES ($1, $2, $3) \
                ON CONFLICT DO NOTHING", // TODO: figure out whether this should be nothing or update
        )
        .await
        .unwrap();

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
        res.unwrap();
    }

    Ok(())
}

pub mod kldata;
use kldata::{label_kldata, parse_kldata};

#[derive(Debug, Serialize)]
struct KldataResp {
    message: String,
    message_id: usize,
    res: u8, // TODO: Should be an enum?
    retry: bool,
}

async fn handle_kldata(State(pool): State<PgConnectionPool>, body: String) -> Json<KldataResp> {
    let mut conn = pool.get().await.unwrap();

    let (message_id, obsinn_chunk) = parse_kldata(&body).unwrap();

    let data = label_kldata(obsinn_chunk, &mut conn).await.unwrap();

    insert_data(data, &mut conn).await.unwrap();

    Json(KldataResp {
        // TODO: fill in meaningful values here
        message: "".into(),
        message_id,
        res: 0,
        retry: false,
    })
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
    let manager = PostgresConnectionManager::new_from_stringlike(connect_string, NoTls).unwrap();
    let pool = bb8::Pool::builder().build(manager).await.unwrap();

    // build our application with a single route
    let app = Router::new()
        .route("/kldata", post(handle_kldata))
        .with_state(pool);

    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3001").await.unwrap();
    axum::serve(listener, app).await?;

    Ok(())
}
