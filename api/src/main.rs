use axum::{extract::State, http::StatusCode, routing::get, Router};
use bb8_postgres::PostgresConnectionManager;
use tokio_postgres::NoTls;

type PgConnectionPool = bb8::Pool<PostgresConnectionManager<NoTls>>;

/// Utility function for mapping any error into a `500 Internal Server Error`
/// response.
fn internal_error<E: std::error::Error>(err: E) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}

async fn timeseries_handler(
    State(pool): State<PgConnectionPool>,
) -> Result<String, (StatusCode, String)> {
    let conn = pool.get().await.map_err(internal_error)?;

    let results = conn
        .query(
            "SELECT obsvalue FROM public.data WHERE data.timeseries = 2",
            &[],
        )
        .await
        .map_err(internal_error)?;

    println!("{:?}", results);

    Ok("".to_string())
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 4 {
        panic!("not enough args passed in, at least host, user, dbname needed, optionally password")
    }

    let mut connect_string = format!("host={} user={} dbname={}", &args[1], &args[2], &args[3]);
    if args.len() > 4 {
        connect_string.push(' ');
        connect_string.push_str(&args[4])
    }

    // set up postgres connection pool
    let manager = PostgresConnectionManager::new_from_stringlike(connect_string, NoTls).unwrap();
    let pool = bb8::Pool::builder().build(manager).await.unwrap();

    // build our application with a single route
    let app = Router::new()
        .route("/timeseries", get(timeseries_handler))
        .with_state(pool);

    // run it with hyper on localhost:3000
    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}
