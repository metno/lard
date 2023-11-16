use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use bb8_postgres::PostgresConnectionManager;
use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use tokio_postgres::NoTls;

type PgConnectionPool = bb8::Pool<PostgresConnectionManager<NoTls>>;

/// Utility function for mapping any error into a `500 Internal Server Error`
/// response.
fn internal_error<E: std::error::Error>(err: E) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}

#[derive(Debug, Serialize)]
struct TimeseriesResp {
    data: Vec<f32>,
    timestamps: Vec<DateTime<Utc>>,
}

#[derive(Debug, Deserialize)]
struct TimeseriesParams {
    start_time: Option<DateTime<Utc>>,
    end_time: Option<DateTime<Utc>>,
}

async fn stations_handler(
    State(pool): State<PgConnectionPool>,
    Path((station_id, element_id)): Path<(i32, String)>,
    Query(params): Query<TimeseriesParams>,
) -> Result<Json<TimeseriesResp>, (StatusCode, String)> {
    let conn = pool.get().await.map_err(internal_error)?;

    let start_time = params
        .start_time
        .unwrap_or_else(|| Utc.with_ymd_and_hms(1950, 1, 1, 0, 0, 0).unwrap());
    let end_time = params
        .end_time
        .unwrap_or_else(|| Utc.with_ymd_and_hms(9999, 1, 1, 0, 0, 0).unwrap());

    let results = conn
        .query(
            "SELECT obsvalue, obstime FROM data \
                WHERE timeseries = ( \
                    SELECT timeseries FROM labels.filter \
                        WHERE station_id = $1 AND element_id = $2 \
                        LIMIT 1 \
                ) \
                    AND obstime BETWEEN $3 AND $4",
            &[&station_id, &element_id, &start_time, &end_time],
        )
        .await
        .map_err(internal_error)?;

    let resp = {
        let mut data = Vec::with_capacity(results.len());
        let mut timestamps = Vec::with_capacity(results.len());

        for row in results {
            data.push(row.get(0));
            timestamps.push(row.get(1));
        }

        TimeseriesResp { data, timestamps }
    };

    Ok(Json(resp))
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
        .route(
            "/stations/:station_id/elements/:element_id",
            get(stations_handler),
        )
        .with_state(pool);

    // run it with hyper on localhost:3000
    axum::Server::bind(&"0.0.0.0:3000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}
