use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use bb8_postgres::PostgresConnectionManager;
use chrono::{DateTime, Duration, Utc};
use latest::{get_latest, LatestElem};
use serde::{Deserialize, Serialize};
use timeseries::{
    get_timeseries_data_irregular, get_timeseries_data_regular, get_timeseries_info, Timeseries,
};
use timeslice::{get_timeslice, Timeslice};
use tokio_postgres::NoTls;

mod latest;
mod timeseries;
mod timeslice;
pub(crate) mod util;

type PgConnectionPool = bb8::Pool<PostgresConnectionManager<NoTls>>;

/// Utility function for mapping any error into a `500 Internal Server Error`
/// response.
fn internal_error<E: std::error::Error>(err: E) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}

#[derive(Debug, Deserialize)]
struct TimeseriesParams {
    start_time: Option<DateTime<Utc>>,
    end_time: Option<DateTime<Utc>>,
    time_resolution: Option<String>,
}

#[derive(Debug, Serialize)]
struct TimeseriesResp {
    tseries: Vec<Timeseries>,
}

#[derive(Debug, Serialize)]
struct TimesliceResp {
    tslices: Vec<Timeslice>,
}

#[derive(Debug, Deserialize)]
struct LatestParams {
    latest_max_age: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
struct LatestResp {
    data: Vec<LatestElem>,
}

async fn stations_handler(
    State(pool): State<PgConnectionPool>,
    // TODO: this should probably take element_id instead of param_id and do a conversion
    Path((station_id, param_id)): Path<(i32, i32)>,
    Query(params): Query<TimeseriesParams>,
) -> Result<Json<TimeseriesResp>, (StatusCode, String)> {
    let conn = pool.get().await.map_err(internal_error)?;

    let header = get_timeseries_info(&conn, station_id, param_id)
        .await
        .map_err(internal_error)?;

    let start_time = params.start_time.unwrap_or(header.fromtime);
    let end_time = params.end_time.unwrap_or(header.totime);

    let ts = if let Some(time_resolution) = params.time_resolution {
        Timeseries::Regular(
            get_timeseries_data_regular(&conn, header, start_time, end_time, time_resolution)
                .await
                .map_err(internal_error)?,
        )
    } else {
        Timeseries::Irregular(
            get_timeseries_data_irregular(&conn, header, start_time, end_time)
                .await
                .map_err(internal_error)?,
        )
    };

    Ok(Json(TimeseriesResp { tseries: vec![ts] }))
}

async fn timeslice_handler(
    State(pool): State<PgConnectionPool>,
    // TODO: this should probably take element_id instead of param_id and do a conversion
    Path((timestamp, param_id)): Path<(DateTime<Utc>, i32)>,
) -> Result<Json<TimesliceResp>, (StatusCode, String)> {
    let conn = pool.get().await.map_err(internal_error)?;

    let slice = get_timeslice(&conn, timestamp, param_id)
        .await
        .map_err(internal_error)?;

    Ok(Json(TimesliceResp {
        tslices: vec![slice],
    }))
}

async fn latest_handler(
    State(pool): State<PgConnectionPool>,
    Query(params): Query<LatestParams>,
) -> Result<Json<LatestResp>, (StatusCode, String)> {
    let conn = pool.get().await.map_err(internal_error)?;

    let latest_max_age = params
        .latest_max_age
        .unwrap_or_else(|| Utc::now() - Duration::hours(3));

    let data = get_latest(&conn, latest_max_age)
        .await
        .map_err(internal_error)?;

    Ok(Json(LatestResp { data }))
}

#[tokio::main]
async fn main() {
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

    // build our application with routes
    let app = Router::new()
        .route(
            "/stations/:station_id/params/:param_id",
            get(stations_handler),
        )
        .route(
            "/timeslices/:timestamp/params/:param_id",
            get(timeslice_handler),
        )
        .route("/latest", get(latest_handler))
        .with_state(pool);

    // run it with hyper on localhost:3000
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
