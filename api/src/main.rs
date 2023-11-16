use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use bb8_postgres::PostgresConnectionManager;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio_postgres::{types::FromSql, NoTls};

type PgConnectionPool = bb8::Pool<PostgresConnectionManager<NoTls>>;

/// Utility function for mapping any error into a `500 Internal Server Error`
/// response.
fn internal_error<E: std::error::Error>(err: E) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}

#[derive(Debug, Serialize, FromSql)]
#[postgres(name = "location")]
struct Location {
    lat: Option<f32>,
    lon: Option<f32>,
    hamsl: Option<f32>,
    hag: Option<f32>,
}

// TODO: this should be more comprehensive once the schema supports it
// TODO: figure out what should be wrapped in Option here
#[derive(Debug, Serialize)]
struct TimeseriesHeader {
    station_id: i32,
    element_id: String,
    lvl: i32,
    sensor: i32,
    location: Location,
}

#[derive(Debug, Serialize)]
struct Timeseries {
    header: TimeseriesHeader,
    data: Vec<f32>,
    timestamps: Vec<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
struct TimeseriesResp {
    tseries: Vec<Timeseries>,
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

    let ts_result = conn
        .query_one(
            "SELECT timeseries.id, \
                COALESCE(timeseries.fromtime, '1950-01-01 00:00:00+00'), \
                COALESCE(timeseries.totime, '9999-01-01 00:00:00+00'), \
                filter.lvl, \
                filter.sensor, \
                timeseries.loc \
                FROM timeseries JOIN labels.filter \
                    ON timeseries.id = filter.timeseries \
                WHERE filter.station_id = $1 AND filter.element_id = $2 \
                LIMIT 1", // TODO: we should probably do something smarter than LIMIT 1
            &[&station_id, &element_id],
        )
        .await
        .map_err(internal_error)?;
    let ts_id: i32 = ts_result.get(0);
    let ts_fromtime: DateTime<Utc> = ts_result.get(1);
    let ts_totime: DateTime<Utc> = ts_result.get(2);
    let header = TimeseriesHeader {
        station_id,
        element_id,
        lvl: ts_result.get(3),
        sensor: ts_result.get(4),
        location: ts_result.get(5),
    };

    let start_time = params.start_time.unwrap_or(ts_fromtime);
    let end_time = params.end_time.unwrap_or(ts_totime);

    let data_results = conn
        .query(
            "SELECT obsvalue, obstime FROM data \
                WHERE timeseries = $1 \
                    AND obstime BETWEEN $2 AND $3",
            &[&ts_id, &start_time, &end_time],
        )
        .await
        .map_err(internal_error)?;

    let resp = {
        let mut data = Vec::with_capacity(data_results.len());
        let mut timestamps = Vec::with_capacity(data_results.len());

        for row in data_results {
            data.push(row.get(0));
            timestamps.push(row.get(1));
        }

        TimeseriesResp {
            tseries: vec![Timeseries {
                header,
                data,
                timestamps,
            }],
        }
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
