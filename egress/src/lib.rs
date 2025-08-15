use std::sync::Arc;

use axum::{
    extract::{FromRef, Path, Query, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use chrono::{DateTime, Duration, Utc};
use latest::{get_latest, LatestElem};
use reports::reports_router;
use serde::{Deserialize, Serialize};
use timeseries::{
    get_timeseries_data_irregular, get_timeseries_data_regular, get_timeseries_info, Timeseries,
};
use timeslice::{get_timeslice, Timeslice};
use tokio_util::sync::CancellationToken;

use util::DbPools;

use crate::filter::{get_filter, FilterData, FilterLabel, FilterTimeseriesTables};

pub mod error;
pub mod filter;
pub mod latest;
pub mod reports;

pub mod timeseries;
pub mod timeslice;

// TODO: move to utils?
type S3Bucket = Arc<s3::Bucket>;

#[derive(Clone, Debug)]
pub struct EgressState {
    db_pools: DbPools,
    // pub s3_client: S3Client,
    s3_bucket: S3Bucket,
    // filter table
    filter_table: FilterTimeseriesTables,
}

impl FromRef<EgressState> for DbPools {
    fn from_ref(state: &EgressState) -> Self {
        state.db_pools.clone() // the pool is internally reference counted, so no Arc needed
    }
}

impl FromRef<EgressState> for S3Bucket {
    fn from_ref(state: &EgressState) -> Self {
        state.s3_bucket.clone()
    }
}

impl FromRef<EgressState> for FilterTimeseriesTables {
    fn from_ref(state: &EgressState) -> FilterTimeseriesTables {
        state.filter_table.clone()
    }
}

#[derive(Debug, Deserialize)]
struct TimeseriesParams {
    start_time: Option<DateTime<Utc>>,
    end_time: Option<DateTime<Utc>>,
    time_resolution: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TimeseriesResp {
    pub tseries: Vec<Timeseries>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TimesliceResp {
    pub tslices: Vec<Timeslice>,
}

#[derive(Debug, Deserialize)]
struct LatestParams {
    latest_max_age: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LatestResp {
    pub data: Vec<LatestElem>,
}

#[derive(Debug, Deserialize)]
struct FilterParams {
    from: DateTime<Utc>,
    to: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FilterResp {
    pub data: Vec<FilterData>,
}

async fn stations_handler(
    State(pools): State<DbPools>,
    // TODO: this should probably take element_id instead of param_id and do a conversion
    Path((station_id, param_id)): Path<(i32, i32)>,
    Query(params): Query<TimeseriesParams>,
) -> Result<Json<TimeseriesResp>, (StatusCode, String)> {
    let conn = pools.open.get().await.map_err(error::internal_error)?;

    let header = get_timeseries_info(&conn, station_id, param_id)
        .await
        .map_err(error::internal_error)?;

    let start_time = params.start_time.unwrap_or(header.fromtime);
    let end_time = params.end_time.unwrap_or(header.totime);

    let ts = if let Some(time_resolution) = params.time_resolution {
        Timeseries::Regular(
            get_timeseries_data_regular(&conn, header, start_time, end_time, time_resolution)
                .await
                .map_err(error::internal_error)?,
        )
    } else {
        Timeseries::Irregular(
            get_timeseries_data_irregular(&conn, header, start_time, end_time)
                .await
                .map_err(error::internal_error)?,
        )
    };

    Ok(Json(TimeseriesResp { tseries: vec![ts] }))
}

async fn timeslice_handler(
    State(pools): State<DbPools>,
    // TODO: this should probably take element_id instead of param_id and do a conversion
    Path((timestamp, param_id)): Path<(DateTime<Utc>, i32)>,
) -> Result<Json<TimesliceResp>, (StatusCode, String)> {
    let conn = pools.open.get().await.map_err(error::internal_error)?;

    let slice = get_timeslice(&conn, timestamp, param_id)
        .await
        .map_err(error::internal_error)?;

    Ok(Json(TimesliceResp {
        tslices: vec![slice],
    }))
}

async fn latest_handler(
    State(pools): State<DbPools>,
    Query(params): Query<LatestParams>,
) -> Result<Json<LatestResp>, (StatusCode, String)> {
    let conn = pools.open.get().await.map_err(error::internal_error)?;

    let latest_max_age = params
        .latest_max_age
        .unwrap_or_else(|| Utc::now() - Duration::hours(3));

    let data = get_latest(&conn, latest_max_age)
        .await
        .map_err(error::internal_error)?;

    Ok(Json(LatestResp { data }))
}

async fn filter_handler(
    State(pools): State<DbPools>,
    State(filter_tables): State<FilterTimeseriesTables>,
    Path((station_id, param_id, sensor, level)): Path<(i32, i32, i32, i32)>,
    Query(params): Query<FilterParams>,
) -> Result<Json<FilterResp>, (StatusCode, String)> {
    let open_conn: bb8::PooledConnection<
        '_,
        bb8_postgres::PostgresConnectionManager<tokio_postgres::NoTls>,
    > = pools.open.get().await.map_err(error::internal_error)?;
    let restricted_conn: bb8::PooledConnection<
        '_,
        bb8_postgres::PostgresConnectionManager<tokio_postgres::NoTls>,
    > = pools
        .restricted
        .get()
        .await
        .map_err(error::internal_error)?;

    let label = FilterLabel::new(station_id, param_id, Some(level), Some(sensor));

    let filter = get_filter(
        &open_conn,
        &restricted_conn,
        params.from,
        params.to,
        filter_tables,
        label,
    )
    .await
    .map_err(error::internal_error)?;

    eprintln!("filter {filter:?}");

    if let Some(f) = filter {
        Ok(Json(FilterResp { data: f }))
    } else {
        let not_found = (
            StatusCode::NOT_FOUND,
            String::from("no filter data for this combination of parameters"),
        );
        Err(not_found)
    }
}

pub async fn run(
    db_pools: DbPools,
    s3_bucket: S3Bucket,
    filter_table: FilterTimeseriesTables,
    cancel_token: CancellationToken,
) {
    // build our application with routes
    // TODO: add authentication middleware that returns the correct db pool?
    let app = Router::new()
        .route(
            "/stations/{station_id}/params/{param_id}",
            get(stations_handler),
        )
        .route(
            "/timeslices/{timestamp}/params/{param_id}",
            get(timeslice_handler),
        )
        .route(
            "/filter/{station_id}/param/{param_id}/level/{level}/sensor/{sensor}",
            get(filter_handler),
        )
        .route("/latest", get(latest_handler))
        .nest("/reports", reports_router())
        .with_state(EgressState {
            db_pools,
            s3_bucket,
            filter_table,
        });

    // run it with hyper on localhost:3000
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { cancel_token.cancelled().await })
        .await
        .unwrap();
}
