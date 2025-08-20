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
use tower_http::compression::CompressionLayer;

use util::DbPools;

use patchwork::{get_patchwork, PatchworkData, PatchworkLabel, PatchworkTimeseriesTables};

pub mod error;
pub mod latest;
pub mod patchwork;
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
    // patchwork table(s) - open and restricted
    patchwork_tables: PatchworkTimeseriesTables,
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

impl FromRef<EgressState> for PatchworkTimeseriesTables {
    fn from_ref(state: &EgressState) -> PatchworkTimeseriesTables {
        state.patchwork_tables.clone()
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
struct PatchworkParams {
    from: DateTime<Utc>,
    to: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PatchworkResp {
    pub data: Vec<PatchworkData>,
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

async fn patchwork_handler(
    State(pools): State<DbPools>,
    State(patchwork_tables): State<PatchworkTimeseriesTables>,
    Path((station_id, param_id, sensor, level)): Path<(i32, i32, i32, i32)>,
    Query(params): Query<PatchworkParams>,
) -> Result<Json<PatchworkResp>, (StatusCode, String)> {
    let label = PatchworkLabel::new(station_id, param_id, Some(level), Some(sensor));
    // this is set to false for now
    let authorized = false;
    let open_conn = pools.open.get().await.map_err(error::internal_error)?;
    let restricted_conn = pools
        .restricted
        .get()
        .await
        .map_err(error::internal_error)?;

    let patchwork = if authorized {
        // TODO: need to implement filtering based on allowed permits
        get_patchwork(
            &restricted_conn,
            params.from,
            params.to,
            label,
            patchwork_tables.open,
            Some(patchwork_tables.restricted),
        )
        .await
        .map_err(error::internal_error)? //.filter(by_permit)
    } else {
        get_patchwork(
            &open_conn,
            params.from,
            params.to,
            label,
            patchwork_tables.open,
            None,
        )
        .await
        .map_err(error::internal_error)?
    };

    if let Some(p) = patchwork {
        Ok(Json(PatchworkResp { data: p }))
    } else {
        let not_found = (
            StatusCode::NOT_FOUND,
            String::from("no patchwork data for this combination of parameters"),
        );
        Err(not_found)
    }
}

pub async fn run(
    db_pools: DbPools,
    s3_bucket: S3Bucket,
    patchwork_tables: PatchworkTimeseriesTables,
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
            "/patchwork/{station_id}/param/{param_id}/level/{level}/sensor/{sensor}",
            get(patchwork_handler),
        )
        .route("/latest", get(latest_handler))
        .nest("/reports", reports_router())
        .with_state(EgressState {
            db_pools,
            s3_bucket,
            patchwork_tables,
        })
        .layer(CompressionLayer::new());

    // run it with hyper on localhost:3000
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { cancel_token.cancelled().await })
        .await
        .unwrap();
}
