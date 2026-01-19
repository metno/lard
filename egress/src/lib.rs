use std::sync::Arc;

use axum::{
    extract::{Extension, FromRef, Json, MatchedPath, Path, Query, Request, State},
    http::StatusCode,
    middleware::{self, Next},
    response::IntoResponse,
    routing::get,
    Router,
};
use chrono::{DateTime, Duration, Utc};
use latest::{get_latest, LatestElem};
use serde::{Deserialize, Serialize};
use timeseries::{
    get_timeseries_data_irregular, get_timeseries_data_regular, get_timeseries_info, Timeseries,
};
use timeslice::{get_timeslice, Timeslice};
use tokio_util::sync::CancellationToken;
use tower_http::compression::CompressionLayer;

use util::deserialize::comma_separated;
use util::DbPools;

pub mod auth;
pub mod calculations;
pub mod cron;
pub mod error;
pub mod latest;
pub mod patchwork;
pub mod products;
pub mod reports;
pub mod timeseries;
pub mod timeslice;

use auth::{auth_middleware, JWKScerts};
use patchwork::{get_patchwork, PatchworkDatum, PatchworkLabel, PatchworkTables};
use products::{products_router, ProductTables};
use reports::reports_router;

use crate::error::Error;

pub const PATCHWORK_HTTP_REQUESTS_DURATION_SECONDS: &str =
    "patchwork_http_requests_duration_seconds";
pub const PATCHWORK_REQUESTS_RECEIVED: &str = "patchwork_requests_received";
pub const PATCHWORK_AVAILABLE_REQUESTS_RECEIVED: &str = "patchwork_available_requests_received";

// TODO: move to utils?
type S3Bucket = Arc<s3::Bucket>;

#[derive(Clone, Debug)]
pub struct EgressState {
    db_pools: DbPools,
    // pub s3_client: S3Client,
    s3_bucket: S3Bucket,
    // patchwork table(s) - open and restricted
    patchwork_tables: PatchworkTables,
    product_tables: ProductTables,
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

impl FromRef<EgressState> for PatchworkTables {
    fn from_ref(state: &EgressState) -> PatchworkTables {
        state.patchwork_tables.clone()
    }
}

impl FromRef<EgressState> for ProductTables {
    fn from_ref(state: &EgressState) -> ProductTables {
        state.product_tables.clone()
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
    #[serde(deserialize_with = "comma_separated")]
    stationids: Vec<i32>,
    #[serde(deserialize_with = "comma_separated")]
    paramids: Vec<i32>,
    #[serde(deserialize_with = "comma_separated")]
    levels: Vec<i32>,
    #[serde(deserialize_with = "comma_separated")]
    sensors: Vec<i32>,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PatchworkResp {
    pub label: PatchworkLabel,
    pub data: Vec<PatchworkDatum>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PatchworkAvailable {
    label: PatchworkLabel,
    // TODO: timeseries can have known holes, so could have an array of from/to
    // or alternatively simply repeat the label with another from/to?
    from: DateTime<Utc>,
    to: Option<DateTime<Utc>>,
    permit: i32, // frost needs to know this for use to show the restricted ones to the right users
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PatchworkAvailableResp {
    pub available: Vec<PatchworkAvailable>,
}

/// Gets an environment variable, providing more details than calling std::env::var() directly.
pub fn getenv(key: &str) -> Result<String, Error> {
    std::env::var(key).map_err(|e| Error::Env(format!("{e}: {key}")))
}

// Handler for basic liveness endpoint
// for use for load balancing
async fn liveness_handler() -> Result<String, (StatusCode, String)> {
    Ok("Liveness check successful".to_string())
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
    State(patchwork_tables): State<PatchworkTables>,
    Query(params): Query<PatchworkParams>,
    Extension(roles): Extension<Option<Vec<i32>>>,
) -> Result<Json<Vec<PatchworkResp>>, (StatusCode, String)> {
    metrics::counter!(PATCHWORK_REQUESTS_RECEIVED).increment(1);
    let mut labels: Vec<PatchworkLabel> = Vec::new();

    // create a list of labels from the query parameters
    // (since they can send in one or more we need to loop)
    for station_id in params.stationids {
        for param_id in &params.paramids {
            for level in &params.levels {
                for sensor in &params.sensors {
                    let label =
                        PatchworkLabel::new(station_id, *param_id, Some(*level), Some(*sensor));
                    labels.push(label);
                }
            }
        }
    }

    let open_conn = pools.open.get().await.map_err(error::internal_error)?;
    let restricted_conn = pools
        .restricted
        .get()
        .await
        .map_err(error::internal_error)?;

    let mut patchwork_response: Vec<PatchworkResp> = Vec::new();
    for label in labels {
        if roles.is_some() {
            // TODO: need to implement filtering based on allowed permits
            let data = get_patchwork(
                &restricted_conn,
                params.from,
                params.to,
                label,
                patchwork_tables.restricted.clone(),
                roles.clone(),
            )
            .await
            .map_err(error::internal_error)?;

            if !data.is_empty() {
                // add to the outer list
                patchwork_response.push(PatchworkResp { label, data });

                // found here so don't need to check the open
                continue;
            }
        }

        let data = get_patchwork(
            &open_conn,
            params.from,
            params.to,
            label,
            patchwork_tables.open.clone(),
            roles.clone(),
        )
        .await
        .map_err(error::internal_error)?;

        if !data.is_empty() {
            // add to the outer list
            patchwork_response.push(PatchworkResp { label, data });
        }
    }

    if patchwork_response.is_empty() {
        let not_found = (
            StatusCode::NOT_FOUND,
            String::from("no patchwork data for this combination of parameters"),
        );
        Err(not_found)
    } else {
        Ok(Json(patchwork_response))
    }
}

pub async fn patchwork_available_handler(
    State(tables): State<PatchworkTables>,
    Extension(opt_roles): Extension<Option<Vec<i32>>>,
) -> Result<Json<PatchworkAvailableResp>, (StatusCode, String)> {
    metrics::counter!(PATCHWORK_AVAILABLE_REQUESTS_RECEIVED).increment(1);
    let mut available_list: Vec<PatchworkAvailable> = Vec::new();

    let ot = tables.open.read().map_err(error::internal_error)?;

    for (label, fills) in ot.iter() {
        // fills are already sorted
        let first_time = fills[0].from;
        let last_time = fills.iter().last().map(|fill| fill.to).unwrap();

        // The restrictions are all the same for a given label, so just take the first one
        let permit = fills[0].permit;

        available_list.push(PatchworkAvailable {
            label: *label,
            from: first_time,
            to: last_time,
            permit,
        });
    }

    if let Some(roles) = opt_roles {
        let rt = tables.restricted.read().map_err(error::internal_error)?;

        for (label, fills) in rt.iter() {
            // Skip if request has wrong permits
            // NOTE: All fills have the same permit id (since restrictions are applied to whole
            // stations or single params)
            if !roles.contains(&fills[0].permit) {
                continue;
            }

            // fills are already sorted
            let first_time = fills[0].from;
            let last_time = fills.iter().last().map(|fill| fill.to).unwrap();

            // The restrictions are all the same for a given label, so just take the first one
            let permit = fills[0].permit;

            available_list.push(PatchworkAvailable {
                label: *label,
                from: first_time,
                to: last_time,
                permit,
            });
        }
    }

    Ok(Json(PatchworkAvailableResp {
        available: available_list,
    }))
}

/// Middleware function that runs around a request, so we can record how long it took
async fn track_patchwork_request_duration(req: Request, next: Next) -> impl IntoResponse {
    let start = std::time::Instant::now();
    let path = if let Some(matched_path) = req.extensions().get::<MatchedPath>() {
        matched_path.as_str().to_owned()
    } else {
        req.uri().path().to_owned()
    };
    let method = req.method().to_string();

    let response = next.run(req).await;

    let duration = start.elapsed().as_secs_f64();
    let status = response.status().as_u16().to_string();

    let labels = [("method", method), ("path", path), ("status", status)];

    metrics::histogram!("patchwork_http_requests_duration_seconds", &labels).record(duration);

    response
}

pub async fn run(
    db_pools: DbPools,
    s3_bucket: S3Bucket,
    patchwork_tables: PatchworkTables,
    product_tables: ProductTables,
    auth_certs: JWKScerts,
    cancel_token: CancellationToken,
) {
    // build our application with routes
    // TODO: add authentication middleware that returns the correct db pool?
    let app = Router::new()
        .route(
            "/patchwork", // all parameters sent as query not in url
            get(patchwork_handler),
        )
        .route_layer(middleware::from_fn(track_patchwork_request_duration))
        .route("/patchwork/available", get(patchwork_available_handler))
        .route(
            "/stations/{station_id}/params/{param_id}",
            get(stations_handler),
        )
        .route(
            "/timeslices/{timestamp}/params/{param_id}",
            get(timeslice_handler),
        )
        .route("/latest", get(latest_handler))
        .route("/liveness", get(liveness_handler))
        .nest("/reports", reports_router())
        .nest("/products", products_router())
        .with_state(EgressState {
            db_pools,
            s3_bucket,
            patchwork_tables,
            product_tables,
        })
        .route_layer(middleware::from_fn_with_state(
            auth_certs.clone(),
            auth_middleware,
        ))
        .layer(CompressionLayer::new());

    // run it with hyper on localhost:3000
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { cancel_token.cancelled().await })
        .await
        .unwrap();
}
