use axum::{
    extract::{FromRef, Path, Query, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use bb8_postgres::PostgresConnectionManager;
use chrono::{DateTime, Duration, Utc};
use drops::{get_product, get_product_availability};
use latest::{get_latest, LatestElem};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use timeseries::{
    get_timeseries_data_irregular, get_timeseries_data_regular, get_timeseries_info, Timeseries,
};
use timeslice::{get_timeslice, Timeslice};
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;
use tracing::info;

pub mod latest;
pub mod timeseries;
pub mod timeslice;

pub type PgConnectionPool = bb8::Pool<PostgresConnectionManager<NoTls>>;

#[derive(Clone)]
struct APIState {
    pool: PgConnectionPool,
    pop_reg: HashMap<String, Arc<dyn drops::operator::Operator + Send + Sync>>,
}

impl FromRef<APIState> for PgConnectionPool {
    fn from_ref(state: &APIState) -> PgConnectionPool {
        state.pool.clone() // the pool is internally reference counted, so no Arc needed
    }
}

impl FromRef<APIState> for HashMap<String, Arc<dyn drops::operator::Operator + Send + Sync>> {
    fn from_ref(
        state: &APIState,
    ) -> HashMap<String, Arc<dyn drops::operator::Operator + Send + Sync>> {
        state.pop_reg.clone()
    }
}

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

#[derive(Debug, Deserialize)]
struct ProductParameters {
    product_type: String,
    input_schema_instance: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProductResponse {
    // TODO

    // for now:
    pub data: Vec<String>,
}

// Handles a request to the /product route.
async fn drops_product_handler(
    State(pool): State<PgConnectionPool>,
    State(pop_reg): State<HashMap<String, Arc<dyn drops::operator::Operator + Send + Sync>>>,
    Query(params): Query<ProductParameters>,
) -> Result<Json<ProductResponse>, (StatusCode, String)> {
    let product_type = params.product_type.as_str();
    let input_schema_instance = params.input_schema_instance.as_str();

    // ignore return value for now ... TODO
    _ = get_product(
        pool,
        pop_reg,
        String::from(product_type),
        String::from(input_schema_instance),
    );

    // for now (TODO)
    let data: Vec<_> = [
        format!("product_type: >{product_type}<"),
        format!("input_schema_instance: >{input_schema_instance}<"),
    ]
    .iter()
    .map(|s| s.to_string())
    .collect();

    Ok(Json(ProductResponse { data }))
}

#[derive(Debug, Deserialize)]
struct ProductAvailabilityParameters {
    product_type: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProductAvailabilityResponse {
    // TODO

    // for now:
    pub data: Vec<String>,
}

// Handles a request to the /product/availability route.
async fn drops_product_availability_handler(
    State(pool): State<PgConnectionPool>,
    State(pop_reg): State<HashMap<String, Arc<dyn drops::operator::Operator + Send + Sync>>>,
    Query(params): Query<ProductAvailabilityParameters>,
) -> Result<Json<ProductAvailabilityResponse>, (StatusCode, String)> {
    let product_type = params.product_type.as_str();

    // ignore return value for now ... TODO
    _ = get_product_availability(pool, pop_reg, String::from(product_type));

    // for now (TODO)
    Ok(Json(ProductAvailabilityResponse {
        data: [format!("product_type: >{product_type}<")]
            .iter()
            .map(|s| s.to_string())
            .collect(),
    }))
}

// Sets up and runs the API server.
pub async fn run(
    pool: PgConnectionPool,
    pop_reg: HashMap<String, Arc<dyn drops::operator::Operator + Send + Sync>>,
    cancel_token: CancellationToken,
) -> Result<(), std::io::Error> {
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
        .route("/product", get(drops_product_handler))
        .route(
            "/product/availability",
            get(drops_product_availability_handler),
        )
        .with_state(APIState { pool, pop_reg });

    // run it with hyper on localhost:3000
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    info!("API started");
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { cancel_token.cancelled().await })
        .await
}
