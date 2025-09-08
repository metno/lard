use std::sync::Arc;

use axum::{
    extract::{Extension, FromRef, Json, Path, Query, State},
    http::StatusCode,
    middleware,
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

use util::DbPools;

use patchwork::{get_patchwork, PatchworkDatum, PatchworkLabel, PatchworkTimeseriesTables};

use auth::{auth_middleware, JWKScerts};

pub mod auth;
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
    stationids: String,
    paramids: String,
    levels: String,
    sensors: String,
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
    Query(params): Query<PatchworkParams>,
    Extension(roles): Extension<Option<Vec<i32>>>,
) -> Result<Json<Vec<PatchworkResp>>, (StatusCode, String)> {
    // parse the strings from the query
    // tried getting them to serialize as vec,
    // but does not work for a list as well as being able to send one object
    let stn_sep: Vec<&str> = params.stationids.split(",").collect(); // seperator used inside the string
    let par_sep: Vec<&str> = params.paramids.split(",").collect(); // seperator used inside the string
    let lev_sep: Vec<&str> = params.levels.split(",").collect(); // seperator used inside the string
    let sen_sep: Vec<&str> = params.sensors.split(",").collect(); // seperator used inside the string

    let mut labels: Vec<PatchworkLabel> = Vec::new();
    // create a list of labels from the query parameters
    // (since they can send in one or more we need to loop)
    for stn in stn_sep.iter() {
        let station_id = stn.parse::<i32>().map_err(error::bad_request)?;
        for par in par_sep.iter() {
            let param_id = par.parse::<i32>().map_err(error::bad_request)?;
            for lev in lev_sep.iter() {
                let level = lev.parse::<i32>().map_err(error::bad_request)?;
                for sen in sen_sep.iter() {
                    let sensor = sen.parse::<i32>().map_err(error::bad_request)?;
                    let label =
                        PatchworkLabel::new(station_id, param_id, Some(level), Some(sensor));
                    labels.push(label);
                }
            }
        }
    }
    //println!("Labels constructed: {labels:?}");

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
            let patchwork = get_patchwork(
                &restricted_conn,
                params.from,
                params.to,
                label,
                patchwork_tables.restricted.clone(),
                roles.clone(),
            )
            .await
            .map_err(error::internal_error)?;
            if let Some(data) = patchwork {
                // add to the outer list
                patchwork_response.push(PatchworkResp { label, data });
                continue; // found here so don't need to check the open
            }
        }
        let patchwork = get_patchwork(
            &open_conn,
            params.from,
            params.to,
            label,
            patchwork_tables.open.clone(),
            roles.clone(),
        )
        .await
        .map_err(error::internal_error)?;
        if let Some(data) = patchwork {
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
    State(patchwork_tables): State<PatchworkTimeseriesTables>,
) -> Result<Json<PatchworkAvailableResp>, (StatusCode, String)> {
    let mut available_list: Vec<PatchworkAvailable> = Vec::new();
    let ot = patchwork_tables
        .open
        .read()
        .map_err(error::internal_error)?;
    for (label, vec_fill) in ot.iter() {
        // find first and last times
        let first_time = vec_fill.iter().map(|item| item.from).min().unwrap();
        let last_time = if vec_fill.iter().any(|item| item.to.is_none()) {
            // if there is a None to time, that means the series is open ended,
            // which is the latest possible to time. but Option's Ord impl
            // counts None as less than Some. So we have this if check to
            // override that behaviour
            None
        } else {
            vec_fill.iter().map(|item| item.to).max().unwrap()
        };
        // The restrictions are all the same for a given label, so just take the first one
        let permit = vec_fill[0].permit;
        // add to list
        available_list.push(PatchworkAvailable {
            label: *label,
            from: first_time,
            to: last_time,
            permit,
        });
    }
    // TODO: handle the restricted table bit maybe need to add which permit-ids the labels have?

    Ok(Json(PatchworkAvailableResp {
        available: available_list,
    }))
}

pub async fn run(
    db_pools: DbPools,
    s3_bucket: S3Bucket,
    patchwork_tables: PatchworkTimeseriesTables,
    auth_certs: JWKScerts,
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
            "/patchwork", // all parameters sent as query not in url
            get(patchwork_handler),
        )
        .route("/patchwork/available", get(patchwork_available_handler))
        .route("/latest", get(latest_handler))
        .nest("/reports", reports::set_routes())
        .with_state(EgressState {
            db_pools,
            s3_bucket,
            patchwork_tables,
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
