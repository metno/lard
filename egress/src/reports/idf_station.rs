use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use util::PooledPgConn;

use crate::{errors, PgConnectionPool};

/// Unit of the intensity values in the response
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum IdfUnit {
    /// Millimeters
    #[default]
    Mm,
    /// Litres per second per hectare
    Lsha,
}

/// Precipitation intensity values fitted from a GEV distribution on annual precipitation timeseries.
/// More information can be found [here](https://doi.org/10.1016/j.jhydrol.2021.127000).
/// The code responsible for generating these values can be found [here](https://github.com/ClimDesign/fixIDF).
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IdfValue {
    /// Duration of the precipitation event in minutes
    pub duration: i32,
    /// Expected time [years] between events of computed intensity
    pub frequency: i32,
    /// Computed rainfall intensity value in millimeters [mm]
    pub intensity: f64,
    /// 0.025 quantile
    pub lower_interval: f64,
    /// 0.975 quantile
    pub upper_interval: f64,
}

/// Metadata and parameters used for fitting IDF values
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IdfMetadata {
    /// Unique ID for an IDF timeseries
    #[serde(skip)]
    tsid: i32,
    /// Number of three month periods considered in the calculation
    number_of_seasons: i32,
    // TODO: should we have these instead?
    // fromtime: Option<DateTime<Utc>>,
    // totime: Option<DateTime<Utc>>,
    /// First year considered in the precipitation timeseries
    first_year_of_period: i32,
    /// Last year considered in the precipitation timeseries
    last_year_of_period: i32,
    /// Quality of the timeseries used for the calculation
    // TODO: weighs length, resolution, and? Is there a proper definition?
    quality_class: i32,
    /// RNG seed used in the calculation
    seed_parameter: i32,
    /// When the calculation was carried out
    updated_at: chrono::DateTime<Utc>,
}

/// Query parameters struct for the station/:station_id endpoint
#[derive(Serialize, Deserialize)]
pub struct IdfStationParams {
    #[serde(default)]
    unit: IdfUnit,
}

/// Response struct returned by the station/:station_id endpoint
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IdfStationResp {
    pub station_id: i32,
    // TODO: is this correct???
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub values: Vec<IdfValue>,
    pub unit: IdfUnit,
    #[serde(flatten)]
    pub metadata: IdfMetadata,
}

/// Subset of [IdfMetadata] included in the availability endpoint response
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IdfStationInfo {
    pub station_id: i32,
    pub first_year_of_period: i32,
    pub last_year_of_period: i32,
    pub number_of_seasons: i32,
    pub quality_class: i32,
}

/// Response struct returned by the availability endpoint
#[derive(Debug, Serialize, Deserialize)]
pub struct IdfStationAvailability {
    pub stations: Vec<IdfStationInfo>,
}

/// Converts value [mm] and duration [minutes] to intensiry in [liter per second per hectare]
fn mm_to_lsha(val: f64, duration: i32) -> f64 {
    1e4 / 60.0 * val / duration as f64
}

async fn get_idf_station_metadata(
    conn: &PooledPgConn<'_>,
    station_id: i32,
) -> Result<IdfMetadata, (StatusCode, String)> {
    // TODO: this could be stored in a separate file
    // or in the main file header
    todo!();

    // Ok(IdfMetadata {
    //     tsid: row.get(0),
    //     first_year_of_period: row.get(1),
    //     last_year_of_period: row.get(2),
    //     number_of_seasons: row.get(3),
    //     quality_class: row.get(4),
    //     updated_at: row.get(5),
    //     seed_parameter: row.get(6),
    // })
}

async fn get_idf_station_values(
    conn: &PooledPgConn<'_>,
    tsid: i32,
    unit: IdfUnit,
) -> Result<Vec<IdfValue>, (StatusCode, String)> {
    // TODO: 2 options
    //  - connect to S3 and get single station object
    //  - load file from distributed file system
    todo!();

    // TODO: collect to values
    // let values = match unit {
    //     IdfUnit::Mm => rows
    //         .iter()
    //         .map(|row| IdfValue {
    //             duration: row.get(0),
    //             frequency: row.get(1),
    //             intensity: row.get(2),
    //             lower_interval: row.get(3),
    //             upper_interval: row.get(4),
    //         })
    //         .collect(),
    //
    //     IdfUnit::Lsha => rows
    //         .iter()
    //         .map(|row| {
    //             let duration = row.get(0);
    //
    //             IdfValue {
    //                 duration,
    //                 frequency: row.get(1),
    //                 intensity: mm_to_lsha(row.get(2), duration),
    //                 lower_interval: mm_to_lsha(row.get(3), duration),
    //                 upper_interval: mm_to_lsha(row.get(4), duration),
    //             }
    //         })
    //         .collect(),
    // };
    //
    // Ok(values)
}

pub async fn idf_station_availability_handler(
    State(pool): State<PgConnectionPool>,
) -> Result<Json<IdfStationAvailability>, (StatusCode, String)> {
    // TODO: 2 options
    //  - connect to S3 and list objects for the idf station bucket
    //  - ls from distributed file system
    //  I would like to have it the same way frost does it for gridded data
    todo!();

    // TODO: collect to IdfStationInfo
    // let stations = ...
    // .map(|row| IdfStationInfo {
    //     station_id: row.get(0),
    //     first_year_of_period: row.get(1),
    //     last_year_of_period: row.get(2),
    //     number_of_seasons: row.get(3),
    //     quality_class: row.get(4),
    // })
    // .collect();

    // Ok(Json(IdfStationAvailability { stations }))
}

pub async fn idf_station_handler(
    Path(station_id): Path<i32>,
    State(pool): State<PgConnectionPool>,
    Query(params): Query<IdfStationParams>,
) -> Result<Json<IdfStationResp>, (StatusCode, String)> {
    // TODO: authentication?
    let conn = pool.get().await.map_err(errors::internal_error)?;

    let metadata = get_idf_station_metadata(&conn, station_id).await?;
    let values = get_idf_station_values(&conn, metadata.tsid, params.unit).await?;

    Ok(Json(IdfStationResp {
        station_id,
        metadata,
        unit: params.unit,
        values,
    }))
}

pub fn idf_station_router() -> Router<PgConnectionPool> {
    Router::new()
        .route("/station", get(idf_station_availability_handler))
        .route("/station/{station_id}", get(idf_station_handler))
}
