use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
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
/// More information can be found [here](https://www.met.no/publikasjoner/met-report/met-report-2022) under the link titled "IVF-verdier for norske nedbørstasjoner".
/// The code responsible for generating this values can be found [here](https://github.com/ClimDesign/fixIDF).
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IdfValue {
    /// Duration of the precipitation event in minutes
    pub duration: i32,
    /// Return period in years
    pub frequency: i32,
    /// Computed intensity value in millimeters (mm)
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
    #[serde(skip)]
    tsid: i32,
    number_of_seasons: i32,
    // TODO: should we have these instead?
    // fromtime: Option<DateTime<Utc>>,
    // totime: Option<DateTime<Utc>>,
    first_year_of_period: i32,
    last_year_of_period: i32,
    quality_class: i32,
    seed_parameter: i32,
    updated_at: chrono::DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IdfStationInfo {
    pub station_id: i32,
    pub first_year_of_period: i32,
    pub last_year_of_period: i32,
    pub number_of_seasons: i32,
    pub quality_class: i32,
}

#[derive(Serialize, Deserialize)]
pub struct IdfStationParams {
    #[serde(default)]
    unit: IdfUnit,
}

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
    // Only select the last updated timeseries
    let row = conn
        .query_one(
            "SELECT \
                id,
                EXTRACT(year FROM fromtime)::int AS first_year, \
                EXTRACT(year FROM totime)::int AS last_year, \
                number_of_seasons, \
                quality_class, \
                updated_at, \
                seed_parameter \
            FROM reports.idf_station_timeseries \
            WHERE station_id = $1 \
            ORDER BY updated_at DESC \
            LIMIT 1",
            &[&station_id],
        )
        .await
        // TODO: or not found?
        .map_err(errors::internal_error)?;

    Ok(IdfMetadata {
        tsid: row.get(0),
        first_year_of_period: row.get(1),
        last_year_of_period: row.get(2),
        number_of_seasons: row.get(3),
        quality_class: row.get(4),
        updated_at: row.get(5),
        seed_parameter: row.get(6),
    })
}

async fn get_idf_station_values(
    conn: &PooledPgConn<'_>,
    tsid: i32,
    unit: IdfUnit,
) -> Result<Vec<IdfValue>, (StatusCode, String)> {
    let rows = conn
        .query(
            "SELECT \
                duration, \
                frequency, \
                intensity, \
                lower_interval, \
                upper_interval \
            FROM reports.idf_station_data \
            WHERE timeseries = $1",
            &[&tsid],
        )
        .await
        // TODO: or not found?
        .map_err(errors::internal_error)?;

    let values = match unit {
        IdfUnit::Mm => rows
            .iter()
            .map(|row| IdfValue {
                duration: row.get(0),
                frequency: row.get(1),
                intensity: row.get(2),
                lower_interval: row.get(3),
                upper_interval: row.get(4),
            })
            .collect(),

        IdfUnit::Lsha => rows
            .iter()
            .map(|row| {
                let duration = row.get(0);

                IdfValue {
                    duration,
                    frequency: row.get(1),
                    intensity: mm_to_lsha(row.get(2), duration),
                    lower_interval: mm_to_lsha(row.get(3), duration),
                    upper_interval: mm_to_lsha(row.get(4), duration),
                }
            })
            .collect(),
    };

    Ok(values)
}

pub async fn idf_station_availability_handler(
    State(pool): State<PgConnectionPool>,
) -> Result<Json<IdfStationAvailability>, (StatusCode, String)> {
    let conn = pool.get().await.map_err(errors::internal_error)?;

    // Select only the last row for a given station_id
    // TODO: could be simplified (?) by 'WHERE updated_at = (select max(update_at) ...)'
    // but this assumes all updates happen simultaneously
    let rows = conn
        .query(
            "SELECT DISTINCT ON(station_id) \
                station_id,
                EXTRACT(year FROM fromtime)::int AS first_year, \
                EXTRACT(year FROM totime)::int AS last_year, \
                number_of_seasons, \
                quality_class \
            FROM reports.idf_station_timeseries \
            ORDER BY station_id, updated_at DESC",
            &[],
        )
        .await
        .map_err(errors::internal_error)?;

    let stations = rows
        .iter()
        .map(|row| IdfStationInfo {
            station_id: row.get(0),
            first_year_of_period: row.get(1),
            last_year_of_period: row.get(2),
            number_of_seasons: row.get(3),
            quality_class: row.get(4),
        })
        .collect();

    Ok(Json(IdfStationAvailability { stations }))
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
