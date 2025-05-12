use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};

use crate::{
    errors::{self, Error},
    EgressState, S3Bucket,
};

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
    /// MET station identifier
    station_id: i32,
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

/// Response struct returned by the availability endpoint
#[derive(Debug, Serialize, Deserialize)]
pub struct IdfStationAvailability {
    pub stations: Vec<IdfMetadata>,
}

/// Converts value [mm] and duration [minutes] to intensiry in [liter per second per hectare]
fn mm_to_lsha(val: f64, duration: i32) -> f64 {
    1e4 / 60.0 * val / duration as f64
}

pub async fn idf_station_availability_handler(
    State(s3_bucket): State<S3Bucket>,
) -> Result<Json<IdfStationAvailability>, (StatusCode, String)> {
    let metadata = s3_bucket
        // TODO: need separator?
        .get_object("/metadata.csv".to_string())
        .await
        .map_err(errors::internal_error)?;

    let bytes = metadata
        .as_str()
        .map_err(errors::internal_error)?
        .as_bytes();

    // NOTE: requires column order to be same as struct field order
    let stations: Vec<IdfMetadata> = csv::Reader::from_reader(bytes)
        .into_deserialize()
        .collect::<Result<Vec<IdfMetadata>, csv::Error>>()
        .map_err(errors::internal_error)?;

    Ok(Json(IdfStationAvailability { stations }))
}

// TODO: need blocking thread?
fn parse_csv(bytes: &[u8], unit: IdfUnit) -> Result<(IdfMetadata, Vec<IdfValue>), Error> {
    let mut reader = csv::Reader::from_reader(bytes);

    // TODO: duplicated metadata record in station csv header row, are there better options?
    let metadata: IdfMetadata = {
        let header = reader.headers()?;
        // NOTE: requires column order to be same as struct field order
        header.deserialize(None)?
    };

    let values: Vec<IdfValue> = match unit {
        IdfUnit::Mm => reader
            .into_deserialize()
            .map(|res| Ok(res?))
            .collect::<Result<Vec<IdfValue>, Error>>(),

        IdfUnit::Lsha => reader
            .into_records()
            .map(|res| {
                let record = res?;
                let duration = record[0].parse()?;

                Ok(IdfValue {
                    duration,
                    frequency: record[1].parse()?,
                    intensity: mm_to_lsha(record[2].parse()?, duration),
                    lower_interval: mm_to_lsha(record[3].parse()?, duration),
                    upper_interval: mm_to_lsha(record[4].parse()?, duration),
                })
            })
            .collect(),
    }?;

    Ok((metadata, values))
}

pub async fn idf_station_handler(
    Path(station_id): Path<i32>,
    State(s3_bucket): State<S3Bucket>,
    Query(params): Query<IdfStationParams>,
) -> Result<Json<IdfStationResp>, (StatusCode, String)> {
    let station_file = s3_bucket
        // TODO: possible vulnerability?
        .get_object(format!("/{}.csv", station_id))
        .await
        .map_err(errors::internal_error)?;

    let bytes = station_file
        .as_str()
        .map_err(errors::internal_error)?
        .as_bytes();

    let (metadata, values) = parse_csv(bytes, params.unit).map_err(errors::internal_error)?;

    Ok(Json(IdfStationResp {
        station_id,
        metadata,
        unit: params.unit,
        values,
    }))
}

pub fn idf_station_router() -> Router<EgressState> {
    Router::new()
        .route("/station", get(idf_station_availability_handler))
        .route("/station/{station_id}", get(idf_station_handler))
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_csv_parsing() {
        todo!();
    }
}
