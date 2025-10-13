use axum::{
    extract::{Path, State},
    Json,
};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use util::idf_parse::{IdfMetadata, IdfValue};

use crate::{
    error::{self, Error},
    reports::{
        idf_station::{parse_metadata_csv, parse_values_csv},
        IdfStationAvailability, IdfUnit,
    },
    S3Bucket,
};

const DUT_PATH: &str = "lard/dut/latest/";

#[derive(Debug, Serialize, Deserialize)]
pub enum DutUnit {
    #[serde(rename = "degC")]
    Celsius,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DutResponse {
    metadata: IdfMetadata,
    unit: DutUnit,
    summer: Vec<IdfValue>,
    winter: Vec<IdfValue>,
}

async fn get_values(
    path: String,
    bucket: &s3::Bucket,
) -> Result<(IdfMetadata, Vec<IdfValue>), Error> {
    let file = bucket.get_object(path).await?;
    let bytes = file.as_str()?.as_bytes();

    // HACK: specifying IdfUnit::Mm does not perform any conversions
    parse_values_csv(bytes, IdfUnit::Mm)
}

pub async fn dut_handler(
    Path(municipality_id): Path<i32>,
    State(s3_bucket): State<S3Bucket>,
) -> Result<Json<DutResponse>, (StatusCode, String)> {
    let (metadata, summer) = get_values(
        format!("{DUT_PATH}{municipality_id}_summer.csv"),
        &s3_bucket,
    )
    .await
    .map_err(error::internal_error)?;

    // Skip metadata since it's the same as summer
    let (_, winter) = get_values(
        format!("{DUT_PATH}{municipality_id}_winter.csv"),
        &s3_bucket,
    )
    .await
    .map_err(error::internal_error)?;

    Ok(Json(DutResponse {
        // TODO: it would be nice if station_id inside metadata gets converted to municipality_id
        metadata,
        unit: DutUnit::Celsius,
        summer,
        winter,
    }))
}

pub async fn dut_availability_handler(
    State(s3_bucket): State<S3Bucket>,
) -> Result<Json<IdfStationAvailability>, (StatusCode, String)> {
    let path = format!("{DUT_PATH}metadata.csv");
    let metadata = s3_bucket
        .get_object(path)
        .await
        .map_err(error::internal_error)?;

    let bytes = metadata.as_str().map_err(error::internal_error)?.as_bytes();
    let stations = parse_metadata_csv(bytes).map_err(error::internal_error)?;

    // TODO: it would be nice if station_id inside metadata gets converted to municipality_id
    Ok(Json(IdfStationAvailability { stations }))
}
