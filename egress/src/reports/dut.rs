use axum::{
    extract::{Path, State},
    Json,
};
use http::StatusCode;
use serde::{Deserialize, Serialize};
use util::dut_parse::{DutMetadata, Season, DUT_S3_PATH};
use util::idf_parse::IdfValue;

use crate::{
    error::{self, Error},
    S3Bucket,
};

/// Response struct returned by the availability endpoint
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct DutAvailability {
    pub municipalities: Vec<DutMetadata>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum DutUnit {
    #[serde(rename = "degC")]
    Celsius,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct DutResponse {
    pub metadata: DutMetadata,
    pub unit: DutUnit,
    pub values: Vec<DutResponseValue>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct DutResponseValue {
    pub season: Season,
    pub duration: u32,
    pub frequency: i32,
    pub intensity: f64,
    pub lower_interval: f64,
    pub upper_interval: f64,
}

async fn get_values(
    path: String,
    bucket: &s3::Bucket,
) -> Result<(DutMetadata, Vec<(Season, IdfValue)>), Error> {
    let file = bucket.get_object(path).await?;
    let bytes = file.as_str()?.as_bytes();

    parse_values_csv(bytes, DutUnit::Celsius)
}

pub async fn dut_handler(
    Path(municipality_id): Path<i32>,
    State(s3_bucket): State<S3Bucket>,
) -> Result<Json<DutResponse>, (StatusCode, String)> {
    let (metadata, values) = get_values(format!("{DUT_S3_PATH}{municipality_id}.csv"), &s3_bucket)
        .await
        .map_err(error::internal_error)?;

    Ok(Json(DutResponse {
        // TODO: it would be nice if station_id inside metadata gets converted to municipality_id
        metadata,
        unit: DutUnit::Celsius,
        values: values
            .into_iter()
            .map(|(season, value)| DutResponseValue {
                season,
                duration: value.duration,
                frequency: value.frequency,
                intensity: value.intensity,
                lower_interval: value.lower_interval,
                upper_interval: value.upper_interval,
            })
            .collect(),
    }))
}

pub async fn dut_availability_handler(
    State(s3_bucket): State<S3Bucket>,
) -> Result<Json<DutAvailability>, (StatusCode, String)> {
    let path = format!("{DUT_S3_PATH}metadata.csv");
    let metadata = s3_bucket
        .get_object(path)
        .await
        .map_err(error::internal_error)?;

    let bytes = metadata.as_str().map_err(error::internal_error)?.as_bytes();
    let municipalities = parse_metadata_csv(bytes).map_err(error::internal_error)?;

    // TODO: it would be nice if station_id inside metadata gets converted to municipality_id
    Ok(Json(DutAvailability { municipalities }))
}

pub fn parse_values_csv(
    bytes: &[u8],
    _unit: DutUnit,
) -> Result<(DutMetadata, Vec<(Season, IdfValue)>), Error> {
    // flexible allows us to store metadata in the header
    let mut reader = csv::ReaderBuilder::new().flexible(true).from_reader(bytes);

    // TODO: duplicated metadata record in station csv header row, are there better options?
    let metadata: DutMetadata = {
        let header = reader.headers()?;
        // NOTE: requires column order to be same as struct field order
        header.deserialize(None)?
    };

    let values: Vec<(Season, IdfValue)> = reader
        // NOTE: requires column order to be same as struct field order
        .into_records()
        .map(|res| {
            let value: (Season, IdfValue) = res?.deserialize(None)?;
            Ok((value.0, value.1))
        })
        .collect::<Result<Vec<(Season, IdfValue)>, Error>>()?;

    Ok((metadata, values))
}

pub fn parse_metadata_csv(bytes: &[u8]) -> Result<Vec<DutMetadata>, csv::Error> {
    // NOTE: requires column order to be same as struct field order
    csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(bytes)
        .into_deserialize()
        .collect::<Result<Vec<DutMetadata>, csv::Error>>()
}
