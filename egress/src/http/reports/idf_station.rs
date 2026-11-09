use axum::{
    Json,
    extract::{Path, Query, State},
    http::StatusCode,
};
use serde::{Deserialize, Serialize};

use crate::{
    S3Bucket,
    reports::idf_station::{Unit, parse_metadata_csv, parse_values_csv},
};
use util::{
    http_error::{internal, not_found},
    idf_parse::{IDF_S3_PATH, IdfMetadata, IdfValue},
};

/// Query parameters struct for the station/:station_id endpoint
#[derive(Serialize, Deserialize)]
pub struct Params {
    #[serde(default)]
    pub unit: Unit,
}

/// Response struct returned by the station/:station_id endpoint
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Resp {
    // TODO: is this correct???
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub values: Vec<IdfValue>,
    pub unit: Unit,
    #[serde(flatten)]
    pub metadata: IdfMetadata,
}

pub async fn handler(
    Path(station_id): Path<i32>,
    State(s3_bucket): State<S3Bucket>,
    Query(params): Query<Params>,
) -> Result<Json<Resp>, (StatusCode, String)> {
    let station_file = s3_bucket
        .ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "no s3 bucket".to_string(),
            )
        })?
        // TODO: possible vulnerability?
        .get_object(format!("{IDF_S3_PATH}{station_id}.csv"))
        .await
        .map_err(not_found)?;

    let bytes = station_file.as_str().map_err(internal)?.as_bytes();

    let (metadata, values) = parse_values_csv(bytes, params.unit).map_err(internal)?;

    Ok(Json(Resp {
        metadata,
        unit: params.unit,
        values,
    }))
}

/// Response struct returned by the availability endpoint
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Availability {
    pub stations: Vec<IdfMetadata>,
}

pub async fn availability_handler(
    State(s3_bucket): State<S3Bucket>,
) -> Result<Json<Availability>, (StatusCode, String)> {
    let path = format!("{IDF_S3_PATH}metadata.csv");
    let metadata = s3_bucket
        .ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "no_s3_bucket".to_string(),
            )
        })?
        .get_object(path)
        .await
        .map_err(internal)?;

    let bytes = metadata.as_str().map_err(internal)?.as_bytes();

    let stations = parse_metadata_csv(bytes).map_err(internal)?;

    Ok(Json(Availability { stations }))
}
