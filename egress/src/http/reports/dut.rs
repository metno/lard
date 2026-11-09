use std::collections::HashMap;

use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use serde::{Deserialize, Serialize};

use crate::{
    Error, S3Bucket,
    reports::dut::{Unit, parse_metadata_csv, parse_values_csv},
};
use ::util::{
    dut_parse::{DUT_S3_PATH, DutMetadata, Season},
    http_error::{internal, not_found},
    idf_parse::IdfValue,
};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Availability {
    pub municipalities: Vec<DutMetadata>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Resp {
    pub metadata: DutMetadata,
    pub unit: Unit,
    // arrange so have an array of values for every season
    pub data: HashMap<Season, Vec<IdfValue>>,
}

async fn get_values(
    path: String,
    bucket: &s3::Bucket,
) -> Result<(DutMetadata, Vec<(Season, IdfValue)>), Error> {
    let file = bucket.get_object(path).await?;
    let bytes = file.as_str()?.as_bytes();

    parse_values_csv(bytes)
}

pub async fn handler(
    Path(municipality_id): Path<i32>,
    State(s3_bucket): State<S3Bucket>,
) -> Result<Json<Resp>, (StatusCode, String)> {
    let (metadata, values) = get_values(
        format!("{DUT_S3_PATH}{municipality_id}.csv"),
        s3_bucket
            .ok_or_else(|| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "no_s3_bucket".to_string(),
                )
            })?
            .as_ref(),
    )
    .await
    .map_err(not_found)?;

    let map: HashMap<Season, Vec<IdfValue>> =
        values
            .into_iter()
            .fold(HashMap::new(), |mut acc, (season, value)| {
                acc.entry(season).or_default().push(value);
                acc
            });

    Ok(Json(Resp {
        // TODO: it would be nice if station_id inside metadata gets converted to municipality_id
        metadata,
        unit: Unit::Celsius,
        data: map,
    }))
}

pub async fn availability_handler(
    State(s3_bucket): State<S3Bucket>,
) -> Result<Json<Availability>, (StatusCode, String)> {
    let path = format!("{DUT_S3_PATH}metadata.csv");
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
    let municipalities = parse_metadata_csv(bytes).map_err(internal)?;

    // TODO: it would be nice if station_id inside metadata gets converted to municipality_id
    Ok(Json(Availability { municipalities }))
}
