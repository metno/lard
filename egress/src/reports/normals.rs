use axum::{
    extract::{Path, State},
    Json,
};
use futures::future::join;
use http::StatusCode;
use serde::{Deserialize, Serialize};

use crate::{
    error::{self, Error},
    S3Bucket,
};

use util::normals_parse::{Normal, NormalMetadata, NORMALS_S3_PATH};

/// Response struct returned by the availability endpoint
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct NormalsAvailability {
    pub normals: Vec<NormalMetadata>,
}

/// Response struct returned by the normals endpoint
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct NormalsResp {
    pub data: Vec<Normal>,
}

async fn get_values(path: String, bucket: &s3::Bucket) -> Result<Vec<Normal>, Error> {
    let file = bucket.get_object(path).await?;
    let bytes = file.as_str()?.as_bytes();

    parse_values_csv(bytes)
}

async fn get_monthly(station_id: i32, s3_bucket: &s3::Bucket) -> Result<Vec<Normal>, error::Error> {
    get_values(
        format!("{NORMALS_S3_PATH}monthly_{station_id}.csv"),
        s3_bucket,
    )
    .await
}

async fn get_diurnal(station_id: i32, s3_bucket: &s3::Bucket) -> Result<Vec<Normal>, error::Error> {
    get_values(
        format!("{NORMALS_S3_PATH}diurnal_{station_id}.csv"),
        s3_bucket,
    )
    .await
}

pub async fn normals_handler(
    Path(station_id): Path<i32>,
    State(s3_bucket): State<S3Bucket>,
) -> Result<Json<NormalsResp>, (StatusCode, String)> {
    // can't assume have both monthly and diurnal? So need to fetch both and combine if exist
    let (monthly, diurnal) = join(
        get_monthly(station_id, &s3_bucket),
        get_diurnal(station_id, &s3_bucket),
    )
    .await;

    let opt_monthly = monthly.ok();
    let opt_diurnal = diurnal.ok();

    match (opt_diurnal, opt_monthly) {
        (Some(d_v), Some(m_v)) => Ok(Json(NormalsResp {
            data: d_v.into_iter().chain(m_v.into_iter()).collect(),
        })),
        (Some(d_v), None) => Ok(Json(NormalsResp { data: d_v })),
        (None, Some(m_v)) => Ok(Json(NormalsResp { data: m_v })),
        (None, None) => Err((
            StatusCode::NOT_FOUND,
            format!("No normals found for station ID {}", station_id),
        )),
    }
}

pub async fn normals_availability_handler(
    State(s3_bucket): State<S3Bucket>,
) -> Result<Json<NormalsAvailability>, (StatusCode, String)> {
    let path_monthly = format!("{NORMALS_S3_PATH}monthly_metadata.csv");
    let metadata_monthly = s3_bucket.get_object(path_monthly).await;
    let path_diurnal = format!("{NORMALS_S3_PATH}diurnal_metadata.csv");
    let metadata_diurnal = s3_bucket.get_object(path_diurnal).await;

    let opt_monthly = metadata_monthly.ok();
    let opt_diurnal = metadata_diurnal.ok();

    match (opt_diurnal, opt_monthly) {
        (Some(d), Some(m)) => {
            let d_bytes = d.as_str().map_err(error::internal_error)?.as_bytes();
            let m_bytes = m.as_str().map_err(error::internal_error)?.as_bytes();

            let mut d_normals = parse_metadata_csv(d_bytes).map_err(error::internal_error)?;
            let mut m_normals = parse_metadata_csv(m_bytes).map_err(error::internal_error)?;

            d_normals.append(&mut m_normals);

            Ok(Json(NormalsAvailability { normals: d_normals }))
        }
        (Some(d), None) => {
            let d_bytes = d.as_str().map_err(error::internal_error)?.as_bytes();
            let d_normals = parse_metadata_csv(d_bytes).map_err(error::internal_error)?;

            Ok(Json(NormalsAvailability { normals: d_normals }))
        }
        (None, Some(m)) => {
            let m_bytes = m.as_str().map_err(error::internal_error)?.as_bytes();
            let m_normals = parse_metadata_csv(m_bytes).map_err(error::internal_error)?;

            Ok(Json(NormalsAvailability { normals: m_normals }))
        }
        (None, None) => Err((
            StatusCode::NOT_FOUND,
            "No available normals found".to_string(),
        )),
    }
}

pub fn parse_values_csv(bytes: &[u8]) -> Result<Vec<Normal>, Error> {
    // for normals we have no headers for now...
    let reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(bytes);

    let values: Vec<Normal> = reader
        // NOTE: requires column order to be same as struct field order
        .into_records()
        .map(|res| {
            let value: Normal = res?.deserialize(None)?;
            Ok(value)
        })
        .collect::<Result<Vec<Normal>, Error>>()?;

    Ok(values)
}

pub fn parse_metadata_csv(bytes: &[u8]) -> Result<Vec<NormalMetadata>, csv::Error> {
    // NOTE: requires column order to be same as struct field order
    csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(bytes)
        .into_deserialize()
        .collect::<Result<Vec<NormalMetadata>, csv::Error>>()
}
