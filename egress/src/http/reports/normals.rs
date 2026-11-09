use axum::{
    Json,
    extract::{Path, State},
};
use futures::future::join;
use http::StatusCode;
use serde::{Deserialize, Serialize};

use crate::{
    S3Bucket,
    reports::normals::{get_diurnal, get_monthly, parse_metadata_json},
};
use util::{
    http_error::internal,
    normals_parse::{NORMALS_S3_PATH, Normal, NormalMetadata},
};

/// Response struct returned by the availability endpoint
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Availability {
    pub normals: Vec<NormalMetadata>,
}

/// Response struct returned by the normals endpoint
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Resp {
    pub data: Vec<Normal>,
}

pub async fn handler(
    Path(station_id): Path<i32>,
    State(s3_bucket): State<S3Bucket>,
) -> Result<Json<Resp>, (StatusCode, String)> {
    let s3_bucket = s3_bucket.ok_or_else(|| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "no s3 bucket".to_string(),
        )
    })?;
    // can't assume have both monthly and diurnal? So need to fetch both and combine if exist
    let (monthly, diurnal) = join(
        get_monthly(station_id, &s3_bucket),
        get_diurnal(station_id, &s3_bucket),
    )
    .await;

    match (diurnal, monthly) {
        (Ok(d_v), Ok(m_v)) => Ok(Json(Resp {
            data: d_v.into_iter().chain(m_v).collect(),
        })),
        // could also check if the status of the error is 404
        // is it ok to assume since one is ok, the other was not found?
        (Ok(d_v), Err(_)) => Ok(Json(Resp { data: d_v })),
        (Err(_), Ok(m_v)) => Ok(Json(Resp { data: m_v })),
        (Err(e1), Err(e2)) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!(
                "No normals found for station_id {station_id}: diurnal error: {e1}, monthly error: {e2}"
            ),
        )),
    }
}

pub async fn availability_handler(
    State(s3_bucket): State<S3Bucket>,
) -> Result<Json<Availability>, (StatusCode, String)> {
    let s3_bucket = s3_bucket.ok_or_else(|| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "no s3 bucket".to_string(),
        )
    })?;
    let path_monthly = format!("{NORMALS_S3_PATH}monthly_metadata.json");
    let metadata_monthly = s3_bucket.get_object(path_monthly).await;
    let path_diurnal = format!("{NORMALS_S3_PATH}diurnal_metadata.json");
    let metadata_diurnal = s3_bucket.get_object(path_diurnal).await;

    match (metadata_diurnal, metadata_monthly) {
        (Ok(d), Ok(m)) => {
            let d_bytes = d.as_str().map_err(internal)?.as_bytes();
            let m_bytes = m.as_str().map_err(internal)?.as_bytes();

            let mut d_normals = parse_metadata_json(d_bytes).map_err(internal)?;
            let mut m_normals = parse_metadata_json(m_bytes).map_err(internal)?;

            d_normals.append(&mut m_normals);

            Ok(Json(Availability { normals: d_normals }))
        }
        // could also check if the status of the error is 404
        // is it ok to assume since one is ok, the other was not found?
        (Ok(d), Err(_)) => {
            let d_bytes = d.as_str().map_err(internal)?.as_bytes();
            let d_normals = parse_metadata_json(d_bytes).map_err(internal)?;

            Ok(Json(Availability { normals: d_normals }))
        }
        (Err(_), Ok(m)) => {
            let m_bytes = m.as_str().map_err(internal)?.as_bytes();
            let m_normals = parse_metadata_json(m_bytes).map_err(internal)?;

            Ok(Json(Availability { normals: m_normals }))
        }
        (Err(e1), Err(e2)) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("No available normals found: diurnal error: {e1}, monthly error: {e2}"),
        )),
    }
}
