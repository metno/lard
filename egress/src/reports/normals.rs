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
    pub data: Vec<(String, Vec<Normal>)>,
}

async fn get_values(
    path: String,
    bucket: &s3::Bucket,
) -> Result<Vec<(String, Vec<Normal>)>, Error> {
    let file = bucket.get_object(path).await?;
    let bytes = file.as_str()?.as_bytes();

    parse_values_csv(bytes)
}

async fn get_monthly(
    station_id: i32,
    s3_bucket: &s3::Bucket,
) -> Result<Vec<(String, Vec<Normal>)>, error::Error> {
    get_values(
        format!("{NORMALS_S3_PATH}monthly_{station_id}.csv"),
        s3_bucket,
    )
    .await
}

async fn get_diurnal(
    station_id: i32,
    s3_bucket: &s3::Bucket,
) -> Result<Vec<(String, Vec<Normal>)>, error::Error> {
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
    let s3_bucket = s3_bucket.ok_or_else(|| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "no s3 bucket".to_string(),
        )
    })?;
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

pub fn parse_values_csv(bytes: &[u8]) -> Result<Vec<(String, Vec<Normal>)>, Error> {
    // for normals we have no headers for now...
    let reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(bytes);

    let mut values = reader
        // NOTE: requires column order to be same as struct field order
        .into_records()
        .map(|res| {
            let value: (String, Vec<Normal>) = res?.deserialize(None)?;
            Ok(value)
        })
        .collect::<Result<Vec<(String, Vec<Normal>)>, Error>>()?;

    // sort by element id, so that the order is deterministic (for testing)
    values.sort_by_key(|k| k.0.clone());

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

#[cfg(test)]
mod test {
    use super::*;
    use csv::Reader;
    use util::normals_parse::{create_normals_csv_content, parse_normals_csv_content, Normal};

    #[tokio::test]
    async fn test_normals_metadata() {
        const CSV_CONTENT: &str = r#"STNR,MONTH,ELEM_CODE,NORMAL,FYEAR,TYEAR
12345,1,DRR_GE1,10.8,1991,2020
99999,1,DRR_GE1,10.8,1991,2020
"#;
        let mut rdr = Reader::from_reader(CSV_CONTENT.as_bytes());

        let hashmap_data = parse_normals_csv_content(&mut rdr, "monthly").unwrap();
        let map = create_normals_csv_content(hashmap_data, "monthly").unwrap();

        // check the metadata file ...
        let filename = "monthly_metadata.csv".to_string();
        let actual = map
            .iter()
            .find(|(name, _content)| *name == filename)
            .map(|(_name, content)| parse_metadata_csv(content.as_bytes()).unwrap());
        if let Some(actual) = actual {
            for x in &actual {
                // this is done explicitly, since otherwise the test is affected by ordering variations in the available stations string
                assert!(
                    x.element_id
                        .contains("number_of_days_gte(sum(precipitation_amount P1D) P1M 1.0)"),
                    "Element ID be expected string"
                );
                assert!(
                    x.available_stations.contains("12345")
                        && x.available_stations.contains("99999"),
                    "Available stations should contain both 12345 and 99999"
                );
            }
        } else {
            panic!("Metadata file not found or failed to parse");
        }
    }

    #[test]
    fn test_normals_parse_content() {
        const CSV_CONTENT: &str = r#"STNR,MONTH,ELEM_CODE,NORMAL,FYEAR,TYEAR
12345,1,DRR_GE1,10.8,1991,2020
12345,26,RR,481,1991,2020
"#;
        let mut rdr = Reader::from_reader(CSV_CONTENT.as_bytes());

        let hashmap_data = parse_normals_csv_content(&mut rdr, "monthly").unwrap();
        let map = create_normals_csv_content(hashmap_data, "monthly").unwrap();

        let stations = [
            (
                12345,
                Some(vec![
                    (
                        "number_of_days_gte(sum(precipitation_amount P1D) P1M 1.0)".to_string(),
                        vec![Normal::new(1, None, 10.8, 1991, 2020)],
                    ),
                    (
                        "sum(precipitation_amount P6M)".to_string(),
                        vec![Normal::new(26, None, 481.0, 1991, 2020)],
                    ),
                ]),
                "available station_id",
            ),
            (99999, None, "wrong station_id"),
        ];

        for (id, expected, case) in stations {
            let filename = format!("monthly_{id}.csv");
            let actual = map
                .iter()
                .find(|(name, _content)| *name == filename)
                .map(|(_name, content)| parse_values_csv(content.as_bytes()).unwrap());
            assert_eq!(actual, expected, "{case}");
        }
    }
}
