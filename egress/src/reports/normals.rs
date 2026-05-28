use axum::{
    Json,
    extract::{Path, State},
};
use futures::future::join;
use http::StatusCode;
use serde::{Deserialize, Serialize};

use crate::{Error, S3Bucket};
use util::{
    http_error::internal,
    normals_parse::{NORMALS_S3_PATH, Normal, NormalMetadata},
};

/// Response struct returned by the availability endpoint
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct NormalsAvailability {
    pub normals: Vec<(String, Vec<i32>)>,
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

async fn get_monthly(station_id: i32, s3_bucket: &s3::Bucket) -> Result<Vec<Normal>, Error> {
    get_values(
        format!("{NORMALS_S3_PATH}monthly_{station_id}.csv"),
        s3_bucket,
    )
    .await
}

async fn get_diurnal(station_id: i32, s3_bucket: &s3::Bucket) -> Result<Vec<Normal>, Error> {
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

    match (diurnal, monthly) {
        (Ok(d_v), Ok(m_v)) => Ok(Json(NormalsResp {
            data: d_v.into_iter().chain(m_v).collect(),
        })),
        // could also check if the status of the error is 404
        // is it ok to assume since one is ok, the other was not found?
        (Ok(d_v), Err(_)) => Ok(Json(NormalsResp { data: d_v })),
        (Err(_), Ok(m_v)) => Ok(Json(NormalsResp { data: m_v })),
        (Err(e1), Err(e2)) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!(
                "No normals found for station_id {station_id}: diurnal error: {e1}, monthly error: {e2}"
            ),
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

    match (metadata_diurnal, metadata_monthly) {
        (Ok(d), Ok(m)) => {
            let d_bytes = d.as_str().map_err(internal)?.as_bytes();
            let m_bytes = m.as_str().map_err(internal)?.as_bytes();

            let mut d_normals = parse_metadata_csv(d_bytes).map_err(internal)?;
            let mut m_normals = parse_metadata_csv(m_bytes).map_err(internal)?;

            d_normals.append(&mut m_normals);

            Ok(Json(NormalsAvailability { normals: d_normals }))
        }
        // could also check if the status of the error is 404
        // is it ok to assume since one is ok, the other was not found?
        (Ok(d), Err(_)) => {
            let d_bytes = d.as_str().map_err(internal)?.as_bytes();
            let d_normals = parse_metadata_csv(d_bytes).map_err(internal)?;

            Ok(Json(NormalsAvailability { normals: d_normals }))
        }
        (Err(_), Ok(m)) => {
            let m_bytes = m.as_str().map_err(internal)?.as_bytes();
            let m_normals = parse_metadata_csv(m_bytes).map_err(internal)?;

            Ok(Json(NormalsAvailability { normals: m_normals }))
        }
        (Err(e1), Err(e2)) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("No available normals found: diurnal error: {e1}, monthly error: {e2}"),
        )),
    }
}

pub fn parse_values_csv(bytes: &[u8]) -> Result<Vec<Normal>, Error> {
    // for normals we have no headers for now...
    let reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(bytes);

    let values = reader
        // NOTE: requires column order to be same as struct field order
        .into_deserialize()
        .collect::<Result<Vec<Normal>, csv::Error>>()?;

    Ok(values)
}

pub fn parse_metadata_csv(bytes: &[u8]) -> Result<Vec<(String, Vec<i32>)>, Error> {
    // NOTE: requires column order to be same as struct field order
    let metadata = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(bytes)
        .into_deserialize()
        .collect::<Result<Vec<NormalMetadata>, csv::Error>>()?;
    // but then convert the string of stations to a vec
    let parsed_metadata = metadata
        .iter()
        .map(|m| {
            (
                m.element_id.clone(),
                m.available_stations
                    .split(",")
                    .filter_map(|s| s.parse().ok())
                    .collect::<Vec<i32>>(),
            )
        })
        .collect();
    Ok(parsed_metadata)
}

#[cfg(test)]
mod test {
    use std::sync::LazyLock;
    use std::vec;
    use std::{
        collections::HashMap,
        sync::{Arc, RwLock},
    };

    use super::*;
    use csv::Reader;
    use util::normals_parse::{Normal, create_normals_csv_content, parse_normals_csv_content};
    use util::stinfofacade::elem::Tables;

    // need to mock these tables...
    //elem_to_param_table: HashMap<String, i32>,
    //code_to_elem_table: HashMap<String, Vec<String>>,
    static MOCK_ELEM_TO_PARAM_TABLE: LazyLock<HashMap<String, i32>> = LazyLock::new(|| {
        vec![
            (
                "number_of_days_gte(sum(precipitation_amount P1D) P1M 1991_2020 1.0)".to_string(),
                1,
            ),
            ("sum(precipitation_amount P6M 1991_2020)".to_string(), 2),
            (
                "frequency_group_thresholds(precipitation_amount P1M 1961_1990)".to_string(),
                3,
            ),
        ]
        .into_iter()
        .collect()
    });
    static MOCK_CODE_TO_ELEM_TABLE: LazyLock<HashMap<String, Vec<String>>> = LazyLock::new(|| {
        vec![
            (
                "DRR_GE1".to_string(),
                vec![
                    "number_of_days_gte(sum(precipitation_amount P1D) P1M 1991_2020 1.0)"
                        .to_string(),
                ],
            ),
            (
                "RR".to_string(),
                vec!["sum(precipitation_amount P6M 1991_2020)".to_string()],
            ),
            (
                "RRGRP".to_string(),
                vec!["frequency_group_thresholds(precipitation_amount P1M 1961_1990)".to_string()],
            ),
        ]
        .into_iter()
        .collect()
    });

    #[tokio::test]
    async fn test_normals_metadata() {
        const CSV_CONTENT: &str = r#"STNR,MONTH,ELEM_CODE,NORMAL,FYEAR,TYEAR
12345,1,DRR_GE1,10.8,1991,2020
99999,1,DRR_GE1,10.8,1991,2020
"#;
        let mut rdr = Reader::from_reader(CSV_CONTENT.as_bytes());
        let elem_tables = Arc::new(RwLock::new(Tables {
            elem_to_param_table: MOCK_ELEM_TO_PARAM_TABLE.clone(),
            code_to_elem_table: MOCK_CODE_TO_ELEM_TABLE.clone(),
        }));

        let hashmap_data = parse_normals_csv_content(&mut rdr, elem_tables).unwrap();
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
                    x.0.contains(
                        "number_of_days_gte(sum(precipitation_amount P1D) P1M 1991_2020 1.0)"
                    ),
                    "Element ID be expected string"
                );
                assert!(
                    x.1.contains(&12345) && x.1.contains(&99999),
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
        let elem_tables = Arc::new(RwLock::new(Tables {
            elem_to_param_table: MOCK_ELEM_TO_PARAM_TABLE.clone(),
            code_to_elem_table: MOCK_CODE_TO_ELEM_TABLE.clone(),
        }));

        let hashmap_data = parse_normals_csv_content(&mut rdr, elem_tables.clone()).unwrap();
        let map = create_normals_csv_content(hashmap_data, "monthly").unwrap();

        let stations = [
            (
                12345,
                Some(vec![
                    Normal {
                        element_id:
                            "number_of_days_gte(sum(precipitation_amount P1D) P1M 1991_2020 1.0)"
                                .to_string(),
                        param_id: Some(1),
                        period: "1991_2020".to_string(),
                        month: 1,
                        day: None,
                        normal_value: Some(10.8),
                        normal_array: None,
                    },
                    Normal {
                        element_id: "sum(precipitation_amount P6M 1991_2020)".to_string(),
                        param_id: Some(2),
                        period: "1991_2020".to_string(),
                        month: 26,
                        day: None,
                        normal_value: Some(481.0),
                        normal_array: None,
                    },
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

    #[test]
    fn test_normals_parse_content_that_results_in_array() {
        const CSV_CONTENT: &str = r#"STNR,MONTH,ELEM_CODE,NORMAL,FYEAR,TYEAR
12345,3,RRGRP0,5.4,1961,1990
12345,3,RRGRP1,17.9,1961,1990
12345,3,RRGRP2,26.0,1961,1990
12345,3,RRGRP3,42.0,1961,1990
12345,3,RRGRP4,55.0,1961,1990
12345,3,RRGRP5,67.1,1961,1990
12345,3,RRGRP6,88.0,1961,1990
"#;
        let mut rdr = Reader::from_reader(CSV_CONTENT.as_bytes());
        let elem_tables = Arc::new(RwLock::new(Tables {
            elem_to_param_table: MOCK_ELEM_TO_PARAM_TABLE.clone(),
            code_to_elem_table: MOCK_CODE_TO_ELEM_TABLE.clone(),
        }));

        let hashmap_data = parse_normals_csv_content(&mut rdr, elem_tables.clone()).unwrap();
        let map = create_normals_csv_content(hashmap_data, "monthly").unwrap();
        let normal_array: [Option<f64>; 7] = [
            Some(5.4),
            Some(17.9),
            Some(26.0),
            Some(42.0),
            Some(55.0),
            Some(67.1),
            Some(88.0),
        ];

        let stations = [(
            12345,
            Some(vec![Normal {
                element_id: "frequency_group_thresholds(precipitation_amount P1M 1961_1990)"
                    .to_string(),
                param_id: Some(3),
                period: "1961_1990".to_string(),
                month: 3,
                day: None,
                normal_value: None,
                normal_array: Some(normal_array),
            }]),
            "available station_id_with array",
        )];

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
