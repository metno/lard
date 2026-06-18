use axum::{
    Json,
    extract::{Path, State},
};
use http::StatusCode;
use serde::{Deserialize, Serialize};

use crate::{Error, S3Bucket};
use util::{
    http_error::{internal, not_found},
    records_parse::{RECORDS_S3_PATH, Record},
};

/// Response struct returned by the availability endpoint
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct RecordsAvailability {
    pub params: Vec<i32>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct RecordsResp {
    pub param: i32,
    pub data: Vec<Record>,
}

async fn get_values(path: String, bucket: &s3::Bucket) -> Result<Vec<Record>, Error> {
    let file = bucket.get_object(path).await?;
    let bytes = file.as_str()?.as_bytes();

    parse_values_csv(bytes)
}

pub async fn records_handler(
    Path(param_id): Path<i32>,
    State(s3_bucket): State<S3Bucket>,
) -> Result<Json<RecordsResp>, (StatusCode, String)> {
    let values = get_values(
        format!("{RECORDS_S3_PATH}{param_id}.csv"),
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
    .map_err(|err| match &err {
        Error::S3(s3::error::S3Error::HttpFailWithBody(404, _)) => not_found(err),
        _ => internal(err),
    })?;

    Ok(Json(RecordsResp {
        param: param_id,
        data: values,
    }))
}

pub async fn records_availability_handler(
    State(s3_bucket): State<S3Bucket>,
) -> Result<Json<RecordsAvailability>, (StatusCode, String)> {
    let path = format!("{RECORDS_S3_PATH}metadata.csv");
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
    let params = parse_metadata_csv(bytes).map_err(internal)?;

    Ok(Json(RecordsAvailability { params }))
}

pub fn parse_values_csv(bytes: &[u8]) -> Result<Vec<Record>, Error> {
    let reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(bytes);

    let values = reader
        // NOTE: requires column order to be same as struct field order
        .into_deserialize()
        .collect::<Result<Vec<Record>, csv::Error>>()?;

    Ok(values)
}

pub fn parse_metadata_csv(bytes: &[u8]) -> Result<Vec<i32>, csv::Error> {
    // NOTE: requires column order to be same as struct field order
    csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(bytes)
        .into_deserialize()
        .collect::<Result<Vec<i32>, csv::Error>>()
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::NaiveDate;
    use csv::Reader;
    use std::{collections::HashMap, sync::LazyLock, vec};
    use util::{
        records_parse::{Record, create_records_csv_content, parse_records_csv_content},
        stinfofacade::elem::Tables,
    };

    // need to mock these tables...
    //elem_to_param_table: HashMap<String, i32>,
    //code_to_elem_table: HashMap<String, Vec<String>>,
    static MOCK_ELEM_TO_PARAM_TABLE: LazyLock<HashMap<String, i32>> = LazyLock::new(|| {
        vec![
            ("min(air_temperature P1D)".to_string(), 3304),
            ("max(air_temperature P1D)".to_string(), 3305),
        ]
        .into_iter()
        .collect()
    });
    static MOCK_CODE_TO_ELEM_TABLE: LazyLock<HashMap<String, Vec<String>>> = LazyLock::new(|| {
        vec![
            (
                "TAN".to_string(),
                vec!["min(air_temperature P1D)".to_string()],
            ),
            (
                "TAX".to_string(),
                vec!["max(air_temperature P1D)".to_string()],
            ),
        ]
        .into_iter()
        .collect()
    });

    #[test]
    fn test_records() {
        const CSV_CONTENT: &str = r#"STNR,DATO_D,ELEM_CODE,RECORD
999,26/07/2020,TAX,35
999,10/01/2020,TAN,-35
999,11/01/2020,UNKNOWN,10
"#;
        let mut rdr = Reader::from_reader(CSV_CONTENT.as_bytes());
        let elem_tables = Tables {
            elem_to_param_table: MOCK_ELEM_TO_PARAM_TABLE.clone(),
            code_to_elem_table: MOCK_CODE_TO_ELEM_TABLE.clone(),
        };

        let hashmap_data = parse_records_csv_content(&mut rdr).unwrap();
        let map = create_records_csv_content(&hashmap_data, &elem_tables).unwrap();

        let cases = [
            (
                3305,
                Some(vec![Record {
                    station_nr: 999,
                    param_id: 3305,
                    date: NaiveDate::from_ymd_opt(2020, 7, 26).unwrap(),
                    value: 35.0,
                }]),
                "available TAX",
            ),
            (
                3304,
                Some(vec![Record {
                    station_nr: 999,
                    param_id: 3304,
                    date: NaiveDate::from_ymd_opt(2020, 1, 10).unwrap(),
                    value: -35.0,
                }]),
                "available TAN",
            ),
            (1234, None, "unavailable param id"),
        ];

        for (param, expected, case_name) in cases {
            let filename = format!("records_{param}.csv");
            let actual = map
                .iter()
                .find(|(name, _content)| *name == filename)
                .map(|(_name, content)| parse_values_csv(content.as_bytes()).unwrap());
            assert_eq!(actual, expected, "{case_name}");
        }

        assert!(
            map.iter()
                .all(|(name, _)| !name.contains("-1") && !name.contains("_0")),
            "No sentinel param IDs should appear in output filenames"
        );

        let metadata_content = map
            .iter()
            .find(|(name, _)| name == "metadata.csv")
            .map(|(_, content)| content.as_bytes())
            .expect("metadata.csv should exist");
        let metadata_params = parse_metadata_csv(metadata_content).unwrap();
        assert_eq!(metadata_params, vec![3304, 3305]);
    }
}
