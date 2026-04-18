use std::collections::HashMap;

use axum::{
    Json,
    extract::{Path, State},
};
use http::StatusCode;
use serde::{Deserialize, Serialize};

use crate::{Error, S3Bucket};
use util::{
    dut_parse::{DUT_S3_PATH, DutMetadata, Season},
    http_error::{internal_error, not_found_error},
    idf_parse::IdfValue,
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
    // arrange so have an array of values for every season
    pub data: HashMap<Season, Vec<IdfValue>>,
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
    .map_err(not_found_error)?;

    let map: HashMap<Season, Vec<IdfValue>> =
        values
            .into_iter()
            .fold(HashMap::new(), |mut acc, (season, value)| {
                acc.entry(season).or_default().push(value);
                acc
            });

    Ok(Json(DutResponse {
        // TODO: it would be nice if station_id inside metadata gets converted to municipality_id
        metadata,
        unit: DutUnit::Celsius,
        data: map,
    }))
}

pub async fn dut_availability_handler(
    State(s3_bucket): State<S3Bucket>,
) -> Result<Json<DutAvailability>, (StatusCode, String)> {
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
        .map_err(internal_error)?;

    let bytes = metadata.as_str().map_err(internal_error)?.as_bytes();
    let municipalities = parse_metadata_csv(bytes).map_err(internal_error)?;

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

#[cfg(test)]
mod test {
    use super::*;
    use chrono::NaiveDate;
    use csv::Reader;
    use util::dut_parse::{create_dut_csv_content, parse_dut_csv_content};

    #[test]
    fn test_dut_municipality() {
        const CSV_CONTENT: &str = r#"stnr,retlev_2.5,retlev,retlev_97.5,duration,time_of_year,retperiod,FDATO,TDATO,SEASONS,UPDATE,SEED,REF_period
111,1.2,1.5,1.7,1,22,2,1991-01-01,2020-12-31,30,2022-11-08,1,1991-2020
"#;
        let mut rdr = Reader::from_reader(CSV_CONTENT.as_bytes());

        let hashmap_data = parse_dut_csv_content(&mut rdr).unwrap();
        let map = create_dut_csv_content(hashmap_data).unwrap();

        // then a tuple called 111.csv should exist (as well as metadata.csv)
        //let found_file = result
        //    .iter()
        //    .find(|(name, _content)| name == "111.csv")
        //    .unwrap();

        let cases = [
            (
                111,
                Some((
                    DutMetadata::new(
                        111,
                        30,
                        NaiveDate::from_ymd_opt(1991, 1, 1).unwrap(),
                        NaiveDate::from_ymd_opt(2020, 12, 31).unwrap(),
                        1,
                        NaiveDate::from_ymd_opt(2022, 11, 8).unwrap(),
                    ),
                    vec![(
                        Season::Summer,
                        IdfValue {
                            duration: 1,
                            frequency: 2,
                            intensity: 1.5,
                            lower_interval: 1.2,
                            upper_interval: 1.7,
                        },
                    )],
                )),
                "available municipality_id",
            ),
            (99999, None, "wrong municipality_id"),
        ];

        for (id, expected, case_name) in cases {
            let filename = format!("{id}.csv");
            let actual =
                map.iter()
                    .find(|(name, _content)| *name == filename)
                    .map(|(_name, content)| {
                        parse_values_csv(content.as_bytes(), DutUnit::Celsius).unwrap()
                    });
            assert_eq!(actual, expected, "{case_name}");
        }
    }
}
