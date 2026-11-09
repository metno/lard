use crate::Error;
use util::normals_parse::{NORMALS_S3_PATH, Normal, NormalMetadata};

pub async fn get_values(path: String, bucket: &s3::Bucket) -> Result<Vec<Normal>, Error> {
    let file = bucket.get_object(path).await?;
    let bytes = file.as_str()?.as_bytes();

    parse_values_json(bytes)
}

pub async fn get_monthly(station_id: i32, s3_bucket: &s3::Bucket) -> Result<Vec<Normal>, Error> {
    get_values(
        format!("{NORMALS_S3_PATH}monthly_{station_id}.json"),
        s3_bucket,
    )
    .await
}

pub async fn get_diurnal(station_id: i32, s3_bucket: &s3::Bucket) -> Result<Vec<Normal>, Error> {
    get_values(
        format!("{NORMALS_S3_PATH}diurnal_{station_id}.json"),
        s3_bucket,
    )
    .await
}

pub fn parse_values_json(bytes: &[u8]) -> Result<Vec<Normal>, Error> {
    serde_json::from_slice(bytes).map_err(Error::Json)
}

pub fn parse_metadata_json(bytes: &[u8]) -> Result<Vec<NormalMetadata>, Error> {
    serde_json::from_slice(bytes).map_err(Error::Json)
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, sync::LazyLock, vec};

    use csv::Reader;

    use super::*;
    use util::{
        normals_parse::{
            Normal, NormalType, Season, Value, create_normals_json_content,
            parse_normals_csv_content,
        },
        stinfofacade::elem::Tables,
    };

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
        let elem_tables = Tables {
            elem_to_param_table: MOCK_ELEM_TO_PARAM_TABLE.clone(),
            code_to_elem_table: MOCK_CODE_TO_ELEM_TABLE.clone(),
        };

        let hashmap_data = parse_normals_csv_content(&mut rdr, elem_tables).unwrap();
        let map = create_normals_json_content(hashmap_data, "monthly").unwrap();

        // check the metadata file ...
        let filename = "monthly_metadata.json".to_string();
        let actual = map
            .iter()
            .find(|(name, _content)| *name == filename)
            .map(|(_name, content)| parse_metadata_json(content.as_bytes()).unwrap());
        if let Some(actual) = actual {
            for x in &actual {
                // this is done explicitly, since otherwise the test is affected by ordering variations in the available stations string
                assert!(
                    x.element_id.contains(
                        "number_of_days_gte(sum(precipitation_amount P1D) P1M 1991_2020 1.0)"
                    ),
                    "Element ID be expected string"
                );
                assert!(
                    x.station_id == 12345 || x.station_id == 99999,
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
        let elem_tables = Tables {
            elem_to_param_table: MOCK_ELEM_TO_PARAM_TABLE.clone(),
            code_to_elem_table: MOCK_CODE_TO_ELEM_TABLE.clone(),
        };

        let hashmap_data = parse_normals_csv_content(&mut rdr, elem_tables.clone()).unwrap();
        let map = create_normals_json_content(hashmap_data, "monthly").unwrap();

        let stations = [
            (
                12345,
                Some(vec![
                    Normal {
                        element_id:
                            "number_of_days_gte(sum(precipitation_amount P1D) P1M 1991_2020 1.0)"
                                .to_string(),
                        param_id: 1,
                        from_year: 1991,
                        to_year: 2020,
                        normal_type: NormalType::Monthly(1),
                        value: Value::Single(10.8),
                    },
                    Normal {
                        element_id: "sum(precipitation_amount P6M 1991_2020)".to_string(),
                        param_id: 2,
                        from_year: 1991,
                        to_year: 2020,
                        normal_type: NormalType::Seasonal(Season::AprToSep),
                        value: Value::Single(481.0),
                    },
                ]),
                "available station_id",
            ),
            (99999, None, "wrong station_id"),
        ];

        for (id, expected, case) in stations {
            let filename = format!("monthly_{id}.json");
            let actual = map
                .iter()
                .find(|(name, _content)| *name == filename)
                .map(|(_name, content)| parse_values_json(content.as_bytes()).unwrap());
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
        let elem_tables = Tables {
            elem_to_param_table: MOCK_ELEM_TO_PARAM_TABLE.clone(),
            code_to_elem_table: MOCK_CODE_TO_ELEM_TABLE.clone(),
        };

        let hashmap_data = parse_normals_csv_content(&mut rdr, elem_tables.clone()).unwrap();
        let map = create_normals_json_content(hashmap_data, "monthly").unwrap();
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
                param_id: 3,
                from_year: 1961,
                to_year: 1990,
                normal_type: NormalType::Monthly(3),
                value: Value::Array(normal_array),
            }]),
            "available station_id_with array",
        )];

        for (id, expected, case) in stations {
            let filename = format!("monthly_{id}.json");
            let actual = map
                .iter()
                .find(|(name, _content)| *name == filename)
                .map(|(_name, content)| parse_values_json(content.as_bytes()).unwrap());
            assert_eq!(actual, expected, "{case}");
        }
    }
}
