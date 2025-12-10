use chrono::NaiveDate;

use lard_egress::reports::{IdfStationAvailability, IdfStationResp, IdfUnit};
use util::idf_parse::{
    create_idf_csv_content, parse_idf_csv_file, IdfMetadata, IdfMetadataAvailability, IdfValue,
    IDF_S3_PATH,
};

pub mod common;
use crate::common::s3_test_wrapper;

#[tokio::test]
async fn test_idf_station_availability() {
    let file = (
        IDF_S3_PATH,
        "metadata.csv",
        "12345,39,1968-01-01,2023-01-01,3,0,2024-01-01,\"[1|1,2|2]\"
67890,50,1999-01-01,2009-01-01,0,0,2010-01-01,\"[1|1,2|2]\"",
    );
    s3_test_wrapper(file, async || {
        let url = "http://localhost:3000/reports/idf/station";
        let expected_resp = IdfStationAvailability {
            stations: vec![
                IdfMetadataAvailability::new(
                    12345,
                    39,
                    NaiveDate::from_ymd_opt(1968, 1, 1).unwrap(),
                    NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
                    3,
                    0,
                    NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
                    vec![(1, 1), (2, 2)],
                ),
                IdfMetadataAvailability::new(
                    67890,
                    50,
                    NaiveDate::from_ymd_opt(1999, 1, 1).unwrap(),
                    NaiveDate::from_ymd_opt(2009, 1, 1).unwrap(),
                    0,
                    0,
                    NaiveDate::from_ymd_opt(2010, 1, 1).unwrap(),
                    vec![(1, 1), (2, 2)],
                ),
            ],
        };

        let resp = reqwest::get(url).await.unwrap();
        if !resp.status().is_success() {
            panic!("Error: {}", resp.text().await.unwrap())
        }

        let json: IdfStationAvailability = resp.json().await.unwrap();
        assert_eq!(json, expected_resp);
    })
    .await
}

#[tokio::test]
async fn test_idf_station_single() {
    let file = (
        IDF_S3_PATH,
        "12345.csv",
        "12345,39,1968-01-01,2023-01-01,3,0,2024-01-01
1,1,1.5,1.2,1.7
1,2,1.5,1.2,1.7
2,1,1.5,1.2,1.7
2,2,1.5,1.2,1.7",
    );

    s3_test_wrapper(file, async || {
        let stations = [
            (
                12345,
                Some(IdfStationResp {
                    values: vec![
                        IdfValue::new(1, 1, 1.5, 1.2, 1.7),
                        IdfValue::new(1, 2, 1.5, 1.2, 1.7),
                        IdfValue::new(2, 1, 1.5, 1.2, 1.7),
                        IdfValue::new(2, 2, 1.5, 1.2, 1.7),
                    ],
                    unit: IdfUnit::Mm,
                    metadata: IdfMetadata::new(
                        12345,
                        39,
                        NaiveDate::from_ymd_opt(1968, 1, 1).unwrap(),
                        NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
                        3,
                        0,
                        NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
                    ),
                }),
                "available station_id",
            ),
            (99999, None, "wrong station_id"),
        ];

        for (id, expected, case) in stations {
            let url = format!("http://localhost:3000/reports/idf/station/{id}");

            let resp = reqwest::get(url).await.expect(case);

            match expected {
                Some(expected) => {
                    if !resp.status().is_success() {
                        panic!("Error: {}", resp.text().await.unwrap())
                    }

                    let json: IdfStationResp = resp.json().await.expect(case);
                    assert_eq!(json, expected, "{case}");
                }
                None => {
                    // station not found gives 404, client error
                    assert!(resp.status().is_client_error(), "{case}")
                }
            }
        }
    })
    .await
}

#[tokio::test]
async fn test_idf_station_read_file() {
    // current directory is /integration_tests
    let file_path = "mock_report_files/mock_idf.csv";
    let hashmap_data = parse_idf_csv_file(file_path).unwrap();
    let result = create_idf_csv_content(hashmap_data).unwrap();

    // then a tuple called 12345.csv should exist (as well as metadata.csv)
    let found_file = result
        .iter()
        .find(|(name, _content)| name == "12345.csv")
        .unwrap();
    let file: (&str, &str, &str) = (IDF_S3_PATH, &found_file.0, &found_file.1);

    s3_test_wrapper(file, async || {
        let stations = [
            (
                12345,
                Some(IdfStationResp {
                    values: vec![
                        IdfValue::new(1, 1, 1.5, 1.2, 1.7),
                        IdfValue::new(1, 2, 1.5, 1.2, 1.7),
                        IdfValue::new(2, 1, 1.5, 1.2, 1.7),
                        IdfValue::new(2, 2, 1.5, 1.2, 1.7),
                    ],
                    unit: IdfUnit::Mm,
                    metadata: IdfMetadata::new(
                        12345,
                        39,
                        NaiveDate::from_ymd_opt(1968, 1, 1).unwrap(),
                        NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
                        3,
                        0,
                        NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
                    ),
                }),
                "available station_id",
            ),
            (99999, None, "wrong station_id"),
        ];

        for (id, expected, case) in stations {
            let url = format!("http://localhost:3000/reports/idf/station/{id}");

            let resp = reqwest::get(url).await.expect(case);

            match expected {
                Some(expected) => {
                    if !resp.status().is_success() {
                        panic!("Error: {}", resp.text().await.unwrap())
                    }

                    let json: IdfStationResp = resp.json().await.expect(case);
                    assert_eq!(json, expected, "{case}");
                }
                None => {
                    // station not found gives 404, client error
                    assert!(resp.status().is_client_error(), "{case}")
                }
            }
        }
    })
    .await
}
