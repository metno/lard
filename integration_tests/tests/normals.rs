use lard_egress::reports::{NormalsAvailability, NormalsResp};
use util::normals_parse::{
    create_normals_csv_content, parse_normals_csv_file, Normal, NormalMetadata, NORMALS_S3_PATH,
};

pub mod common;
use crate::common::s3_test_wrapper;

#[tokio::test]
async fn test_normals_station_availability() {
    let file = (
        NORMALS_S3_PATH,
        "monthly_metadata.csv",
        "12345,\"number_of_days_gte(sum(precipitation_amount P1D) P1M 1.0),sum(precipitation_amount P6M)\"",
    );
    s3_test_wrapper(file, async || {
        let url = "http://localhost:3000/reports/normals";
        let expected_resp = NormalsAvailability {
            normals: vec![
                NormalMetadata::new(
                    12345,
                    "number_of_days_gte(sum(precipitation_amount P1D) P1M 1.0),sum(precipitation_amount P6M)".to_string(),
                ),
            ],
        };

        let resp = reqwest::get(url).await.unwrap();
        if !resp.status().is_success() {
            panic!("Error: {}", resp.text().await.unwrap())
        }

        let json: NormalsAvailability = resp.json().await.unwrap();
        assert_eq!(json, expected_resp);
    })
    .await
}

#[tokio::test]
async fn test_normals_single() {
    let file = (
        NORMALS_S3_PATH,
        "monthly_12345.csv",
        "1,number_of_days_gte(sum(precipitation_amount P1D) P1M 1.0),10.8,1991,2020
26,sum(precipitation_amount P6M),481,1991,2020",
    );

    s3_test_wrapper(file, async || {
        let stations = [
            (
                12345,
                Some(NormalsResp {
                    data: vec![
                        Normal::new(
                            1,
                            "number_of_days_gte(sum(precipitation_amount P1D) P1M 1.0)".to_string(),
                            10.8,
                            1991,
                            2020,
                        ),
                        Normal::new(
                            26,
                            "sum(precipitation_amount P6M)".to_string(),
                            481.0,
                            1991,
                            2020,
                        ),
                    ],
                }),
                "available station_id",
            ),
            (99999, None, "wrong station_id"),
        ];

        for (id, expected, case) in stations {
            let url = format!("http://localhost:3000/reports/normals/{id}");

            let resp = reqwest::get(url).await.expect(case);

            match expected {
                Some(expected) => {
                    if !resp.status().is_success() {
                        panic!("Error: {}", resp.text().await.unwrap())
                    }

                    let json: NormalsResp = resp.json().await.expect(case);
                    assert_eq!(json, expected, "{case}");
                }
                None => {
                    assert!(resp.status().is_client_error(), "{case}")
                }
            }
        }
    })
    .await
}

#[tokio::test]
async fn test_normals_read_file() {
    // current directory is /integration_tests
    let file_path = "mock_report_files/mock_normals.csv";
    let hashmap_data = parse_normals_csv_file(file_path).unwrap();
    let result = create_normals_csv_content(hashmap_data, "monthly").unwrap();

    // then a tuple called monthly_12345.csv should exist (as well as monthly_metadata.csv)
    let found_file = result
        .iter()
        .find(|(name, _content)| name == "monthly_12345.csv")
        .unwrap();
    let file: (&str, &str, &str) = (NORMALS_S3_PATH, &found_file.0, &found_file.1);

    s3_test_wrapper(file, async || {
        let stations = [
            (
                12345,
                Some(NormalsResp {
                    data: vec![
                        Normal::new(
                            1,
                            "number_of_days_gte(sum(precipitation_amount P1D) P1M 1.0)".to_string(),
                            10.8,
                            1991,
                            2020,
                        ),
                        Normal::new(
                            26,
                            "sum(precipitation_amount P6M)".to_string(),
                            481.0,
                            1991,
                            2020,
                        ),
                    ],
                }),
                "available station_id",
            ),
            (99999, None, "wrong station_id"),
        ];

        for (id, expected, case) in stations {
            let url = format!("http://localhost:3000/reports/normals/{id}");

            let resp = reqwest::get(url).await.expect(case);

            match expected {
                Some(expected) => {
                    if !resp.status().is_success() {
                        panic!("Error: {}", resp.text().await.unwrap())
                    }
                    let json: NormalsResp = resp.json().await.expect(case);
                    assert_eq!(json, expected, "{case}");
                }
                None => {
                    assert!(resp.status().is_client_error(), "{case}")
                }
            }
        }
    })
    .await
}
