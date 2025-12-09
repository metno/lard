use chrono::NaiveDate;

use lard_egress::reports::{DutResponse, DutUnit};
use util::dut_parse::{
    create_dut_csv_content, parse_dut_csv_file, DutMetadata, Season, DUT_S3_PATH,
};

use crate::common::s3_test_wrapper;
pub mod common;

#[tokio::test]
async fn test_dut_municipality_read_file() {
    // current directory is /integration_tests
    let file_path = "mock_report_files/mock_dut.csv";
    let hashmap_data = parse_dut_csv_file(file_path).unwrap();
    let result = create_dut_csv_content(hashmap_data).unwrap();

    // then a tuple called 111.csv should exist (as well as metadata.csv)
    let found_file = result
        .iter()
        .find(|(name, _content)| name == "111.csv")
        .unwrap();
    let file: (&str, &str, &str) = (DUT_S3_PATH, &found_file.0, &found_file.1);

    s3_test_wrapper(file, async || {
        let stations = [
            (
                111,
                Some(DutResponse {
                    values: vec![lard_egress::reports::DutResponseValue {
                        season: Season::Summer,
                        duration: 1,
                        frequency: 2,
                        intensity: 1.5,
                        lower_interval: 1.2,
                        upper_interval: 1.7,
                    }],
                    unit: DutUnit::Celsius,
                    metadata: DutMetadata::new(
                        111,
                        30,
                        NaiveDate::from_ymd_opt(1991, 1, 1).unwrap(),
                        NaiveDate::from_ymd_opt(2020, 12, 31).unwrap(),
                        1,
                        NaiveDate::from_ymd_opt(2022, 11, 8).unwrap(),
                    ),
                }),
                "available municipality_id",
            ),
            (99999, None, "wrong municipality_id"),
        ];

        for (id, expected, case) in stations {
            let url = format!("http://localhost:3000/reports/dut/{id}");

            let resp = reqwest::get(url).await.expect(case);

            match expected {
                Some(expected) => {
                    if !resp.status().is_success() {
                        panic!("Error: {}", resp.text().await.unwrap())
                    }

                    let json: DutResponse = resp.json().await.expect(case);
                    assert_eq!(json, expected, "{case}");
                }
                None => {
                    // TODO: this should probably return a 404
                    assert!(resp.status().is_server_error(), "{case}")
                }
            }
        }
    })
    .await
}
