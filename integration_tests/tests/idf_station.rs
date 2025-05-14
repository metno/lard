use chrono::NaiveDate;

use lard_egress::reports::{
    IdfMetadata, IdfStationAvailability, IdfStationResp, IdfUnit, IdfValue,
};
pub mod common;

#[tokio::test]
async fn test_idf_station_availability() {
    common::e2e_test_wrapper(async {
        let url = "http://localhost:3000/reports/idf/station";
        let expected_resp = IdfStationAvailability {
            stations: vec![
                IdfMetadata::new(
                    12345,
                    39,
                    1968,
                    2023,
                    3,
                    0,
                    NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
                ),
                IdfMetadata::new(
                    67890,
                    50,
                    1999,
                    2009,
                    0,
                    0,
                    NaiveDate::from_ymd_opt(2010, 1, 1).unwrap(),
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
    common::e2e_test_wrapper(async {
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
                        1968,
                        2023,
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
                    // TODO: this should probably return a 404
                    assert!(resp.status().is_server_error(), "{case}")
                }
            }
        }
    })
    .await
}
