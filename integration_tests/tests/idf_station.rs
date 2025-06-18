use std::{panic::AssertUnwindSafe, sync::Arc};

use chrono::NaiveDate;

use futures::FutureExt;
use lard_egress::reports::{
    IdfMetadata, IdfStationAvailability, IdfStationResp, IdfUnit, IdfValue,
};
use tokio_util::sync::CancellationToken;
pub mod common;

pub async fn s3_test_wrapper((path, content): (&str, &str), test: impl AsyncFnOnce() -> ()) {
    let db_pools = common::create_db_pools().await;

    // set up cancellation token and signal catcher to detect premature shutdown
    let cancel_token = CancellationToken::new();
    let bucket: Arc<s3::Bucket> = Arc::from(
        s3::Bucket::new(
            &std::env::var("S3_BUCKET_NAME").unwrap(),
            s3::Region::from_env("AWS_REGION", Some("S3_ENDPOINT_URL")).unwrap(),
            // Requires "AWS_ACCESS_KEY_ID" and "AWS_SECRET_ACCESS_KEY" to be set
            s3::creds::Credentials::from_env().unwrap(),
        )
        .unwrap()
        // TODO: not sure what the path would be otherwise
        .with_path_style(),
    );

    if let Err(e) = bucket.put_object(path, content.as_bytes()).await {
        panic!("{e}")
    };

    let mut egress = tokio::spawn(lard_egress::run(
        db_pools.open.clone(),
        bucket,
        cancel_token.clone(),
    ));

    tokio::select! {
        _ = &mut egress => panic!("API server task terminated first"),
        // Clean up database even if test panics, to avoid test poisoning
        test_result = AssertUnwindSafe(test()).catch_unwind() => {
            // For debugging a specific test, it might be useful to skip the cleanup process
            #[cfg(not(feature = "debug"))]
            common::db_cleanup(db_pools).await;

            assert!(test_result.is_ok())
        }
    }

    cancel_token.cancel();
    egress.await.unwrap()
}

#[tokio::test]
async fn test_idf_station_availability() {
    let file = (
        "/metadata.csv",
        "12345,39,1968,2023,3,0,2024-01-01
67890,50,1999,2009,0,0,2010-01-01",
    );

    s3_test_wrapper(file, async || {
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
    let file = (
        "/12345.csv",
        "12345,39,1968,2023,3,0,2024-01-01
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
