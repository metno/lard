use chrono::{DateTime, Duration, TimeZone, Utc};
use rdkafka::producer::FutureProducer;
use reqwest::{Client, StatusCode};

use lard_egress::{PatchworkAvailableResp, PatchworkResp, patchwork::PatchworkTables};
use util::{DbPools, mock::auth::create_mock_jwt};

pub mod common;
use common::{
    Param, TestData,
    legacy::{IngestData, e2e_test_wrapper_legacy, ingest_raw},
};

#[tokio::test]
async fn test_patchwork_available_endpoint() {
    // We insert a single timeseries so we will only get out a single label
    let n_labels = 1;

    e2e_test_wrapper_legacy(
        &["TA"],
        async |producer: FutureProducer, db_pools: DbPools, tables: PatchworkTables| {
            let data = IngestData::new(vec![TestData {
                station_id: 20001,
                params: vec![Param::new("TA")],
                start_time: Utc.with_ymd_and_hms(2024, 12, 15, 0, 0, 0).unwrap(),
                period: Duration::hours(1),
                type_id: 508,
                len: 2,
            }]);

            ingest_raw(&data, producer, db_pools, tables).await;

            let url = "http://localhost:3000/patchwork/available";
            let resp = reqwest::get(url).await.unwrap();
            assert!(resp.status().is_success());

            let json: PatchworkAvailableResp = resp.json().await.unwrap();
            assert_eq!(json.available.len(), n_labels);
        },
    )
    .await
}

#[tokio::test]
async fn test_patchwork_endpoint_failure() {
    let cases = [
        // made up param, shouldn't exist
        "?paramid=12345&level=0&sensor=0&from=2024-12-31T23:00:00Z&to=2025-01-01T01:30:00Z",
    ];

    e2e_test_wrapper_legacy(
        &["TA"],
        async |producer: FutureProducer, db_pools: DbPools, tables: PatchworkTables| {
            let data = IngestData::new(vec![TestData {
                station_id: 10001,
                params: vec![Param::new("TA")],
                start_time: Utc.with_ymd_and_hms(2024, 12, 31, 0, 0, 0).unwrap(),
                period: Duration::hours(1),
                type_id: 501,
                len: 48,
            }]);

            ingest_raw(&data, producer, db_pools, tables).await;

            for params in cases {
                let url = format!("http://localhost:3000/patchwork/station/10001{}", params);
                let resp = reqwest::get(url).await.unwrap();
                assert!(resp.status().is_client_error()); // expect 404
            }
        },
    )
    .await
}

#[tokio::test]
async fn test_patchwork_endpoint() {
    let token_permitid5 = create_mock_jwt(vec!["read-permitid-5".to_string()]).unwrap_or_default();
    let token_stationid1234 =
        create_mock_jwt(vec!["read-stationid-1234".to_string()]).unwrap_or_default();
    let token_both = create_mock_jwt(vec![
        "read-permitid-5".to_string(),
        "read-stationid-1234".to_string(),
    ])
    .unwrap_or_default();
    let token_nothing = create_mock_jwt(vec![]).unwrap_or_default();

    // Use values present in the mocks
    let cases = vec![
        (
            10001,
            // default level for 211 is 200
            // we also default to sensor 0
            "?paramid=211\
            &from=2024-12-31T23:00:00Z\
            &to=2025-01-01T01:30:00Z",
            None,
            200,
            3,
        ),
        // default level for grass param is actually None
        (
            20001,
            "?paramid=225\
            &from=2024-12-31T23:00:00Z\
            &to=2025-01-01T01:30:00Z",
            None,
            200,
            3,
        ),
        // 99995 has permitid 5 in mock_permit_tables(), so is restricted
        (
            99995,
            "?paramid=211\
            &from=2024-12-31T23:00:00Z\
            &to=2025-01-01T01:30:00Z",
            None, // no token, no data access
            404,  // just don't see it...
            0,
        ),
        (
            99995,
            "?paramid=211\
            &from=2024-12-31T23:00:00Z\
            &to=2025-01-01T01:30:00Z",
            Some(token_permitid5), // token with permitid 5, should have access
            200,
            3,
        ),
        // check functionality to open for a specific station (that we don't have a permit for)
        (
            1234,
            "?paramid=211\
            &from=2024-12-31T23:00:00Z\
            &to=2025-01-01T01:30:00Z",
            Some(token_nothing), // token with no stationid access, should not have access
            404,                 // just don't see it...
            0,
        ),
        (
            1234,
            "?paramid=211\
            &from=2024-12-31T23:00:00Z\
            &to=2025-01-01T01:30:00Z",
            Some(token_stationid1234),
            200,
            2,
        ),
        (
            1234,
            // leave the sensor and level here to check if also works
            // even if they would default to the same values
            "?paramid=211\
            &level=200\
            &sensor=0\
            &from=2024-12-31T23:00:00Z\
            &to=2025-01-01T01:30:00Z",
            Some(token_both), // should still work if we have both stationid and permitid access
            200,
            2,
        ),
    ];

    e2e_test_wrapper_legacy(
        &["TA", "TGX"],
        async |producer: FutureProducer, db_pools: DbPools, tables: PatchworkTables| {
            let t1: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 12, 31, 20, 0, 0).unwrap();
            let test_data = IngestData::new(vec![
                TestData {
                    station_id: 10001,
                    params: vec![Param::new("TA")],
                    start_time: t1,
                    period: Duration::hours(1),
                    type_id: 508,
                    len: 8,
                },
                TestData {
                    station_id: 10001,
                    params: vec![Param::new("TA")],
                    start_time: t1,
                    period: Duration::hours(1),
                    type_id: 501,
                    len: 8,
                },
                TestData {
                    station_id: 20001,
                    params: vec![Param::new("TGX")],
                    start_time: t1,
                    period: Duration::hours(1),
                    type_id: 501,
                    len: 8,
                },
                TestData {
                    station_id: 99995,
                    params: vec![Param::new("TA")],
                    start_time: t1,
                    period: Duration::hours(1),
                    type_id: 508,
                    len: 8,
                },
                TestData {
                    station_id: 99995,
                    params: vec![Param::new("TA")],
                    start_time: t1,
                    period: Duration::hours(1),
                    type_id: 501,
                    len: 8,
                },
                TestData {
                    station_id: 1234,
                    params: vec![Param::new("TA")],
                    start_time: t1,
                    period: Duration::hours(1),
                    type_id: 501,
                    len: 8,
                },
            ]);

            ingest_raw(&test_data, producer, db_pools, tables).await;

            for (station_id, params, token, status, n_data_found) in cases {
                let url = format!(
                    "http://localhost:3000/patchwork/station/{}{}",
                    station_id, params
                );
                let client = Client::new();
                let request = match token {
                    Some(t) => client.get(url).bearer_auth(t),
                    None => client.get(url).basic_auth("test", Some("test")),
                };

                let resp = request.send().await.unwrap();
                assert!(resp.status() == status);

                if status == StatusCode::OK {
                    let json: PatchworkResp = resp.json().await.unwrap();
                    assert_eq!(json.data.len(), n_data_found);
                }
            }
        },
    )
    .await
}
