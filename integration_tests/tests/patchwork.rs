use chrono::{DateTime, Duration, TimeZone, Utc};
use rdkafka::producer::FutureProducer;
use reqwest::Client;
use reqwest::StatusCode;

use lard_egress::{
    patchwork::PatchworkTables, products::ProductTables, PatchworkAvailableResp, PatchworkResp,
};

use util::DbPools;

pub mod common;
use common::{
    legacy::{e2e_test_wrapper_legacy, ingest_raw, IngestData},
    Param, TestData, RESTRICTED_TOKEN,
};

#[tokio::test]
async fn test_patchwork_available_endpoint() {
    // We insert a single timeseries so we will only get out a single label
    let n_labels = 1;

    e2e_test_wrapper_legacy(
        async |producer: FutureProducer,
               db_pools: DbPools,
               patchwork_tables: PatchworkTables,
               product_tables: ProductTables| {
            let data = IngestData::new(vec![TestData {
                station_id: 20001,
                params: vec![Param::new("TA")],
                start_time: Utc.with_ymd_and_hms(2024, 12, 15, 0, 0, 0).unwrap(),
                period: Duration::hours(1),
                type_id: 508,
                len: 2,
            }]);

            ingest_raw(&data, producer, db_pools, patchwork_tables, product_tables).await;

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
        "?stationid=10001&paramid=12345&level=0&sensor=0&from=2024-12-31T23:00:00Z&to=2025-01-01T01:30:00Z",
    ];

    e2e_test_wrapper_legacy(
        async |producer: FutureProducer,
               db_pools: DbPools,
               patchwork_tables: PatchworkTables,
               product_tables: ProductTables| {
            let data = IngestData::new(vec![TestData {
                station_id: 10001,
                params: vec![Param::new("TA")],
                start_time: Utc.with_ymd_and_hms(2024, 12, 31, 0, 0, 0).unwrap(),
                period: Duration::hours(1),
                type_id: 501,
                len: 48,
            }]);

            ingest_raw(&data, producer, db_pools, patchwork_tables, product_tables).await;

            for query in cases {
                let url = format!("http://localhost:3000/patchwork{query:?}");
                let resp = reqwest::get(url).await.unwrap();
                assert!(resp.status().is_client_error()); // expect 404
            }
        },
    )
    .await
}

#[tokio::test]
async fn test_patchwork_endpoint() {
    // Use values present in the mocks
    let cases = vec![
        (
            "?stationid=10001\
            &paramid=211\
            &level=200\
            &sensor=0\
            &from=2024-12-31T23:00:00Z\
            &to=2025-01-01T01:30:00Z",
            None,
            200,
            3,
        ),
        // omit level for grass param
        (
            "?stationid=20001\
            &paramid=225\
            &sensor=0\
            &from=2024-12-31T23:00:00Z\
            &to=2025-01-01T01:30:00Z",
            None,
            200,
            3,
        ),
        // 99995 has permitid 5 in mock_permit_tables(), so is restricted
        (
            "?stationid=99995\
            &paramid=211\
            &level=200\
            &sensor=0\
            &from=2024-12-31T23:00:00Z\
            &to=2025-01-01T01:30:00Z",
            None, // no token, no data access
            404,  // just don't see it...
            0,
        ),
        (
            "?stationid=99995\
            &paramid=211\
            &level=200\
            &sensor=0\
            &from=2024-12-31T23:00:00Z\
            &to=2025-01-01T01:30:00Z",
            Some(RESTRICTED_TOKEN),
            200,
            3,
        ),
    ];

    e2e_test_wrapper_legacy(
        async |producer: FutureProducer,
               db_pools: DbPools,
               patchwork_tables: PatchworkTables,
               product_tables: ProductTables| {
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
            ]);

            ingest_raw(
                &test_data,
                producer,
                db_pools,
                patchwork_tables,
                product_tables,
            )
            .await;

            for (query, token, status, n_data_found) in cases {
                let url = format!("http://localhost:3000/patchwork{query}");
                let client = Client::new();
                let request = match token {
                    Some(t) => client.get(url).bearer_auth(t),
                    None => client.get(url).basic_auth("test", Some("test")),
                };

                let resp = request.send().await.unwrap();
                assert!(resp.status() == status);

                if status == StatusCode::OK {
                    let json: Vec<PatchworkResp> = resp.json().await.unwrap();
                    for x in json {
                        assert_eq!(x.data.len(), n_data_found);
                    }
                }
            }
        },
    )
    .await
}
