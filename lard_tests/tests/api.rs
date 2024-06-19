use std::future::Future;

use chrono::{TimeZone, Utc};

pub mod common;

async fn api_test_wrapper<T: Future<Output = ()>>(test: T) {
    let server = common::init_api_server().await;

    tokio::select! {
        _ = server => panic!("Server task terminated first"),
        _ = test => {}
    }
}

// TODO: test getting all tseries without specifying param_id
#[tokio::test]
async fn test_stations_endpoint_irregular() {
    api_test_wrapper(async {
        let station_id = 10000;
        let param_id = 103;
        let expected_data_len = 20;

        let url = format!(
            "http://localhost:3000/stations/{}/params/{}",
            station_id, param_id
        );
        let resp = reqwest::get(url).await.unwrap();
        assert!(resp.status().is_success());

        let resp: common::StationsResponse = resp.json().await.unwrap();
        assert_eq!(resp.tseries.len(), 1);

        let ts = &resp.tseries[0];
        assert_eq!(ts.regularity, "Irregular");
        assert_eq!(ts.data.len(), expected_data_len);
    })
    .await
}

#[tokio::test]
async fn test_stations_endpoint_regular() {
    api_test_wrapper(async {
        let station_id = 30000;
        let param_id = 211;
        let resolution = "PT1H";
        let expected_data_len = 12;

        let url = format!(
            "http://localhost:3000/stations/{}/params/{}?time_resolution={}",
            station_id, param_id, resolution
        );
        let resp = reqwest::get(url).await.unwrap();
        assert!(resp.status().is_success());

        let resp: common::StationsResponse = resp.json().await.unwrap();
        assert_eq!(resp.tseries.len(), 1);

        let ts = &resp.tseries[0];
        assert_eq!(ts.regularity, "Regular");
        assert_eq!(ts.data.len(), expected_data_len);
    })
    .await
}

// TODO: use `test_case` here too?
#[tokio::test]
async fn test_latest_endpoint() {
    api_test_wrapper(async {
        let query = "";
        let url = format!("http://localhost:3000/latest{}", query);
        let expected_data_len = 2;

        let resp = reqwest::get(url).await.unwrap();
        assert!(resp.status().is_success());

        let json: common::LatestResponse = resp.json().await.unwrap();
        assert_eq!(json.data.len(), expected_data_len);
    })
    .await
}

#[tokio::test]
async fn test_latest_endpoint_with_query() {
    api_test_wrapper(async {
        let query = "?latest_max_age=2012-02-14T12:00:00Z";
        let url = format!("http://localhost:3000/latest{}", query);
        // TODO: Right now this test works only if it is run before ingestion/e2e integration tests
        let expected_data_len = 5;

        let resp = reqwest::get(url).await.unwrap();
        assert!(resp.status().is_success());

        let json: common::LatestResponse = resp.json().await.unwrap();
        assert_eq!(json.data.len(), expected_data_len);
    })
    .await
}

#[tokio::test]
async fn test_timeslice_endpoint() {
    api_test_wrapper(async {
        let time = Utc.with_ymd_and_hms(2023, 5, 5, 00, 30, 00).unwrap();
        let param_id = 3;
        let expected_data_len = 0;

        let url = format!(
            "http://localhost:3000/timeslices/{}/params/{}",
            time, param_id
        );

        let resp = reqwest::get(url).await.unwrap();
        assert!(resp.status().is_success());

        let json: common::TimesliceResponse = resp.json().await.unwrap();
        assert_eq!(json.tslices.len(), 1);

        let slice = &json.tslices[0];
        assert_eq!(slice.param_id, param_id);
        assert_eq!(slice.timestamp, time);
        assert_eq!(slice.data.len(), expected_data_len);
    })
    .await
}
