use std::future::Future;

use test_case::test_case;

pub mod common;

async fn e2e_test_wrapper<T: Future<Output = ()>>(test: T) {
    let api_server = common::init_api_server().await;
    let ingestor = common::init_ingestion_server().await;

    tokio::select! {
        _ = api_server => panic!("API server task terminated first"),
        _ = ingestor => panic!("Ingestor server task terminated first"),
        _ = test => {}
    }
}

#[test_case(20000, 0; "to closed timeseries")]
#[test_case(12000, 6; "to open timeseries")]
#[tokio::test]
async fn append_data(station_id: i32, expected_rows: usize) {
    e2e_test_wrapper(async move {
        let param_id = 211;
        let obsinn_msg = format!(
            "kldata/nationalnr={}/type=508/messageid=23
TA
20230505014000,20.0
20230505014100,20.1
20230505014200,20.0
20230505014300,20.2
20230505014400,20.2
20230505014500,20.1
",
            station_id
        );

        let api_url = format!(
            "http://localhost:3000/stations/{}/params/{}",
            station_id, param_id
        );

        let client = reqwest::Client::new();
        let resp = client.get(&api_url).send().await.unwrap();
        let json: common::StationsResponse = resp.json().await.unwrap();
        let count_before_ingestion = json.tseries[0].data.len();

        let resp = client
            .post("http://localhost:3001/kldata")
            .body(obsinn_msg)
            .send()
            .await
            .unwrap();

        let json: common::IngestorResponse = resp.json().await.unwrap();
        assert_eq!(json.res, 0);

        let resp = client.get(&api_url).send().await.unwrap();
        let json: common::StationsResponse = resp.json().await.unwrap();
        let count_after_ingestion = json.tseries[0].data.len();

        let rows_added = count_after_ingestion - count_before_ingestion;

        // NOTE: The assert might fail locally if the database hasn't been cleaned up between runs
        assert_eq!(rows_added, expected_rows);
    })
    .await
}

#[test_case(40000; "new_station")]
#[test_case(11000; "old_station")]
#[tokio::test]
async fn create_timeseries(station_id: i32) {
    e2e_test_wrapper(async move {
        // 145, AGM, mean(air_gap PT10M)
        // 146, AGX, max(air_gap PT10M)
        let param_ids = [145, 146];
        let expected_len = 5;
        let obsinn_msg = format!(
            // 506 == ten minute data
            "kldata/nationalnr={}/type=506/messageid=23
AGM(1,2),AGX(1,2)
20240606000000,11.0,12.0
20240606001000,12.0,19.0
20240606002000,10.0,16.0
20240606003000,12.0,16.0
20240606004000,11.0,15.0
",
            station_id
        );

        let client = reqwest::Client::new();
        let resp = client
            .post("http://localhost:3001/kldata")
            .body(obsinn_msg)
            .send()
            .await
            .unwrap();
        let json: common::IngestorResponse = resp.json().await.unwrap();
        assert_eq!(json.res, 0);

        for id in param_ids {
            let url = format!(
                "http://localhost:3000/stations/{}/params/{}",
                station_id, id
            );

            let resp = client.get(&url).send().await.unwrap();
            assert!(resp.status().is_success());

            let json: common::StationsResponse = resp.json().await.unwrap();
            assert_eq!(json.tseries[0].data.len(), expected_len)
        }
    })
    .await
}
