use chrono::{Duration, DurationRound, SecondsFormat, TimeDelta, Utc};
use rdkafka::producer::FutureProducer;

use lard_egress::{
    calculations::{CalculationsAvailableResponse, CalculationsResponse},
    patchwork::PatchworkTables,
};

use util::DbPools;

pub mod common;
use common::{
    legacy::{e2e_test_wrapper_legacy, ingest_raw, IngestData},
    Param, TestData,
};

#[tokio::test]
async fn test_calculations_dew_point() {
    e2e_test_wrapper_legacy(
        &["TA", "UU"],
        async |producer: FutureProducer, db_pools: DbPools, patchwork_tables: PatchworkTables| {
            let now = Utc::now().duration_round(TimeDelta::hours(1)).unwrap();
            let eight_hours_ago =
                Utc::now().duration_round(TimeDelta::hours(1)).unwrap() - Duration::hours(8);

            let data = IngestData::new(vec![
                TestData {
                    station_id: 20001,
                    params: vec![Param::new("TA")],
                    start_time: eight_hours_ago,
                    period: Duration::hours(1),
                    type_id: 501,
                    len: 8,
                },
                TestData {
                    station_id: 20001,
                    params: vec![Param::new("UU")],
                    start_time: eight_hours_ago,
                    period: Duration::hours(1),
                    type_id: 501,
                    len: 8,
                },
            ]);

            ingest_raw(&data, producer, db_pools, patchwork_tables.clone()).await;

            // check available
            let url_available = "http://localhost:3000/calculations/available/217";

            let resp_available = reqwest::get(url_available).await.unwrap();
            assert!(resp_available.status().is_success());
            // check calculations available response can be deserialized
            let json_available = resp_available
                .json::<Vec<CalculationsAvailableResponse>>()
                .await
                .unwrap();
            assert!(
                !json_available.is_empty(),
                "Expected at least one calculation available result"
            );

            let station_id = "20001";
            let params = format!(
                "?level=200\
                &sensor=0\
                &from={}&to={}",
                eight_hours_ago.to_rfc3339_opts(SecondsFormat::Secs, true),
                now.to_rfc3339_opts(SecondsFormat::Secs, true)
            );

            // get the dew_point of station 20001
            let url =
                format!("http://localhost:3000/calculations/217/station/{station_id}{params}",);

            let resp = reqwest::get(url).await.unwrap();
            assert!(resp.status().is_success());

            let json = resp.json::<Vec<CalculationsResponse>>().await.unwrap();
            assert!(!json.is_empty(), "Expected at least one calculation result")
        },
    )
    .await
}
