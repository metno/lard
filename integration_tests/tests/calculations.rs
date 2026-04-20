use chrono::DateTime;
use chrono::{Duration, DurationRound, SecondsFormat, TimeDelta, Utc};
use rdkafka::producer::FutureProducer;

use lard_egress::patchwork::PatchworkTables;

use util::DbPools;

pub mod common;
use common::{
    Param, TestData,
    legacy::{IngestData, e2e_test_wrapper_legacy, ingest_raw},
};

#[tokio::test]
async fn test_calculations_specific_humidity() {
    e2e_test_wrapper_legacy(
        &["TA", "UU", "PA"],
        async |producer: FutureProducer, db_pools: DbPools, patchwork_tables: PatchworkTables| {
            let now = Utc::now().duration_round(TimeDelta::hours(1)).unwrap();
            let two_days_ago =
                Utc::now().duration_round(TimeDelta::hours(1)).unwrap() - Duration::hours(48);

            // test a calculation that needs 3 parameters
            let data = IngestData::new(vec![
                TestData {
                    station_id: 20001,
                    params: vec![Param::new("TA")],
                    start_time: two_days_ago,
                    period: Duration::hours(1),
                    type_id: 501,
                    len: 48,
                },
                TestData {
                    station_id: 20001,
                    params: vec![Param::new("UU")],
                    start_time: two_days_ago,
                    period: Duration::hours(1),
                    type_id: 501,
                    len: 48,
                },
                TestData {
                    station_id: 20001,
                    params: vec![Param::new("PA")],
                    start_time: two_days_ago,
                    period: Duration::hours(1),
                    type_id: 501,
                    len: 48,
                },
            ]);

            ingest_raw(&data, producer, db_pools, patchwork_tables.clone()).await;

            let station_id = "20001";
            let params = format!(
                "?level=200\
                &sensor=0\
                &from={}&to={}",
                two_days_ago.to_rfc3339_opts(SecondsFormat::Secs, true),
                now.to_rfc3339_opts(SecondsFormat::Secs, true)
            );

            // get the specific_humidity of station 20001
            let url =
                format!("http://localhost:3000/calculations/station/{station_id}/3123{params}",);

            let resp = reqwest::get(url).await.unwrap();
            assert!(resp.status().is_success());

            let json: Vec<(DateTime<Utc>, f64, Option<i32>)> = resp.json().await.unwrap();
            assert!(!json.is_empty(), "Expected at least one calculation result")
        },
    )
    .await
}
