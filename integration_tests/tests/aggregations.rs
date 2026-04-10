use chrono::{Duration, DurationRound, SecondsFormat, TimeDelta, Utc};
use rdkafka::producer::FutureProducer;

use lard_egress::aggregations::{AggregationPeriod, AggregationResp, AggregationType};
use lard_egress::patchwork::PatchworkTables;

use util::DbPools;

pub mod common;
use common::{
    Param, TestData,
    legacy::{IngestData, e2e_test_wrapper_legacy, ingest_raw},
};

#[tokio::test]
async fn test_aggregations() {
    e2e_test_wrapper_legacy(
        &["TA"],
        async |producer: FutureProducer, db_pools: DbPools, patchwork_tables: PatchworkTables| {
            let two_days_ago =
                Utc::now().duration_round(TimeDelta::hours(1)).unwrap() - Duration::hours(48);

            let data = IngestData::new(vec![TestData {
                station_id: 20001,
                params: vec![Param::new("TA")],
                start_time: two_days_ago,
                period: Duration::hours(1),
                type_id: 501,
                len: 48,
            }]);

            ingest_raw(&data, producer, db_pools, patchwork_tables.clone()).await;

            let station_id = "20001";
            let params = format!(
                "?agg_type={:?}&period={:?}&from={}", // to defaults to now
                AggregationType::Max,
                AggregationPeriod::Daily,
                two_days_ago.to_rfc3339_opts(SecondsFormat::Secs, true),
            );

            // get daily max air_temperature
            let url = format!(
                "http://localhost:3000/aggregations/station/{station_id}/param/211{params}",
            );

            let resp = reqwest::get(url).await.unwrap();
            assert!(resp.status().is_success());

            let json: Vec<AggregationResp> = resp.json().await.unwrap();
            print!("{:#?}", json);
            assert!(!json.is_empty(), "Expected at least one aggregation result")
        },
    )
    .await
}
