use chrono::{Duration, DurationRound, SecondsFormat, TimeDelta, Utc};
use rdkafka::producer::FutureProducer;

use lard_egress::AggregationResp;
use lard_egress::aggregations::{AggregationPeriod, AggregationType};
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
        &["TA", "RR_1"],
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
            },
            TestData {
                station_id: 20001,
                params: vec![Param::new("RR_1")],
                start_time: two_days_ago,
                period: Duration::hours(1),
                type_id: 501,
                len: 48,
            },
            TestData {
                station_id: 20002,
                params: vec![Param::new("TA")],
                start_time: two_days_ago,
                period: Duration::hours(1),
                type_id: 501,
                len: 12,
            }
            ]);

            let cases = vec![
                (
                    "can get daily max air_temperature for a station with hourly data",
                    20001,
                    211,
                    format!(
                        "?agg_type={:?}&period={:?}&from={}", // to defaults to now
                        AggregationType::Max,
                        AggregationPeriod::Daily,
                        two_days_ago.duration_trunc(TimeDelta::days(1)).unwrap().to_rfc3339_opts(SecondsFormat::Secs, true),
                    ),
                    200,
                ),
                (
                    "can get daily sum precipitation for a station with hourly data, with an offset of 6 hours",
                    20001,
                    106,
                    format!(
                        "?agg_type={:?}&period={:?}&offset_hours={:?}&from={}", // to defaults to now
                        AggregationType::Sum,
                        AggregationPeriod::Daily,
                        Duration::hours(6).num_hours(),
                        two_days_ago.duration_trunc(TimeDelta::days(1)).unwrap().to_rfc3339_opts(SecondsFormat::Secs, true),
                    ),
                    200,
                ),
                (
                    "cannot get daily max air_temperature since not enough data",
                    20002,
                    211,
                    format!(
                        "?agg_type={:?}&period={:?}&from={}&to={}",
                        AggregationType::Max,
                        AggregationPeriod::Daily,
                        two_days_ago.duration_trunc(TimeDelta::days(1)).unwrap().to_rfc3339_opts(SecondsFormat::Secs, true),
                        (two_days_ago + Duration::hours(24)).duration_trunc(TimeDelta::days(1)).unwrap().to_rfc3339_opts(SecondsFormat::Secs, true),
                    ),
                    404,
                ),
                (
                    "can get daily max air_temperature despite not enough data when minimum-count filtering is disabled",
                    20002,
                    211,
                    format!(
                        "?agg_type={:?}&period={:?}&count_cutoff=false&from={}&to={}",
                        AggregationType::Max,
                        AggregationPeriod::Daily,
                        two_days_ago.duration_trunc(TimeDelta::days(1)).unwrap().to_rfc3339_opts(SecondsFormat::Secs, true),
                        (two_days_ago + Duration::hours(24)).duration_trunc(TimeDelta::days(1)).unwrap().to_rfc3339_opts(SecondsFormat::Secs, true),
                    ),
                    200,
                ),
            ];

            ingest_raw(&data, producer, db_pools, patchwork_tables.clone()).await;

            for (description, station_id, param_id, params, expected_status) in cases {
                let url = format!(
                    "http://localhost:3000/aggregations/station/{station_id}/param/{param_id}{params}",
                );

                let resp = reqwest::get(url).await.unwrap();
                let status = resp.status().as_u16();
                assert_eq!(
                    status, expected_status,
                    "Expected status {} but got {} for case: {}",
                    expected_status, status, description
                );

                if expected_status == 200 {
                    let body: AggregationResp = resp.json().await.unwrap();
                    assert!(
                        !body.aggregations.is_empty(),
                        "Expected at least one aggregation for case: {}",
                        description
                    );
                    assert!(
                        body.aggregations.iter().any(|agg| !agg.data.is_empty()),
                        "Expected at least one non-empty aggregation timeseries for case: {}",
                        description
                    );
                }
            }
        },
    )
    .await
}
