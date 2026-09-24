use chrono::{Duration, DurationRound, SecondsFormat, TimeDelta, Utc};
use rdkafka::producer::FutureProducer;

use lard_egress::AggregationResp;
use lard_egress::aggregations::{AggregationPeriod, AggregationType};
use lard_egress::patchwork::PatchworkTables;

use util::{DbPools, stinfofacade::permissions::timeseries_get_permit};

pub mod common;
use common::{
    Param, TestData,
    legacy::{IngestData, e2e_test_wrapper_legacy, ingest_raw},
    mocks,
};

const SET_TIMERESOLUTION_QUERY: &str = r#"
    UPDATE timeseries
    SET timeresolution = $1, timeresolution_assessed = TRUE
    WHERE id = $2"#;
const GET_TEST_TIMESERIES_ID_QUERY: &str = r#"
    SELECT t.id
    FROM public.timeseries t
    JOIN labels.met l ON l.timeseries = t.id
    WHERE l.station_id = $1
      AND l.type_id = $2
      AND l.param_id = $3
    LIMIT 1"#;

#[tokio::test]
async fn test_aggregations() {
    e2e_test_wrapper_legacy(
        &["TA", "RR_1"],
        async |producer: FutureProducer, db_pools: DbPools, patchwork_tables: PatchworkTables| {
            let two_days_ago =
                Utc::now().duration_round(TimeDelta::hours(1)).unwrap() - Duration::hours(48);
            let one_day_ago =
                Utc::now().duration_round(TimeDelta::hours(1)).unwrap() - Duration::hours(24);

            let test_series = vec![TestData {
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
            },
            TestData {
                station_id: 20002,
                params: vec![Param::new("RR_1")],
                start_time: two_days_ago,
                period: Duration::hours(1),
                type_id: 501,
                len: 24,
            },
            // shift the start time of the last two series by 10 minutes to test the deviation calculation
            TestData {
                station_id: 20002,
                params: vec![Param::new("RR_1")],
                start_time: one_day_ago - Duration::minutes(10),
                period: Duration::hours(1),
                type_id: 501,
                len: 24,
            }
            ];

            let mut timeresolution_targets: Vec<(i32, i32, i32, pg_interval::Interval)> =
                test_series
                    .iter()
                    .map(|ts| {
                        let param = ts.params.first().unwrap();
                        (
                            ts.station_id,
                            ts.type_id,
                            param.id,
                            pg_interval::Interval::new(0, 0, ts.period.num_seconds() * 1_000_000),
                        )
                    })
                    .collect();
            timeresolution_targets.dedup(); // this will remove the last entry since it has the same station_id, type_id, and param_id as the previous one 

            let data = IngestData::new(test_series);

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
                (
                    "later part of the timeseries is shifted by 10 minutes, so the aggregation should be rejected due to deviation",
                    20002,
                    106,
                    format!(
                        "?agg_type={:?}&period={:?}&from={}",
                        AggregationType::Sum,
                        AggregationPeriod::Daily,
                        two_days_ago.duration_trunc(TimeDelta::days(1)).unwrap().to_rfc3339_opts(SecondsFormat::Secs, true),
                    ),
                    404,
                ),
            ];

            ingest_raw(&data, producer, db_pools.clone(), patchwork_tables.clone()).await;

            let open_conn = db_pools.open.get().await.unwrap();
            let restricted_conn = db_pools.restricted.get().await.unwrap();

            for (station_id, type_id, param_id, timeresolution) in
                timeresolution_targets
            {
                let permit = timeseries_get_permit(
                    mocks::mock_permit_tables(),
                    station_id,
                    type_id,
                    Some(param_id),
                )
                .unwrap();
                let conn = if permit == Some(1) {
                    &open_conn
                } else {
                    &restricted_conn
                };

                let ts_id: i64 = conn
                    .query_one(
                        GET_TEST_TIMESERIES_ID_QUERY,
                        &[&station_id, &type_id, &param_id],
                    )
                    .await
                    .unwrap()
                    .get(0);

                conn.execute(SET_TIMERESOLUTION_QUERY, &[&timeresolution, &ts_id])
                    .await
                    .unwrap();
            }

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
