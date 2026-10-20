use chrono::{Duration, SecondsFormat, TimeZone, Utc};

use lard_egress::AggregationResp;
use lard_egress::aggregations::{AggregationPeriod, AggregationType};

use util::DbPools;

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

pub async fn ensure_aggregations(pools: DbPools) {
    let t1: String = Utc
        .with_ymd_and_hms(2025, 1, 1, 0, 0, 0)
        .unwrap()
        .to_rfc3339_opts(SecondsFormat::Secs, true);
    let t2: String = Utc
        .with_ymd_and_hms(2025, 1, 3, 0, 0, 0)
        .unwrap()
        .to_rfc3339_opts(SecondsFormat::Secs, true);

    let open_conn = pools.open.get().await.unwrap();

    // set the timeresolution for testing the cuttoff
    let timeresolution = pg_interval::Interval::from_iso("PT1H").unwrap();
    let station_ids = [20005, 20006, 20007];
    let type_ids = [501, 508];
    let parameter_id = 211; // only set air temp right now (aren't setting timeresolution for 106 on purpose)
    for station in station_ids {
        for type_id in type_ids {
            let row = open_conn
                .query_opt(
                    GET_TEST_TIMESERIES_ID_QUERY,
                    &[&station, &type_id, &parameter_id],
                )
                .await
                .unwrap();

            if let Some(r) = row {
                let id: i64 = r.get(0);
                open_conn
                    .execute(SET_TIMERESOLUTION_QUERY, &[&timeresolution, &id])
                    .await
                    .unwrap();
            }
        }
    }
    // set one of the 106 sections to daily (4002), leave the other without a time resolution (506)
    let ts_id: i64 = open_conn
        .query_one(GET_TEST_TIMESERIES_ID_QUERY, &[&20007, &4002, &106])
        .await
        .unwrap()
        .get(0);

    open_conn
        .execute(
            SET_TIMERESOLUTION_QUERY,
            &[&pg_interval::Interval::from_iso("P1D").unwrap(), &ts_id],
        )
        .await
        .unwrap();

    let cases = vec![
        (
            "can get daily max air_temperature for a station with hourly data",
            20005,
            211,
            format!(
                "?agg_type={:?}&period={:?}&from={}&to={}",
                AggregationType::Max,
                AggregationPeriod::Daily,
                t1,
                t2,
            ),
            200,
            vec![Some(2.0), Some(2.0)],
        ),
        (
            "can get daily max air_temperature for a station with hourly data, with an offset of 6 hours",
            20005,
            211,
            format!(
                "?agg_type={:?}&period={:?}&offset_hours={:?}&from={}&to={}",
                AggregationType::Max,
                AggregationPeriod::Daily,
                Duration::hours(6).num_hours(),
                t1,
                t2,
            ),
            200,
            vec![Some(2.0), None],
        ),
        (
            "cannot get daily max air_temperature since outside availability",
            20005,
            211,
            format!(
                "?agg_type={:?}&period={:?}&from={}", // to defaults to now if left out
                AggregationType::Max,
                AggregationPeriod::Daily,
                Utc.with_ymd_and_hms(2026, 10, 31, 0, 0, 0)
                    .unwrap()
                    .to_rfc3339_opts(SecondsFormat::Secs, true),
            ),
            404,
            vec![None],
        ),
        (
            "cannot get daily avg air_temperature since not enough data in that period",
            20006,
            211,
            format!(
                "?agg_type={:?}&period={:?}&from={}&to={}",
                AggregationType::Avg,
                AggregationPeriod::Daily,
                t1,
                t2,
            ),
            200, // exists but only nulls
            vec![None],
        ),
        (
            "can get daily avg air_temperature despite not enough data when minimum-count filtering is disabled",
            20006,
            211,
            format!(
                "?agg_type={:?}&period={:?}&count_cutoff=false&from={}&to={}",
                AggregationType::Avg,
                AggregationPeriod::Daily,
                t1,
                t2,
            ),
            200,
            vec![Some(1.0)],
        ),
        (
            "cannot get aggregation since some data is 10 min late",
            20007,
            211,
            format!(
                "?agg_type={:?}&period={:?}&from={}&to={}",
                AggregationType::Max,
                AggregationPeriod::Daily,
                t1,
                t2,
            ),
            200, // exist but only nulls
            vec![None],
        ),
        (
            "can get aggregation with late data if disable the regularity check",
            20007,
            211,
            format!(
                "?agg_type={:?}&period={:?}&check_regular=false&from={}&to={}",
                AggregationType::Max,
                AggregationPeriod::Daily,
                t1,
                t2,
            ),
            200,
            vec![Some(1.0)],
        ),
        (
            // NOTE: we do not set the timeresolution of this timeseries at the top of the tests
            // but its is PT10M
            "get sum of rainfall for a timeseries with no timeresolution",
            20007,
            106,
            format!(
                "?agg_type={:?}&period={:?}&check_regular=false&from={}&to={}",
                AggregationType::Sum,
                AggregationPeriod::Daily,
                t1,
                t2,
            ),
            200,
            vec![Some(144.0), Some(144.0)],
        ),
        (
            "get daily sum of rainfall for data that is already P1D",
            20007,
            106,
            format!(
                "?agg_type={:?}&period={:?}&check_regular=false&from={}&to={}",
                AggregationType::Sum,
                AggregationPeriod::Daily,
                Utc.with_ymd_and_hms(2024, 12, 29, 0, 0, 0)
                    .unwrap()
                    .to_rfc3339_opts(SecondsFormat::Secs, true),
                t1,
            ),
            200,
            vec![Some(1.0), Some(1.0), None],
        ),
        (
            "in order to get some of this aggregation, the overlapp between two timeseries from patchwork must be shifted",
            20005,
            211,
            format!(
                "?agg_type={:?}&period={:?}&offset_hours={:?}&from={}&to={}",
                AggregationType::Min,
                AggregationPeriod::Daily,
                Duration::hours(18).num_hours(), // asking for something over midnight where there is a switch in the patchwork priorities
                Utc.with_ymd_and_hms(2024, 12, 30, 0, 0, 0)
                    .unwrap()
                    .to_rfc3339_opts(SecondsFormat::Secs, true),
                t2,
            ),
            200,
            // first timeseries, lacking data, second timeseries
            vec![Some(3.0), None, Some(2.0)],
        ),
    ];

    for (description, station_id, param_id, params, expected_status, expected_values) in cases {
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
                !body.data.is_empty(),
                "Expected at least one aggregation for case: {}",
                description
            );
            assert!(
                expected_values.len() == body.data.len(),
                "Expected length of data {} but got {}, for case: {}",
                expected_values.len(),
                body.data.len(),
                description
            );
            // zip the two vecs
            for (datum, expect) in body.data.iter().zip(expected_values.iter()) {
                assert!(
                    *expect == datum.value,
                    "Expected value {:?} but got {:?} timeseries for case: {}",
                    expect,
                    datum.value,
                    description
                );
            }
        }
    }
    eprintln!("aggregations ok");
}
