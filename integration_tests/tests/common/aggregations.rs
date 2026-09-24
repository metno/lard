use chrono::{Duration, SecondsFormat, TimeZone, Utc};
use lard_egress::aggregations::{AggregationPeriod, AggregationType};
use lard_egress::http::aggregations::Resp;

use util::DbPools;

pub async fn ensure_aggregations(pools: DbPools) {
    let t1: String = Utc
        .with_ymd_and_hms(2025, 1, 1, 0, 0, 0)
        .unwrap()
        .to_rfc3339_opts(SecondsFormat::Secs, true);
    let t2: String = Utc
        .with_ymd_and_hms(2025, 1, 3, 0, 0, 0)
        .unwrap()
        .to_rfc3339_opts(SecondsFormat::Secs, true);

    let _open_conn = pools.open.get().await.unwrap();

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
            vec![Some(2.0), Some(2.0)],
        ),
        (
            "cannot get daily max air_temperature since outside availability",
            20005,
            211,
            format!(
                "?agg_type={:?}&period={:?}&from={}&to={}",
                AggregationType::Max,
                AggregationPeriod::Daily,
                Utc.with_ymd_and_hms(2025, 1, 2, 0, 0, 0)
                    .unwrap()
                    .to_rfc3339_opts(SecondsFormat::Secs, true),
                Utc.with_ymd_and_hms(2025, 1, 2, 0, 30, 0)
                    .unwrap()
                    .to_rfc3339_opts(SecondsFormat::Secs, true),
            ),
            404,
            vec![None],
        ),
        (
            "empty aggregations for daily avg air_temperature since not enough data in that period",
            20006,
            211,
            format!(
                "?agg_type={:?}&period={:?}&from={}&to={}",
                AggregationType::Avg,
                AggregationPeriod::Daily,
                t1,
                t2,
            ),
            200,
            vec![None, None],
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
            vec![Some(1.0), Some(1.0)],
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
            "get sum of rainfall for a timeseries with no timeresolution",
            20007,
            106,
            format!(
                "?agg_type={:?}&period={:?}&from={}&to={}",
                AggregationType::Sum,
                AggregationPeriod::Daily,
                t1,
                t2,
            ),
            200,
            vec![Some(144.0), Some(144.0)],
        ),
        (
            "cannot daily sum of rainfall for data that is already P1D",
            20007,
            106,
            format!(
                "?agg_type={:?}&period={:?}&from={}&to={}",
                AggregationType::Sum,
                AggregationPeriod::Daily,
                Utc.with_ymd_and_hms(2024, 12, 29, 0, 0, 0)
                    .unwrap()
                    .to_rfc3339_opts(SecondsFormat::Secs, true),
                t1,
            ),
            404,
            vec![None],
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
            vec![Some(3.0), None, Some(2.0), Some(2.0)],
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
            let body: Resp = resp.json().await.unwrap();
            //println!("body: {:?}", body);
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
