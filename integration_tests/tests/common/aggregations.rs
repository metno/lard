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
    //let t3: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 12, 31, 18, 0, 0).unwrap();

    let open_conn = pools.open.get().await.unwrap();

    // set the timeresolution for testing the cuttoff
    let timeresolution: pg_interval::Interval = pg_interval::Interval::new(0, 0, 3600 * 1_000_000);
    let station_ids = [20005, 20006];
    let parameter_id = 211; // only have air temp right now
    let type_id = 501; // not setting for the 508 one...
    for station in station_ids {
        let ts_id: i64 = open_conn
            .query_one(
                GET_TEST_TIMESERIES_ID_QUERY,
                &[&station, &type_id, &parameter_id],
            )
            .await
            .unwrap()
            .get(0);

        open_conn
            .execute(SET_TIMERESOLUTION_QUERY, &[&timeresolution, &ts_id])
            .await
            .unwrap();
    }

    let cases = vec![
        (
            "can get daily max air_temperature for a station with hourly data",
            20005,
            211,
            format!(
                "?agg_type={:?}&period={:?}&from={}&to={}", // to defaults to now if left out
                AggregationType::Max,
                AggregationPeriod::Daily,
                t1,
                t2,
            ),
            200,
        ),
        (
            "can get daily avg air_temperature for a station with hourly data, with an offset of 6 hours",
            20005,
            211,
            format!(
                "?agg_type={:?}&period={:?}&offset_hours={:?}&from={}&to={}",
                AggregationType::Avg,
                AggregationPeriod::Daily,
                Duration::hours(6).num_hours(),
                t1,
                t2,
            ),
            200,
        ),
        (
            "cannot get daily max air_temperature since not enough data",
            20006,
            211,
            format!(
                "?agg_type={:?}&period={:?}&from={}&to={}",
                AggregationType::Max,
                AggregationPeriod::Daily,
                t1,
                t2,
            ),
            404,
        ),
        (
            "can get daily max air_temperature despite not enough data when minimum-count filtering is disabled",
            20006,
            211,
            format!(
                "?agg_type={:?}&period={:?}&count_cutoff=false&from={}&to={}",
                AggregationType::Max,
                AggregationPeriod::Daily,
                t1,
                t2,
            ),
            200,
        ),
        (
            "in order to get this aggregation, we need to shift the overlapp between two timeseries from patchwork",
            20005,
            211,
            format!(
                "?agg_type={:?}&period={:?}&offset_hours={:?}&from={}",
                AggregationType::Min,
                AggregationPeriod::TwiceDaily,
                Duration::hours(18).num_hours(), // asking for something over midnight where there is a switch in the patchwork priorities
                Utc.with_ymd_and_hms(2024, 12, 31, 0, 0, 0)
                    .unwrap()
                    .to_rfc3339_opts(SecondsFormat::Secs, true),
            ),
            200,
        ),
    ];

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
    eprintln!("aggregations ok");
}
