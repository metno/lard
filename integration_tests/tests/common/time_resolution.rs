use chrono::Duration;
use pg_interval::Interval;

use lard_ingestion::util::time_resolution::{
    find_time_resolution_of_timeseries_recent, last_obstime_ts, set_timeresolutions,
};
use util::DbPools;

#[derive(Debug)]
enum ExpectedOutput {
    Resolution(Interval),
    Unclear,
    RecentMismatch((Interval, Interval)),
}

// TODO: deal with offset and add to testing
pub async fn ensure_time_resolution(pools: DbPools) {
    let hourly = Interval::from_duration(Duration::hours(1)).unwrap();
    let ten_minutely = Interval::from_duration(Duration::minutes(10)).unwrap();

    let cases = vec![
        (
            "consistent hourly series resolves to one hour",
            0,
            ExpectedOutput::Resolution(hourly),
        ),
        (
            "mixed hourly and two-hourly series is unclear",
            1,
            ExpectedOutput::Unclear,
        ),
        (
            "series with change from hourly to 10-minute is rejected as a mismatch",
            2,
            ExpectedOutput::RecentMismatch((hourly, ten_minutely)),
        ),
    ];

    let open_conn = pools.open.get().await.unwrap();
    let (unclear_issues, mismatched_issues, _) = set_timeresolutions(&open_conn).await.unwrap();

    for (description, sensor, expected) in cases {
        // legacy should get a kvalobs label
        let ts = open_conn
            .query_one(
                "SELECT timeseries FROM labels.met WHERE station_id = 20004 AND sensor = $1",
                &[&sensor],
            )
            .await
            .unwrap()
            .get::<_, i64>("timeseries");

        // get the resolution (if it exists)
        let resolution = open_conn
            .query_one(
                "SELECT timeresolution FROM public.timeseries WHERE id = $1",
                &[&ts],
            )
            .await
            .unwrap()
            .get::<_, Option<Interval>>("timeresolution");

        match expected {
            ExpectedOutput::Resolution(expected_resolution) => {
                assert_eq!(
                    resolution,
                    Some(expected_resolution),
                    "Test case '{}' failed: expected resolution {:?}, got {:?}",
                    description,
                    expected_resolution,
                    resolution
                );
            }
            ExpectedOutput::Unclear => {
                assert!(
                    resolution.is_none(),
                    "Test case '{}' failed: expected unresolved timeseries, got {:?}",
                    description,
                    resolution
                );
                assert!(
                    unclear_issues.contains_key(&ts),
                    "Test case '{}' failed: expected unclear issue for ts {}, got {:?}",
                    description,
                    ts,
                    unclear_issues
                );
            }
            ExpectedOutput::RecentMismatch((older_res1, recent_res2)) => {
                assert!(
                    resolution.is_none(),
                    "Test case '{}' failed: expected unresolved timeseries, got {:?}",
                    description,
                    resolution
                );

                let mismatch_pair = mismatched_issues
                    .get(&ts)
                    .copied()
                    .expect("expected mismatch issue to be recorded for timeseries");
                assert_eq!(
                    mismatch_pair,
                    (older_res1, recent_res2),
                    "Test case '{}' failed: expected mismatch ({:?}, {:?}), got {:?}",
                    description,
                    older_res1,
                    recent_res2,
                    mismatch_pair
                );

                let last_obstime = last_obstime_ts(&open_conn, ts)
                    .await
                    .unwrap()
                    .expect("timeseries should have observations");

                let recent_results = find_time_resolution_of_timeseries_recent(
                    &open_conn,
                    ts,
                    &older_res1,
                    last_obstime,
                )
                .await
                .unwrap();
                let recent_resolution = recent_results
                    .first()
                    .map(|(resolution, _)| *resolution)
                    .expect("should find a resolution for the recent observations");

                assert_eq!(
                    recent_resolution, recent_res2,
                    "Test case '{}' failed: expected recent resolution {:?}, got {:?}",
                    description, recent_res2, recent_resolution
                );
            }
        }
    }

    eprintln!("time_resolution ok");
}
