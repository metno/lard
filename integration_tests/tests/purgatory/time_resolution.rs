use futures::FutureExt;
use lard_ingestion::util::time_resolution::{
    find_time_resolution_of_timeseries_recent, last_obstime_ts, set_timeresolutions,
};
use pg_interval::Interval;

#[ignore]
#[tokio::test]
async fn test_find_time_resolution() {
    e2e_test_wrapper_legacy(
        &["TA", "RR_1"],
        async |producer: FutureProducer, db_pools: DbPools, tables: PatchworkTables| {
            #[derive(Debug)]
            enum ExpectedOutput {
                Resolution(Interval),
                Unclear,
                RecentMismatch((Interval, Interval)),
            }

            // TODO: deal with offset and add to testing
            let start_time = Utc.with_ymd_and_hms(2024, 6, 6, 6, 0, 0).unwrap();
            let hourly = Interval::from_duration(Duration::hours(1)).unwrap();
            let daily = Interval::from_duration(Duration::days(1)).unwrap();

            let test_data = IngestData::new(vec![
                TestData {
                    station_id: 20001,
                    params: vec![Param::new("RR_1").with_sensor_level((0, 200))],
                    start_time,
                    period: Duration::hours(1),
                    type_id: 501,
                    len: 201,
                },
                TestData {
                    station_id: 20002,
                    params: vec![Param::new("TA").with_sensor_level((0, 200))],
                    start_time,
                    period: Duration::hours(2),
                    type_id: 501,
                    len: 50,
                },
                TestData {
                    station_id: 20002,
                    params: vec![Param::new("TA").with_sensor_level((0, 200))],
                    start_time: start_time + Duration::hours(100),
                    period: Duration::hours(1),
                    type_id: 501,
                    len: 50,
                },
                TestData {
                    station_id: 20002,
                    params: vec![Param::new("TA").with_sensor_level((0, 200))],
                    start_time: start_time + Duration::hours(150),
                    period: Duration::hours(2),
                    type_id: 501,
                    len: 50,
                },
                TestData {
                    station_id: 20002,
                    params: vec![Param::new("TA").with_sensor_level((0, 200))],
                    start_time: start_time + Duration::hours(250),
                    period: Duration::hours(1),
                    type_id: 501,
                    len: 60,
                },
                TestData {
                    station_id: 20001,
                    params: vec![Param::new("TA").with_sensor_level((0, 200))],
                    start_time,
                    period: Duration::hours(1),
                    type_id: 501,
                    len: 201,
                },
                TestData {
                    station_id: 20001,
                    params: vec![Param::new("TA").with_sensor_level((0, 200))],
                    start_time: start_time + Duration::hours(201),
                    period: Duration::days(1),
                    type_id: 501,
                    len: 60,
                },
            ]);

            let cases = vec![
                (
                    "consistent hourly series resolves to one hour",
                    20001,
                    106,
                    ExpectedOutput::Resolution(hourly),
                ),
                (
                    "mixed hourly and two-hourly series is unclear",
                    20002,
                    211,
                    ExpectedOutput::Unclear,
                ),
                (
                    "series with change from hourly to daily is rejected as a mismatch",
                    20001,
                    211,
                    ExpectedOutput::RecentMismatch((hourly, daily)),
                ),
            ];

            ingest_raw(&test_data, producer, db_pools.clone(), tables).await;

            let open_conn = db_pools.open.get().await.unwrap();
            let resolution_result = set_timeresolutions(&open_conn).await.unwrap();
            let (unclear_issues, mismatched_issues, count) = resolution_result;
            assert!(unclear_issues.len() == 1, "Expected 1 unclear issue, but found: {:?}", unclear_issues);
            assert!(mismatched_issues.len() == 1, "Expected 1 mismatched issue, but found: {:?}", mismatched_issues);
            assert_eq!(count, 1, "Expected 1 timeseries to be processed, but found: {}", count);

            for (description, station_id, param_id, expected) in cases {
                // legacy should get a kvalobs label 
                let ts = open_conn
                    .query_one(
                        "SELECT timeseries FROM labels.kvalobs WHERE station_id = $1 AND param_id = $2",
                        &[&station_id, &param_id],
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

                        let mismatch_pair = mismatched_issues.get(&ts).copied().expect(
                            "expected mismatch issue to be recorded for timeseries",
                        );
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
                            recent_resolution,
                            recent_res2,
                            "Test case '{}' failed: expected recent resolution {:?}, got {:?}",
                            description,
                            recent_res2,
                            recent_resolution
                        );

                    }
                }
            }
        },
    ).await
}
