use chrono::{DateTime, Duration, TimeZone, Utc};
use pg_interval::Interval;
use rdkafka::producer::{FutureProducer, FutureRecord};

use lard_egress::patchwork::PatchworkTables;
use lard_ingestion::util::time_resolution::{
    find_time_resolution_of_timeseries_recent, last_obstime_ts, set_timeresolutions,
};

use util::DbPools;

pub mod common;
use common::{
    Param, TestData,
    legacy::{
        IngestData, KAFKA_CHECKED_TOPIC, e2e_test_wrapper_legacy, ingest_raw, wait_for_db_readiness,
    },
};

#[tokio::test]
async fn test_kafka_checked() {
    e2e_test_wrapper_legacy(
        &["RR_1"],
        async |producer: FutureProducer, db_pools: DbPools, _| {
            // This observation was 2.5 hours late??
            let kafka_xml = r#"<?xml?>
            <KvalobsData producer=\"kvqabase\" created=\"2024-06-06 08:30:43\">
                <station val=\"20001\">
                    <typeid val=\"-4\">
                        <obstime val=\"2024-06-06 06:00:00\">
                            <tbtime val=\"2024-06-06 08:30:42.943247\">
                                <sensor val=\"0\">
                                    <level val=\"0\">
                                        <kvdata paramid=\"106\">
                                            <original>10</original>
                                            <corrected>10</corrected>
                                            <controlinfo>1000000000000000</controlinfo>
                                            <useinfo>9000000000000000</useinfo>
                                            <cfailed></cfailed>
                                        </kvdata>
                                    </level>
                                </sensor>
                            </tbtime>
                        </obstime>
                    </typeid>
                </station>
            </KvalobsData>"#;

            producer
                .send_result(
                    FutureRecord::to(KAFKA_CHECKED_TOPIC)
                        .key("")
                        .payload(kafka_xml),
                )
                .unwrap()
                .await
                .unwrap()
                .unwrap();

            // As we have no way to sync with message processing in kvkafka ingestion, we just keep
            // trying to fetch data with a timeout
            let expected_rows = 1;
            let open_conn = db_pools.open.get().await.unwrap();
            wait_for_db_readiness(&open_conn, expected_rows).await;

            // TODO: we do not have an API endpoint to query the flags.kvdata table
            let data_row = open_conn
                .query_one(
                    "SELECT timeseries, obstime, original, corrected, \
                        quality_code, controlinfo, useinfo, cfailed \
                    FROM legacy.data",
                    &[],
                )
                .await
                .unwrap();

            #[allow(clippy::type_complexity)]
            let (
                timeseries,
                obstime,
                original,
                corrected,
                quality_code,
                controlinfo,
                useinfo,
                cfailed,
            ): (
                i64,
                DateTime<Utc>,
                Option<f64>,
                Option<f64>,
                Option<i32>,
                Option<String>,
                Option<String>,
                Option<String>,
            ) = (
                data_row.get(0),
                data_row.get(1),
                data_row.get(2),
                data_row.get(3),
                data_row.get(4),
                data_row.get(5),
                data_row.get(6),
                data_row.get(7),
            );
            assert_eq!(obstime, Utc.with_ymd_and_hms(2024, 6, 6, 6, 0, 0).unwrap());
            assert_eq!(original, Some(10.));
            assert_eq!(corrected, Some(10.));
            assert_eq!(
                quality_code,
                lard_ingestion::util::quality_code::get_quality_code(
                    useinfo.clone().unwrap().as_str()
                )
            );
            assert_eq!(controlinfo, Some("1000000000000000".to_string()));
            assert_eq!(useinfo, Some("9000000000000000".to_string()));
            assert_eq!(cfailed, None);

            let label_row = open_conn
                .query_one(
                    "SELECT  station_id, param_id, type_id, lvl, sensor \
                    FROM labels.kvalobs \
                    WHERE timeseries = $1",
                    &[&timeseries],
                )
                .await
                .unwrap();

            #[allow(clippy::type_complexity)]
            let (station_id, param_id, type_id, lvl, sensor): (
                // should these really all be Option??
                Option<i32>,
                Option<i32>,
                Option<i32>,
                Option<i32>,
                Option<i32>,
            ) = (
                label_row.get(0),
                label_row.get(1),
                label_row.get(2),
                label_row.get(3),
                label_row.get(4),
            );

            assert_eq!(station_id, Some(20001));
            assert_eq!(param_id, Some(106));
            assert_eq!(type_id, Some(-4));
            assert_eq!(lvl, Some(0));
            assert_eq!(sensor, Some(0));
        },
    )
    .await
}

#[tokio::test]
async fn test_kafka_checked_special_values() {
    e2e_test_wrapper_legacy(
        &["RR_1"],
        async |producer: FutureProducer, db_pools: DbPools, _| {
            // This observation was 2.5 hours late??
            let kafka_xml = r#"<?xml?>
            <KvalobsData producer=\"kvqabase\" created=\"2024-06-06 08:30:43\">
                <station val=\"20001\">
                    <typeid val=\"-4\">
                        <obstime val=\"2024-06-06 06:00:00\">
                            <tbtime val=\"2024-06-06 08:30:42.943247\">
                                <sensor val=\"0\">
                                    <level val=\"0\">
                                        <kvdata paramid=\"106\">
                                            <original>-32767</original>
                                            <corrected>-32766</corrected>
                                            <controlinfo>0000000000000000</controlinfo>
                                            <useinfo>0000000000000000</useinfo>
                                            <cfailed></cfailed>
                                        </kvdata>
                                    </level>
                                </sensor>
                            </tbtime>
                        </obstime>
                    </typeid>
                </station>
            </KvalobsData>"#;

            producer
                .send_result(
                    FutureRecord::to(KAFKA_CHECKED_TOPIC)
                        .key("")
                        .payload(kafka_xml),
                )
                .unwrap()
                .await
                .unwrap()
                .unwrap();

            // As we have no way to sync with message processing in kvkafka ingestion, we just keep
            // trying to fetch data with a timeout
            let expected_rows = 1;
            let open_conn = db_pools.open.get().await.unwrap();
            wait_for_db_readiness(&open_conn, expected_rows).await;

            // TODO: we do not have an API endpoint to query the flags.kvdata table
            let data_row = open_conn
                .query_one(
                    "SELECT timeseries, obstime, original, corrected, \
                        quality_code, controlinfo, useinfo, cfailed \
                    FROM legacy.data",
                    &[],
                )
                .await
                .unwrap();

            #[allow(clippy::type_complexity)]
            let (
                _timeseries,
                obstime,
                original,
                corrected,
                quality_code,
                controlinfo,
                useinfo,
                cfailed,
            ): (
                i64,
                DateTime<Utc>,
                Option<f64>,
                Option<f64>,
                Option<i32>,
                Option<String>,
                Option<String>,
                Option<String>,
            ) = (
                data_row.get(0),
                data_row.get(1),
                data_row.get(2),
                data_row.get(3),
                data_row.get(4),
                data_row.get(5),
                data_row.get(6),
                data_row.get(7),
            );
            assert_eq!(obstime, Utc.with_ymd_and_hms(2024, 6, 6, 6, 0, 0).unwrap());
            assert_eq!(original, None); // -32767 should be converted to a Null
            assert_eq!(corrected, None);
            assert_eq!(
                quality_code,
                lard_ingestion::util::quality_code::get_quality_code(
                    useinfo.clone().unwrap().as_str()
                )
            );
            assert_eq!(controlinfo, Some("0000000000000000".to_string()));
            assert_eq!(useinfo, Some("0000000000000000".to_string()));
            assert_eq!(cfailed, None);
        },
    )
    .await
}

#[tokio::test]
async fn test_kafka_raw() {
    e2e_test_wrapper_legacy(
        &["TA"],
        async |producer: FutureProducer, db_pools: DbPools, tables: PatchworkTables| {
            let test_data = IngestData::new(vec![TestData {
                station_id: 20001,
                params: vec![Param::new("TA").with_sensor_level((0, 200))],
                start_time: Utc.with_ymd_and_hms(2024, 6, 6, 6, 0, 0).unwrap(),
                period: Duration::hours(1),
                type_id: 501,
                len: 1,
            }]);

            ingest_raw(&test_data, producer, db_pools.clone(), tables).await;

            let open_conn = db_pools.open.get().await.unwrap();
            // TODO: we do not have an API endpoint to query the flags.kvdata table
            let data_row = open_conn
                .query_one("SELECT timeseries, obstime, original FROM legacy.data", &[])
                .await
                .unwrap();

            let (timeseries, obstime, original): (i64, DateTime<Utc>, Option<f64>) =
                (data_row.get(0), data_row.get(1), data_row.get(2));
            assert_eq!(obstime, Utc.with_ymd_and_hms(2024, 6, 6, 6, 0, 0).unwrap());
            assert_eq!(original, Some(1.));

            let label_row = open_conn
                .query_one(
                    "SELECT station_id, param_id, type_id, lvl, sensor \
                        FROM labels.kvalobs \
                        WHERE timeseries = $1",
                    &[&timeseries],
                )
                .await
                .unwrap();

            let station_id: i32 = label_row.get(0);
            let param_id: i32 = label_row.get(1);
            let type_id: i32 = label_row.get(2);
            let lvl: i32 = label_row.get(3);
            let sensor: i32 = label_row.get(4);

            assert_eq!(station_id, 20001);
            assert_eq!(param_id, 211);
            assert_eq!(type_id, 501);
            assert_eq!(lvl, 200);
            assert_eq!(sensor, 0);
        },
    )
    .await
}

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
