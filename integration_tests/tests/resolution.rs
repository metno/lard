use chrono::{Duration, TimeZone, Utc};
use lard_egress::patchwork::PatchworkTables;
use lard_ingestion::util::time_resolution::determine_time_resolution_of_timeseries;
use pg_interval::Interval;
use rdkafka::producer::FutureProducer;

use util::DbPools;

pub mod common;
use common::{
    Param, TestData,
    legacy::{IngestData, e2e_test_wrapper_legacy, ingest_raw},
};

#[tokio::test]
async fn test_find_time_resolution() {
    e2e_test_wrapper_legacy(
        &["TA"],
        async |producer: FutureProducer, db_pools: DbPools, tables: PatchworkTables| {
            // data with a offset of 20 mins, and a resolution of 1h
            // TODO: not currently dealing with offset... will implement in future perhaps?
            let test_data = IngestData::new(vec![TestData {
                station_id: 20001,
                params: vec![Param::new("TA").with_sensor_level((0, 200))],
                start_time: Utc.with_ymd_and_hms(2024, 6, 6, 6, 20, 0).unwrap(),
                period: Duration::hours(1),
                type_id: 501,
                len: 30,
            }]);

            ingest_raw(&test_data, producer, db_pools.clone(), tables).await;

            let open_conn = db_pools.open.get().await.unwrap();
            // legacy should get a kvalobs label 
            let ts = open_conn
                .query_one("SELECT timeseries FROM labels.kvalobs WHERE station_id = 20001 AND param_id = 211", &[])
                .await
                .unwrap()
                .get::<_, i64>("timeseries");

            let resolution = determine_time_resolution_of_timeseries(&open_conn, ts)
                .await
                .unwrap_or_else(|e| {
                    panic!(
                        "Failed to determine time resolution for timeseries {ts}, error message: {e:?}"
                    )
                });
            let hourly = Interval::from_duration(Duration::hours(1)).unwrap();
            assert_eq!(
                resolution, hourly,
                "Expected time resolution to be 1h, but got {:?}",
                resolution
            );
        },
    )
    .await
}
