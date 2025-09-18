use chrono::{DateTime, Duration, TimeZone, Utc};
use rdkafka::producer::{FutureProducer, FutureRecord};

use lard_egress::patchwork::PatchworkTables;

use util::DbPools;

pub mod common;
use common::{
    legacy::{
        e2e_test_wrapper_legacy, ingest_raw, wait_for_db_readiness, IngestData, KAFKA_CHECKED_TOPIC,
    },
    Param, TestData,
};

#[tokio::test]
async fn test_kafka_checked() {
    e2e_test_wrapper_legacy(async |producer: FutureProducer, db_pools: DbPools, _| {
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
            lard_ingestion::util::quality_code::get_quality_code(useinfo.clone().unwrap().as_str())
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
    })
    .await
}

#[tokio::test]
async fn test_kafka_raw() {
    e2e_test_wrapper_legacy(
        async |producer: FutureProducer, db_pools: DbPools, tables: PatchworkTables| {
            let test_data = IngestData::new(vec![TestData {
                station_id: 20001,
                params: vec![Param::with_sensor_level("TA", (0, 200))],
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
            assert_eq!(original, Some(0.));

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
