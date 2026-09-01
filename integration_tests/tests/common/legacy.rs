use std::time::Instant;

use chrono::{DateTime, Duration, TimeZone, Utc};
use rdkafka::producer::{FutureProducer, FutureRecord};

use lard_egress::patchwork::PatchworkTables;
use util::{
    DbPools, PooledPgConn,
    stinfofacade::permissions::{PermitTables, timeseries_get_permit},
};

use crate::common::{Param, TestData, update_patchwork_table};

pub const KAFKA_CHECKED_TOPIC: &str = "checked";
pub const KAFKA_RAW_TOPIC: &str = "raw";
pub const KAFKA_CHECKED_HIST_TOPIC: &str = "hist.checked";
pub const KAFKA_GROUP: &str = "lard_test";

struct IngestData<'a> {
    ts: TestData<'a>,
    expected_open: usize,
    expected_restricted: usize,
}

impl<'a> IngestData<'a> {
    pub fn new(data: TestData<'a>, permit_tables: PermitTables) -> Self {
        let mut expected_open = 0;
        let mut expected_restricted = 0;

        // Calculate expected rows to be found in the database after ingestion
        // To be honest this feels like another hack
        for param in &data.params {
            let permit = timeseries_get_permit(
                permit_tables.clone(),
                data.station_id,
                data.type_id,
                Some(param.id),
            )
            .unwrap();
            if permit == Some(1) {
                expected_open += data.len;
            } else {
                expected_restricted += data.len
            }
        }

        Self {
            ts: data,
            expected_open,
            expected_restricted,
        }
    }
}

// Helper function that waits for data to be available
async fn wait_for_db_readiness(
    conn: &PooledPgConn<'_>,
    station_id: i32,
    param_id: i32,
    expected_rows: usize,
) {
    let timeout = std::time::Duration::from_secs(10);
    let timeout_start = Instant::now();
    loop {
        let rows_scalar = conn
            .query(
                "SELECT d.timeseries
                 FROM legacy.data d
                     JOIN labels.met l
                         ON d.timeseries = l.timeseries
                 WHERE l.station_id = $1
                     AND l.param_id = $2",
                &[&station_id, &param_id],
            )
            .await;
        let rows_nonscalar = conn
            .query(
                "SELECT d.timeseries
                 FROM public.nonscalar_data d
                     JOIN labels.met l
                         ON d.timeseries = l.timeseries
                 WHERE l.station_id = $1
                     AND l.param_id = $2",
                &[&station_id, &param_id],
            )
            .await;

        if let (Ok(scalar), Ok(nonscalar)) = (rows_scalar, rows_nonscalar)
            && scalar.len() + nonscalar.len() == expected_rows
        {
            break;
        };

        if timeout_start.elapsed() > timeout {
            panic!("Timed out waiting for data to appear")
        }
    }
}

/// Helper function that ingests data into the raw queue, waits for it to be available, and updates
/// the patchwork tables
async fn ingest_raw(
    data: &IngestData<'_>,
    producer: &FutureProducer,
    pools: DbPools,
    tables: PatchworkTables,
) {
    producer
        .send_result(
            FutureRecord::to(KAFKA_RAW_TOPIC)
                .key("")
                .payload(&data.ts.obsinn_ones()),
        )
        .unwrap()
        .await
        .unwrap()
        .unwrap();

    let open_conn = pools.open.get().await.unwrap();
    let restricted_conn = pools.restricted.get().await.unwrap();

    // As we have no way to sync with message processing in kvkafka ingestion, we just keep
    // trying to fetch data with a timeout
    tokio::join!(
        wait_for_db_readiness(
            &open_conn,
            data.ts.station_id,
            data.ts.params.first().unwrap().id,
            data.expected_open
        ),
        wait_for_db_readiness(
            &restricted_conn,
            data.ts.station_id,
            data.ts.params.first().unwrap().id,
            data.expected_restricted
        ),
    );

    tokio::join!(
        update_patchwork_table(&open_conn, tables.open),
        update_patchwork_table(&restricted_conn, tables.restricted)
    );
}

struct CheckedDatum {
    type_id: Option<i32>,
    lvl: Option<i32>,
    sensor: Option<i32>,
    obstime: DateTime<Utc>,
    original: Option<f64>,
    corrected: Option<f64>,
    qualitycode: Option<i32>,
    controlinfo: Option<String>,
    useinfo: Option<String>,
    cfailed: Option<String>,
}

async fn query_checked(conn: &PooledPgConn<'_>, station_id: i32, param_id: i32) -> CheckedDatum {
    // TODO: we do not have an API endpoint to query the flags.kvdata table
    let row = conn
        .query_one(
            "SELECT
                l.type_id,
                l.lvl,
                l.sensor,
                d.obstime,
                d.original,
                d.corrected, 
                d.quality_code,
                d.controlinfo,
                d.useinfo,
                d.cfailed 
            FROM legacy.data as d
                JOIN labels.met as l
                    ON d.timeseries = l.timeseries
            WHERE l.station_id = $1
                AND l.param_id = $2",
            &[&station_id, &param_id],
        )
        .await
        .unwrap();

    CheckedDatum {
        type_id: row.get(0),
        lvl: row.get(1),
        sensor: row.get(2),
        obstime: row.get(3),
        original: row.get(4),
        corrected: row.get(5),
        qualitycode: row.get(6),
        controlinfo: row.get(7),
        useinfo: row.get(8),
        cfailed: row.get(9),
    }
}

pub async fn ensure_kafka_ingestion(
    producer: FutureProducer,
    db_pools: DbPools,
    patchwork_tables: PatchworkTables,
    permit_tables: PermitTables,
) {
    // TODO: we should change this to match the param and time of one of the checked obs, so
    // we cover the update mechanism
    let test_data = IngestData::new(
        TestData {
            station_id: 30005,
            params: vec![Param::new("TA").with_sensor_level((0, 200))],
            start_time: Utc.with_ymd_and_hms(2024, 6, 6, 6, 0, 0).unwrap(),
            period: Duration::hours(1),
            type_id: 501,
            len: 1,
        },
        permit_tables,
    );

    // This observation was 2.5 hours late??
    let kafka_xml1 = r#"<?xml?>
        <KvalobsData producer=\"kvqabase\" created=\"2024-06-06 08:30:43\">
            <station val=\"30002\">
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

    // special values
    let kafka_xml2 = r#"<?xml?>
            <KvalobsData producer=\"kvqabase\" created=\"2024-06-06 08:30:43\">
                <station val=\"30004\">
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

    ingest_raw(&test_data, &producer, db_pools.clone(), patchwork_tables).await;

    let open_conn = db_pools.open.get().await.unwrap();

    let row_raw = query_checked(&open_conn, 30005, 211).await;
    assert_eq!(
        row_raw.obstime,
        Utc.with_ymd_and_hms(2024, 6, 6, 6, 0, 0).unwrap()
    );
    assert_eq!(row_raw.original, Some(1.));
    // TODO: also check the qc stuff is NULL?

    assert_eq!(row_raw.type_id, Some(501));
    assert_eq!(row_raw.lvl, Some(200));
    assert_eq!(row_raw.sensor, Some(0));

    producer
        .send_result(
            FutureRecord::to(KAFKA_CHECKED_TOPIC)
                .key("")
                .payload(kafka_xml1),
        )
        .unwrap()
        .await
        .unwrap()
        .unwrap();

    producer
        .send_result(
            FutureRecord::to(KAFKA_CHECKED_TOPIC)
                .key("")
                .payload(kafka_xml2),
        )
        .unwrap()
        .await
        .unwrap()
        .unwrap();

    // As we have no way to sync with message processing in kvkafka ingestion, we just keep
    // trying to fetch data with a timeout
    wait_for_db_readiness(&open_conn, 30002, 106, 1).await;
    wait_for_db_readiness(&open_conn, 30004, 106, 1).await;

    let row_checked = query_checked(&open_conn, 30002, 106).await;

    assert_eq!(
        row_checked.obstime,
        Utc.with_ymd_and_hms(2024, 6, 6, 6, 0, 0).unwrap()
    );
    assert_eq!(row_checked.original, Some(10.));
    assert_eq!(row_checked.corrected, Some(10.));
    assert_eq!(
        row_checked.qualitycode,
        lard_ingestion::util::quality_code::get_quality_code(
            row_checked.useinfo.clone().unwrap().as_str()
        )
    );
    assert_eq!(
        row_checked.controlinfo,
        Some("1000000000000000".to_string())
    );
    assert_eq!(row_checked.useinfo, Some("9000000000000000".to_string()));
    assert_eq!(row_checked.cfailed, None);

    assert_eq!(row_checked.type_id, Some(-4));
    assert_eq!(row_checked.lvl, Some(0));
    assert_eq!(row_checked.sensor, Some(0));

    let row_special = query_checked(&open_conn, 30004, 106).await;

    assert_eq!(
        row_special.obstime,
        Utc.with_ymd_and_hms(2024, 6, 6, 6, 0, 0).unwrap()
    );
    assert_eq!(row_special.original, None); // -32767 should be converted to a Null
    assert_eq!(row_special.corrected, None);
    assert_eq!(
        row_special.qualitycode,
        lard_ingestion::util::quality_code::get_quality_code(
            row_special.useinfo.clone().unwrap().as_str()
        )
    );
    assert_eq!(
        row_special.controlinfo,
        Some("0000000000000000".to_string())
    );
    assert_eq!(row_special.useinfo, Some("0000000000000000".to_string()));
    assert_eq!(row_special.cfailed, None);

    eprintln!("kafka_ingestion ok");
}
