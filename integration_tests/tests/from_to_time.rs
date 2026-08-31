use chrono::{DateTime, Duration, TimeZone, Utc};
use rdkafka::producer::FutureProducer;

use lard_egress::patchwork::PatchworkTables;
use util::{
    DbPools, PooledPgConn,
    mock::metadata::MetadataMock,
    stinfofacade::{self, from_to_time::ObsPgmProblem, from_to_time::update_from_to},
};

pub mod common;
use common::{
    Param, TestData,
    legacy::{IngestData, e2e_test_wrapper_legacy, ingest_raw},
};

// TODO: we should implement an availability endpoint?
async fn get_fromtotime(
    conn: &PooledPgConn<'_>,
) -> Vec<(Option<DateTime<Utc>>, Option<DateTime<Utc>>)> {
    conn.query(
        "SELECT timeseries.fromtime, timeseries.totime FROM timeseries \
        JOIN labels.met \
            ON timeseries.id = met.timeseries \
        ORDER BY station_id",
        &[],
    )
    .await
    .unwrap()
    .iter()
    .map(|row| (row.get(0), row.get(1)))
    .collect()
}

#[ignore]
#[tokio::test]
async fn test_fromtotime_update() {
    e2e_test_wrapper_legacy(
        &["KLOBS", "TA"],
        async |producer: FutureProducer, db_pools: DbPools, patchwork_tables: PatchworkTables| {
            let timeseries = IngestData::new(vec![
                TestData {
                    station_id: 10001,
                    params: vec![Param::new("KLOBS")],
                    start_time: Utc.with_ymd_and_hms(1980, 12, 31, 12, 0, 0).unwrap(),
                    period: Duration::hours(1),
                    type_id: 503,
                    len: 14, // metadata should cut off the last part of this that goes into 1981
                },
                TestData {
                    station_id: 20001,
                    params: vec![Param::new("TA")],
                    start_time: Utc.with_ymd_and_hms(1950, 1, 1, 0, 0, 0).unwrap(),
                    period: Duration::hours(1),
                    type_id: 501,
                    len: 12,
                },
            ]);
            ingest_raw(&timeseries, producer, db_pools.clone(), patchwork_tables).await;

            let fromtime = Utc.with_ymd_and_hms(1980, 12, 1, 0, 0, 0).unwrap();
            let totime: DateTime<Utc> = Utc.with_ymd_and_hms(1981, 1, 1, 0, 0, 0).unwrap();

            let metadata_mock = MetadataMock {
                station: 10001,
                fromtime,
                totime,
            };

            let expected = vec![
                // timeseries on station 10001 should be closed based on metadata
                (
                    Some(Utc.with_ymd_and_hms(1980, 12, 31, 12, 0, 0).unwrap()),
                    Some(totime),
                ),
                // timeseries on station 20001 is not, so it is left open
                (
                    Some(Utc.with_ymd_and_hms(1950, 1, 1, 0, 0, 0).unwrap()),
                    None,
                ),
            ];

            let mut conn = db_pools.open.get().await.unwrap();

            // totimes should be empty
            for fromtotimes in get_fromtotime(&conn).await {
                assert_eq!(fromtotimes.1, None); // to time
            }

            let (obs_pgm_times_map, station_times_map) =
                metadata_mock.cache_closed_stinfosys().await;

            let param_tables = stinfofacade::param::from_codes(&["TA", "KLOBS"]);

            let (problems_tx, mut problems_rx) = tokio::sync::mpsc::channel::<ObsPgmProblem>(8);

            // ignore problems
            tokio::spawn(async move { while problems_rx.recv().await.is_some() {} });

            update_from_to(
                &mut conn,
                &obs_pgm_times_map,
                &station_times_map,
                param_tables,
                problems_tx.clone(),
                tokio_util::sync::CancellationToken::new(),
            )
            .await
            .unwrap();

            let after = get_fromtotime(&conn).await;

            // Now the totime for station 10001 should be set (and the to time for station 20001 should be its first observation time)
            for (db, expect) in after.into_iter().zip(expected) {
                assert_eq!(db.0, expect.0);
                assert_eq!(db.1, expect.1);
            }
        },
    )
    .await
}
