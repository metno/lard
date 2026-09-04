use std::collections::HashMap;

use chrono::{DateTime, TimeZone, Utc};

use util::{
    DbPools, MetTimeseriesKey, OpenTimerange, PooledPgConn,
    stinfofacade::{from_to_time::ObsPgmProblem, from_to_time::update_from_to, param::ParamTables},
};

// TODO: we should implement an availability endpoint?
async fn get_fromtotime(
    conn: &PooledPgConn<'_>,
) -> Vec<(Option<DateTime<Utc>>, Option<DateTime<Utc>>)> {
    conn.query(
        "SELECT t.fromtime, t.totime FROM timeseries t \
        JOIN labels.met l \
            ON t.id = l.timeseries \
        WHERE l.station_id IN (20001, 20003)
            AND l.param_id = 211 \
            AND l.type_id = 501 \
        ORDER BY l.station_id",
        &[],
    )
    .await
    .unwrap()
    .iter()
    .map(|row| (row.get(0), row.get(1)))
    .collect()
}

pub async fn ensure_fromtotime_update(pools: DbPools, param_tables: ParamTables) {
    let fromtime = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
    let totime: DateTime<Utc> = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();

    let station_times = HashMap::from([(
        20003,
        OpenTimerange {
            from: Some(fromtime),
            to: Some(totime),
        },
    )]);
    let obs_pgm_times: HashMap<MetTimeseriesKey, OpenTimerange> = HashMap::new();

    let expected = vec![
        // timeseries on station 20001 is not covered by mock obs_pgm, so it is left open
        (Some(fromtime), None),
        // timeseries on station 20003 should be closed based on metadata
        (Some(fromtime), Some(totime)),
    ];

    let mut conn = pools.open.get().await.unwrap();

    // totimes should be empty
    for fromtotimes in get_fromtotime(&conn).await {
        assert_eq!(fromtotimes.1, None); // to time
    }

    let (problems_tx, mut problems_rx) = tokio::sync::mpsc::channel::<ObsPgmProblem>(8);

    // ignore problems
    tokio::spawn(async move { while problems_rx.recv().await.is_some() {} });

    update_from_to(
        &mut conn,
        &obs_pgm_times,
        &station_times,
        param_tables,
        problems_tx.clone(),
        tokio_util::sync::CancellationToken::new(),
    )
    .await
    .unwrap();

    let after = get_fromtotime(&conn).await;
    assert_eq!(after.len(), expected.len());

    // Now the totime for station 20003 should be set (and the to time for station 20001 should be its first observation time)
    for (db, expect) in after.into_iter().zip(expected) {
        assert_eq!(db.0, expect.0);
        assert_eq!(db.1, expect.1);
    }

    eprintln!("fromtotime_update ok");
}
