use futures::future;
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::HashMap;
use tracing::error;

use crate::{get_scalar_paramids, util::stinfosys::calc_from_tos, Error, FROM_TO_FUTURES_FAILURES};
use util::{MetLabel, MetTimeseriesKey, OpenTimerange, PooledPgConn};

// TODO: remove the WHERE when we remove/prevent NULL param IDs in the table
// NOTE: In addition to finding open timeseries, we also find the timeseries
// where somehow the fromtime is before the to time. This is because of an
// earlier bug, but could happen for other reasons.
const OPEN_TIMESERIES_QUERY: &str = "\
    SELECT \
        timeseries.id, \
        met.station_id, \
        met.param_id, \
        met.type_id, \
        met.lvl, \
        met.sensor \
    FROM labels.met \
    JOIN timeseries \
        ON met.timeseries = timeseries.id \
    WHERE met.param_id IS NOT NULL";
// NOTE: the from to in the timeseries table need to be kept updated
// so we also need to check the from/to of the underlying data
const MAX_MIN_TIMESERIES_LEGACY_DATA_QUERY: &str = "SELECT timeseries, 
        MIN(obstime), \
        MAX(obstime) \
    FROM legacy.data \
    WHERE timeseries = $1
    GROUP BY timeseries";
// For now data is in legacy data...
const _MAX_MIN_TIMESERIES_DATA_QUERY: &str = "SELECT timeseries, 
        MIN(obstime), \
        MAX(obstime) \
    FROM data \
    WHERE timeseries = $1
    GROUP BY timeseries";
const _MAX_MIN_TIMESERIES_NONSCALAR_DATA_QUERY: &str = "SELECT timeseries, 
        MIN(obstime), \
        MAX(obstime) \
    FROM nonscalar_data \
    WHERE timeseries = $1
    GROUP BY timeseries";

// Deactivated is information for the database
// for a timeseries it is enough that the fromtime is closed
const UPDATE_QUERY: &str = "\
    UPDATE public.timeseries SET \
        fromtime = $1, \
        totime = $2, \
        deactivated = false \
    WHERE id = $3";

async fn get_from_to_ts(
    conn: &mut PooledPgConn<'_>,
    labels: Vec<MetLabel>,
) -> Result<HashMap<i64, OpenTimerange>, Error> {
    let mut ts_from_to: HashMap<i64, OpenTimerange> = HashMap::new();
    let _scalar_list = get_scalar_paramids("../resources/paramconversions.csv").unwrap();

    let mut futures_ts_from_to = labels
        .iter()
        .map(async |label| {
            // for now we only have data in legacy.data
            conn.query_one(MAX_MIN_TIMESERIES_LEGACY_DATA_QUERY, &[&label.id])
                .await
            /*
            if scalar_list.contains(&label.key.param_id) {
                // nonscalar
                conn.query_one(MAX_MIN_TIMESERIES_NONSCALAR_DATA_QUERY, &[&label.id])
                    .await
            } else {
                conn.query_one(MAX_MIN_TIMESERIES_DATA_QUERY, &[&label.id])
                    .await
            }
            */
        })
        .collect::<FuturesUnordered<_>>()
        .enumerate();

    while let Some((i, res)) = futures_ts_from_to.next().await {
        let row = match res {
            Ok(val) => val,
            Err(err) => {
                // log these fails
                metrics::counter!(FROM_TO_FUTURES_FAILURES).increment(1);
                error!("max min for timeseries future failed: {}, {}", err, i);
                continue;
            }
        };
        ts_from_to.insert(row.get(0), OpenTimerange::new(row.get(1), row.get(2)));
    }
    Ok(ts_from_to)
}

pub async fn update_from_to(
    conn: &mut PooledPgConn<'_>,
    obs_pgm_times: &HashMap<MetTimeseriesKey, OpenTimerange>,
    station_times: &HashMap<i32, OpenTimerange>,
) -> Result<(), Error> {
    let rows = conn.query(OPEN_TIMESERIES_QUERY, &[]).await?;

    let labels: Vec<MetLabel> = rows
        .iter()
        .map(|row| {
            MetLabel::new(
                row.get(0),
                row.get(1),
                row.get(2),
                row.get(3),
                row.get(4),
                row.get(5),
            )
        })
        .collect();

    let ts_from_to = get_from_to_ts(conn, labels.clone()).await?;

    let closed = calc_from_tos(obs_pgm_times, station_times, ts_from_to, labels);

    let tx = conn.transaction().await?;

    // Explicitly take the lock so we can prevent concurrent access to the rows we are going to update
    tx.execute(
        "LOCK TABLE public.timeseries IN SHARE ROW EXCLUSIVE MODE",
        &[],
    )
    .await?;

    future::join_all(closed.into_iter().map(async |(tsid, timerange)| {
        match tx
            .execute(UPDATE_QUERY, &[&timerange.from, &timerange.to, &tsid])
            .await
        {
            Ok(_) => (), //info!("Tsid {} updated", ts.tsid),
            Err(err) => error!("Could not update tsid {}: {}", tsid, err),
        }
    }))
    .await;

    tx.commit().await?;

    Ok(())
}
