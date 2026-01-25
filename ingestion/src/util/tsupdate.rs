use futures::future;
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::HashMap;
use tokio::time::Instant;
use tracing::{error, info};

use crate::{util::stinfosys::calc_from_tos, Error, FROM_TO_FUTURES_FAILURES};
use util::{
    stinfofacade::param::ParamTables, MetLabel, MetTimeseriesKey, OpenTimerange, PooledPgConn,
};

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
const MAX_MIN_TIMESERIES_LEGACY_DATA_QUERY: &str = "SELECT
        MIN(obstime), \
        MAX(obstime) \
    FROM legacy.data \
    WHERE timeseries = $1";
// For now data is in legacy data...
const _MAX_MIN_TIMESERIES_DATA_QUERY: &str = "SELECT 
        MIN(obstime), \
        MAX(obstime) \
    FROM data \
    WHERE timeseries = $1";
const MAX_MIN_TIMESERIES_NONSCALAR_DATA_QUERY: &str = "SELECT 
        MIN(obstime), \
        MAX(obstime) \
    FROM nonscalar_data \
    WHERE timeseries = $1";

const UPDATE_QUERY: &str = "\
    UPDATE public.timeseries SET \
        fromtime = $1, \
        totime = $2 \
    WHERE id = $3";

async fn get_from_to_ts(
    conn: &mut PooledPgConn<'_>,
    labels: Vec<MetLabel>,
    params: ParamTables,
) -> Result<HashMap<i64, OpenTimerange>, Error> {
    let mut ts_from_to: HashMap<i64, OpenTimerange> = HashMap::new();

    let scalar_paramids = params.read()?.scalar_paramids.clone();

    let mut futures_ts_from_to = labels
        .iter()
        .map(async |label| {
            if scalar_paramids.contains(&label.key.param_id) {
                // for now we only have data in legacy.data, eventually we will
                // need to switch these
                //conn.query_one(MAX_MIN_TIMESERIES_DATA_QUERY, &[&label.id])
                //    .await
                (
                    label.id,
                    conn.query_one(MAX_MIN_TIMESERIES_LEGACY_DATA_QUERY, &[&label.id])
                        .await,
                )
            } else {
                // nonscalar
                (
                    label.id,
                    conn.query_one(MAX_MIN_TIMESERIES_NONSCALAR_DATA_QUERY, &[&label.id])
                        .await,
                )
            }
        })
        .collect::<FuturesUnordered<_>>()
        .enumerate();

    while let Some((_i, res)) = futures_ts_from_to.next().await {
        match res {
            (id, Ok(val)) => ts_from_to.insert(id, OpenTimerange::new(val.get(0), val.get(1))),
            (id, Err(_err)) => {
                // log these fails
                metrics::counter!(FROM_TO_FUTURES_FAILURES).increment(1);
                // Too much noise in log for now, due to issue noted below...
                /*
                error!(
                    "max min for timeseries future failed: {}, for tsid: {}",
                    err, id
                );
                continue;
                */
                // NOTE: due to issue with scalar vs nonscalar data, we cannot realiably get the timeseries max and min.
                // for now we if the call fails the time range will be None, None
                ts_from_to.insert(id, OpenTimerange::new(None, None))
            }
        };
    }
    Ok(ts_from_to)
}

pub async fn update_from_to(
    conn: &mut PooledPgConn<'_>,
    obs_pgm_times: &HashMap<MetTimeseriesKey, OpenTimerange>,
    station_times: &HashMap<i32, OpenTimerange>,
    params: ParamTables,
) -> Result<(), Error> {
    let now = Instant::now();
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

    let ts_from_to = get_from_to_ts(conn, labels.clone(), params).await?;

    let closed = calc_from_tos(obs_pgm_times, station_times, ts_from_to, labels);
    info!("Updating from/to for {} timeseries (.len())", closed.len());

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
            Ok(_) => (), //info!("Tsid {} updated", tsid),
            Err(err) => error!("Could not update tsid {}: {}", tsid, err),
        }
    }))
    .await;

    tx.commit().await?;
    info!(
        "Finished updating from/to for timeseries {:.2?}",
        now.elapsed()
    );

    Ok(())
}
