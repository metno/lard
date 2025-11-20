use chrono::{DateTime, Utc};
use futures::future;
use std::collections::HashMap;
use tracing::error;

use crate::{util::stinfosys::fetch_from_to_for_update, Error};
use lard_egress::patchwork::OpenTimerange;
use util::{MetLabel, MetTimeseriesKey, PooledPgConn};

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
        met.sensor, \
        timeseries.fromtime, \
        timeseries.totime \
    FROM labels.met \
    JOIN timeseries \
        ON met.timeseries = timeseries.id \
    WHERE met.param_id IS NOT NULL \
    AND (timeseries.totime IS NULL \
    OR timeseries.totime < timeseries.fromtime)";
// TODO: should we also get the from/to from the underlying data?
// this would be an intensive call and maybe should not be done often?

// Deactivated is information for the database
// for a timeseries it is enough that the fromtime is closed
const UPDATE_QUERY: &str = "\
    UPDATE public.timeseries SET \
        totime = $1, \
        fromtime = $2, \
        deactivated = false \
    WHERE id = $3";

pub struct TSupdateTimeseries {
    /// Timeseries to be updated
    pub tsid: i64,
    /// Fromtime value found in the metadata source
    pub fromtime: DateTime<Utc>,
    /// Totime value found in the metadata source
    pub totime: DateTime<Utc>,
}

impl TSupdateTimeseries {
    pub fn new(tsid: i64, fromtime: DateTime<Utc>, totime: DateTime<Utc>) -> TSupdateTimeseries {
        TSupdateTimeseries {
            tsid,
            fromtime,
            totime,
        }
    }
}

pub async fn set_from_to_obs_pgm(
    conn: &mut PooledPgConn<'_>,
    obs_pgm_times: &HashMap<MetTimeseriesKey, OpenTimerange>,
    station_times: &HashMap<i32, OpenTimerange>,
) -> Result<(), Error> {
    let tx = conn.transaction().await?;

    // Explicitly take the lock so we can prevent concurrent access to the rows we are going to update
    tx.execute(
        "LOCK TABLE public.timeseries IN SHARE ROW EXCLUSIVE MODE",
        &[],
    )
    .await?;

    let rows = tx.query(OPEN_TIMESERIES_QUERY, &[]).await?;

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

    let mut ts_from_to: HashMap<i64, OpenTimerange> = HashMap::new();
    rows.iter().for_each(|row| {
        ts_from_to.insert(row.get(0), OpenTimerange::new(row.get(6), row.get(7)));
    });

    let deactivated =
        fetch_from_to_for_update(obs_pgm_times, station_times, ts_from_to, labels).await?;

    future::join_all(deactivated.into_iter().map(async |ts| {
        match tx
            .execute(UPDATE_QUERY, &[&ts.totime, &ts.fromtime, &ts.tsid])
            .await
        {
            Ok(_) => (), //info!("Tsid {} updated", ts.tsid),
            Err(err) => error!("Could not update tsid {}: {}", ts.tsid, err),
        }
    }))
    .await;

    tx.commit().await?;

    Ok(())
}
