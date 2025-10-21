use chrono::{DateTime, Utc};
use futures::future;
use tracing::{error, info};

use util::{MetLabel, PooledPgConn};

use crate::{util::metadata::MetadataFetch, Error};

// TODO: remove the WHERE when we remove/prevent NULL param IDs in the table
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
    WHERE met.param_id IS NOT NULL \
    AND timeseries.totime IS NULL";

const UPDATE_QUERY: &str = "\
    UPDATE public.timeseries SET \
        totime = $1, \
        deactivated = true \
    WHERE id = $2";

pub struct DeactivatedTimeseries {
    /// Timeseries to be updated
    pub tsid: i64,
    /// Totime value found in the metadata source
    pub totime: DateTime<Utc>,
}

pub async fn set_deactivated(
    metadata_db: impl MetadataFetch,
    conn: &mut PooledPgConn<'_>,
) -> Result<(), Error> {
    let tx = conn.transaction().await?;

    // Explicitly take the lock so we can prevent concurrent access to the rows we are going to update
    tx.execute(
        "LOCK TABLE public.timeseries IN SHARE ROW EXCLUSIVE MODE",
        &[],
    )
    .await?;

    let rows = tx.query(OPEN_TIMESERIES_QUERY, &[]).await?;

    let labels = rows
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

    let deactivated = metadata_db.fetch_deactivated(labels).await?;

    future::join_all(deactivated.into_iter().map(async |ts| {
        match tx.execute(UPDATE_QUERY, &[&ts.totime, &ts.tsid]).await {
            Ok(_) => info!("Tsid {} updated", ts.tsid),
            Err(err) => error!("Could not update tsid {}: {}", ts.tsid, err),
        }
    }))
    .await;

    tx.commit().await?;

    Ok(())
}
