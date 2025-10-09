use chrono::{DateTime, Utc};
use futures::future;
use tracing::{error, info};

use util::{MetLabel, PooledPgConn};

use crate::{util::metadata::MetadataFetch, Error};

pub struct DeactivatedTimeseries {
    /// Timeseries to be updated
    pub tsid: i64,
    /// Totime value found in Stinfosys
    pub totime: DateTime<Utc>,
}

pub async fn fetch_active_timeseries(conn: &PooledPgConn<'_>) -> Result<Vec<MetLabel>, Error> {
    // TODO: remove the WHERE when we remove/prevent NULL param IDs in the table
    const LABEL_QUERY: &str = "\
        SELECT \
            timeseries.id, \
            met.station_id, \
            met.param_id, \
            met.type_id, \
            met.lvl, \
            met.sensor \
        FROM labels.met \
        JOIN timeseries \
            ON met.timeseries = timeseries.id
        WHERE met.param_id IS NOT NULL";

    let rows = conn.query(LABEL_QUERY, &[]).await?;

    let labels = rows
        .iter()
        .map(|row| MetLabel {
            id: row.get(0),
            station_id: row.get(1),
            param_id: row.get(2),
            type_id: row.get(3),
            level: row.get(4),
            sensor: row.get(5),
        })
        .collect();

    Ok(labels)
}

pub async fn set_deactivated(
    metadata_db: impl MetadataFetch,
    conn: &PooledPgConn<'_>,
) -> Result<(), Error> {
    let labels = fetch_active_timeseries(conn).await?;
    let deactivated = metadata_db.fetch_deactivated(labels).await?;

    const UPDATE_QUERY: &str = "\
        UPDATE public.timeseries SET \
            totime = $1, \
            deactivated = true \
        WHERE id = $2";

    future::join_all(deactivated.into_iter().map(|ts| async move {
        match conn.execute(UPDATE_QUERY, &[&ts.totime, &ts.tsid]).await {
            Ok(_) => info!("Tsid {} updated", ts.tsid),
            Err(err) => error!("Could not update tsid {}: {}", ts.tsid, err),
        }
    }))
    .await;

    Ok(())
}
