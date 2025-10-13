use chrono::NaiveDateTime;
use futures::{stream::FuturesUnordered, StreamExt};
use tokio_postgres::{Client, NoTls};
use tracing::error;

use util::MetLabel;

use crate::{
    util::{levels::LevelTable, metadata::MetadataFetch, tsupdate::DeactivatedTimeseries},
    Error,
};

pub struct Stinfosys {
    conn_string: String,
    levels: LevelTable,
}

impl Stinfosys {
    pub fn new(conn_string: String, levels: LevelTable) -> Self {
        Self {
            conn_string,
            levels,
        }
    }
}

impl MetadataFetch for &Stinfosys {
    async fn fetch_deactivated(
        &self,
        labels: Vec<MetLabel>,
    ) -> Result<Vec<DeactivatedTimeseries>, Error> {
        let (client, conn) = tokio_postgres::connect(&self.conn_string, NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = conn.await {
                error!("connection error: {e}");
            }
        });

        let mut futures = labels
            .iter()
            .map(async |label| -> Result<_, Error> {
                let (station_totime, obs_pgm_totime) = tokio::try_join!(
                    fetch_station_totime(label, &client),
                    fetch_obs_pgm_totime(label, self.levels.clone(), &client),
                )?;

                // Prefer obs_pgm if available
                let totime = match obs_pgm_totime {
                    Some(_) => obs_pgm_totime,
                    None => station_totime,
                };

                Ok((label.id, totime))
            })
            .collect::<FuturesUnordered<_>>();

        let mut deactivated = vec![];
        while let Some(res) = futures.next().await {
            let ts = match res? {
                (tsid, Some(totime)) => DeactivatedTimeseries {
                    tsid,
                    totime: totime.and_utc(),
                },
                // Skip if totime is NULL
                _ => continue,
            };

            deactivated.push(ts);
        }

        Ok(deactivated)
    }
}

async fn fetch_obs_pgm_totime(
    label: &MetLabel,
    levels: LevelTable,
    conn: &Client,
) -> Result<Option<NaiveDateTime>, Error> {
    // The funny looking ARRAY_AGG is needed because each timeseries can have multiple from/to times.
    // Most likely related to the fact that stations in the `station` tables can also have
    // multiple entries, see [fetch_station_totime]
    // We order the array by decreasing totime and only return the latest one (first
    // element in the array)
    // NOTE: we can't use the MAX operator since in Postgres NULLs are excluded
    const OBS_PGM_QUERY: &str = "\
        SELECT \
            (ARRAY_AGG(totime ORDER BY totime DESC NULLS FIRST))[1], \
            stationid, \
            paramid, \
            hlevel, \
            nsensor, \
            priority_messageid \
        FROM obs_pgm \
            WHERE stationid = $1 \
            AND paramid = $2 \
            AND hlevel IS NOT DISTINCT FROM $3 \
            AND nsensor IS NOT DISTINCT FROM $4 \
            AND priority_messageid = $5 \
        GROUP BY stationid, paramid, hlevel, nsensor, priority_messageid";

    let level = {
        levels
            .read()
            .map_err(|e| Error::Lock(e.to_string()))?
            .get(&label.param_id)
            .map(|level| level.default_hlevel)
    };

    let row_opt = conn
        .query_opt(
            OBS_PGM_QUERY,
            &[
                &label.station_id,
                &label.param_id,
                &level,
                &label.sensor,
                &label.type_id,
            ],
        )
        .await?;

    Ok(row_opt.and_then(|row| row.get(0)))
}

async fn fetch_station_totime(
    label: &MetLabel,
    conn: &Client,
) -> Result<Option<NaiveDateTime>, Error> {
    // The funny looking ARRAY_AGG is needed because each station can have multiple from/to times.
    // For example, the timeseries might have been "reset" after a change of the station position,
    // even though the station ID did not change.
    // We order the aggregated array by decreasing totime and only return the latest one (first
    // element in the array)
    // NOTE: we can't use the MAX operator since in Postgres NULLs are excluded
    const STATION_QUERY: &str = "\
        SELECT \
            (ARRAY_AGG(totime ORDER BY totime DESC NULLS FIRST))[1], \
            stationid \
        FROM station \
        WHERE stationid = $1 \
        GROUP BY stationid";

    let row_opt = conn.query_opt(STATION_QUERY, &[&label.station_id]).await?;

    Ok(row_opt.and_then(|row| row.get(0)))
}
