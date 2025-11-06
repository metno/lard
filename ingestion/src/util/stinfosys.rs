use std::collections::HashMap;

use chrono::{DateTime, NaiveDateTime, Utc};
use futures::{stream::FuturesUnordered, StreamExt};
use tokio_postgres::{Client, NoTls};
use tracing::error;

use util::{MetLabel, MetTimeseriesKey};

use crate::{
    util::{
        levels::{param_get_level, LevelTable},
        tsupdate::DeactivatedTimeseries,
    },
    Error,
};

pub struct Stinfosys {
    conn_string: String,
    levels: LevelTable,
}

type StationTotimeMap = HashMap<i32, DateTime<Utc>>;
type StationFromtimeMap = HashMap<i32, DateTime<Utc>>;
type ObsPgmTotimeMap = HashMap<MetTimeseriesKey, DateTime<Utc>>;
type ObsPgmFromtimeMap = HashMap<MetTimeseriesKey, DateTime<Utc>>;

impl Stinfosys {
    pub fn new(conn_string: String, levels: LevelTable) -> Self {
        Self {
            conn_string,
            levels,
        }
    }

    pub async fn cache_deactivated_stinfosys(
        &self,
    ) -> Result<
        (
            HashMap<MetTimeseriesKey, DateTime<Utc>>,
            HashMap<MetTimeseriesKey, DateTime<Utc>>,
            HashMap<i32, DateTime<Utc>>,
            HashMap<i32, DateTime<Utc>>,
        ),
        Error,
    > {
        let (client, conn) = tokio_postgres::connect(&self.conn_string, NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = conn.await {
                error!("connection error: {e}");
            }
        });

        // Fetch all deactivated timeseries in Stinfosys
        let (obs_pgm_fromtime, obs_pgm_totime, station_fromtime, station_totime) = tokio::try_join!(
            fetch_obs_pgm_fromtime(self.levels.clone(), &client),
            fetch_obs_pgm_totime(self.levels.clone(), &client),
            fetch_station_fromtime(&client),
            fetch_station_totime(&client),
        )?;

        Ok((
            obs_pgm_fromtime,
            obs_pgm_totime,
            station_fromtime,
            station_totime,
        ))
    }
}

pub async fn fetch_deactivated(
    obs_pgm_fromtime: &HashMap<MetTimeseriesKey, DateTime<Utc>>,
    obs_pgm_totime: &HashMap<MetTimeseriesKey, DateTime<Utc>>,
    station_fromtime: &HashMap<i32, DateTime<Utc>>,
    station_totime: &HashMap<i32, DateTime<Utc>>,
    labels: Vec<MetLabel>,
) -> Result<Vec<DeactivatedTimeseries>, Error> {
    let mut futures = labels
        .iter()
        .map(async |label| -> Result<_, Error> {
            if station_fromtime.get(&label.key.station_id).is_some()
                && obs_pgm_totime.get(&label.key).is_some()
                && obs_pgm_totime.get(&label.key) < station_fromtime.get(&label.key.station_id)
            {
                //   |------obs_pgm------|
                //                           |--station--|
                // use the station from/to so as not to cause "twisting"
                // aka a to time before from time
                // TODO: Could do something smarter here, this data is maybe mislabeled?
                let fromtime = station_fromtime.get(&label.key.station_id).copied();
                let totime = station_totime.get(&label.key.station_id).copied();
                Ok((label.id, fromtime, totime))
            } else {
                // station had data and it overlaps in some way with obs_pgm
                // so we assume we should use obs_pgm deactivation times...
                //   |------obs_pgm------|
                //       |--station--|
                let mut fromtime = obs_pgm_fromtime
                    .get(&label.key)
                    .or(station_fromtime.get(&label.key.station_id))
                    .copied();
                // check which is less "permissive" for fromtime
                if obs_pgm_fromtime.get(&label.key).is_some()
                    && station_fromtime.get(&label.key.station_id).is_some()
                {
                    // choose the later time
                    if let (Some(pgm_time), Some(station_time)) = (
                        obs_pgm_fromtime.get(&label.key),
                        station_fromtime.get(&label.key.station_id),
                    ) {
                        //   |----obs_pgm----|
                        //       |--station--|
                        if station_time > pgm_time {
                            // use station time
                            fromtime = Some(*station_time);
                        }
                    }
                }
                let mut totime = obs_pgm_totime
                    .get(&label.key)
                    .or(station_totime.get(&label.key.station_id))
                    .copied();
                // check which is less "permissive" for totime
                if obs_pgm_totime.get(&label.key).is_some()
                    && station_totime.get(&label.key.station_id).is_some()
                {
                    // choose the earlier time
                    if let (Some(pgm_time), Some(station_time)) = (
                        obs_pgm_totime.get(&label.key),
                        station_totime.get(&label.key.station_id),
                    ) {
                        //   |----obs_pgm----|
                        //   |--station--|
                        if station_time < pgm_time {
                            // use station time
                            totime = Some(*station_time);
                        }
                    }
                }
                Ok((label.id, fromtime, totime))
            }
        })
        .collect::<FuturesUnordered<_>>();

    let mut deactivated = vec![];
    while let Some(res) = futures.next().await {
        let ts = match res? {
            (tsid, Some(fromtime), Some(totime)) => DeactivatedTimeseries {
                tsid,
                fromtime,
                totime,
            },
            // Skip if a valid totime was not found in stinfosys
            _ => continue,
        };

        deactivated.push(ts);
    }

    Ok(deactivated)
}

async fn fetch_obs_pgm_totime(levels: LevelTable, conn: &Client) -> Result<ObsPgmTotimeMap, Error> {
    // The funny looking ARRAY_AGG is needed because each timeseries can have multiple from/to times.
    // Most likely related to the fact that stations in the `station` tables can also have
    // multiple entries, see [fetch_station_totime]
    // We order the array by decreasing totime and only return the latest one (first
    // element in the array)
    // NOTE: we can't use the MAX operator since in Postgres NULLs are excluded
    const OBS_PGM_QUERY: &str = "\
        SELECT \
            stationid, \
            paramid, \
            hlevel, \
            nsensor, \
            priority_messageid, \
            (ARRAY_AGG(totime ORDER BY totime DESC NULLS FIRST))[1] \
        FROM obs_pgm \
        GROUP BY stationid, paramid, hlevel, nsensor, priority_messageid \
        HAVING (ARRAY_AGG(totime ORDER BY totime DESC NULLS FIRST))[1] IS NOT NULL";

    let rows = conn.query(OBS_PGM_QUERY, &[]).await?;

    let mut map = ObsPgmTotimeMap::new();
    for row in rows {
        let param_id: i32 = row.get(1);

        let level = row.get(2);
        let level = param_get_level(levels.clone(), param_id, level)?;

        let key = MetTimeseriesKey {
            station_id: row.get(0),
            param_id,
            level,
            sensor: row.get(3),
            type_id: row.get(4),
        };

        let totime: NaiveDateTime = row.get(5);
        map.insert(key, totime.and_utc());
    }

    Ok(map)
}

async fn fetch_obs_pgm_fromtime(
    levels: LevelTable,
    conn: &Client,
) -> Result<ObsPgmFromtimeMap, Error> {
    const OBS_PGM_QUERY: &str = "\
        SELECT \
            stationid, \
            paramid, \
            hlevel, \
            nsensor, \
            priority_messageid, \
            (ARRAY_AGG(fromtime ORDER BY fromtime ASC))[1] \
        FROM obs_pgm \
        GROUP BY stationid, paramid, hlevel, nsensor, priority_messageid \
        HAVING (ARRAY_AGG(fromtime ORDER BY fromtime ASC))[1] IS NOT NULL";

    let rows = conn.query(OBS_PGM_QUERY, &[]).await?;

    let mut map = ObsPgmFromtimeMap::new();
    for row in rows {
        let param_id: i32 = row.get(1);

        let level = row.get(2);
        let level = param_get_level(levels.clone(), param_id, level)?;

        let key = MetTimeseriesKey {
            station_id: row.get(0),
            param_id,
            level,
            sensor: row.get(3),
            type_id: row.get(4),
        };

        let totime: NaiveDateTime = row.get(5);
        map.insert(key, totime.and_utc());
    }

    Ok(map)
}

async fn fetch_station_totime(conn: &Client) -> Result<StationTotimeMap, Error> {
    // The funny looking ARRAY_AGG is needed because each station can have multiple from/to times.
    // For example, the timeseries might have been "reset" after a change of the station position,
    // even though the station ID did not change.
    // We order the aggregated array by decreasing totime and only return the latest one (first
    // element in the array)
    // NOTE: we can't use the MAX operator since in Postgres NULLs are excluded
    const STATION_QUERY: &str = "\
        SELECT \
            stationid, \
            (ARRAY_AGG(totime ORDER BY totime DESC NULLS FIRST))[1] \
        FROM station \
        GROUP BY stationid \
        HAVING (ARRAY_AGG(totime ORDER BY totime DESC NULLS FIRST))[1] IS NOT NULL";

    let rows = conn.query(STATION_QUERY, &[]).await?;

    Ok(rows
        .iter()
        .map(|row| {
            let totime: NaiveDateTime = row.get(1);

            (row.get(0), totime.and_utc())
        })
        .collect())
}

async fn fetch_station_fromtime(conn: &Client) -> Result<StationFromtimeMap, Error> {
    const STATION_QUERY: &str = "\
        SELECT \
            stationid, \
            (ARRAY_AGG(fromtime ORDER BY fromtime ASC))[1] \
        FROM station \
        GROUP BY stationid \
        HAVING (ARRAY_AGG(fromtime ORDER BY fromtime ASC))[1] IS NOT NULL";

    let rows = conn.query(STATION_QUERY, &[]).await?;

    Ok(rows
        .iter()
        .map(|row| {
            let totime: NaiveDateTime = row.get(1);

            (row.get(0), totime.and_utc())
        })
        .collect())
}
