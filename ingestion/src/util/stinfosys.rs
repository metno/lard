use std::collections::HashMap;

use chrono::NaiveDateTime;
use tokio_postgres::{Client, NoTls};
use tracing::error;

use crate::{
    util::levels::{param_get_level, LevelTable},
    Error,
};
use lard_egress::patchwork::OpenTimerange;
use util::{MetLabel, MetTimeseriesKey};

pub struct Stinfosys {
    conn_string: String,
    levels: LevelTable,
}

type StationFromTotimeMap = HashMap<i32, OpenTimerange>;
type ObsPgmFromTotimeMap = HashMap<MetTimeseriesKey, OpenTimerange>;

impl Stinfosys {
    pub fn new(conn_string: String, levels: LevelTable) -> Self {
        Self {
            conn_string,
            levels,
        }
    }

    pub async fn cache_closed_stinfosys(
        &self,
    ) -> Result<
        (
            HashMap<MetTimeseriesKey, OpenTimerange>,
            HashMap<i32, OpenTimerange>,
        ),
        Error,
    > {
        let (client, conn) = tokio_postgres::connect(&self.conn_string, NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = conn.await {
                error!("connection error: {e}");
            }
        });

        // Fetch all closed timeseries in Stinfosys
        let (obs_pgm_times, station_times) = tokio::try_join!(
            fetch_obs_pgm_times(self.levels.clone(), &client),
            fetch_station_times(&client),
        )?;

        Ok((obs_pgm_times, station_times))
    }
}

pub fn calc_from_tos(
    obs_pgm_ranges: &HashMap<MetTimeseriesKey, OpenTimerange>,
    station_ranges: &HashMap<i32, OpenTimerange>,
    data_ranges: HashMap<i64, OpenTimerange>,
    labels: Vec<MetLabel>,
) -> Vec<(i64, OpenTimerange)> {
    labels
        .iter()
        .filter_map(|label| {
            // Prefer obs_pgm if available, and only use station if no obs_pgm info exists
            let stinfo_range = *obs_pgm_ranges
                .get(&label.key)
                .or(station_ranges.get(&label.key.station_id))
                .unwrap_or(&OpenTimerange {
                    from: None,
                    to: None,
                });
            // we `?` this one because if it's None, the ts doesn't exist and we can't update it
            let data = *data_ranges.get(&label.id)?;

            let overlap = stinfo_range.overlap(data);

            // if the metadata for the timeseries has a to_time, we shouldn't close the ts because it might still
            // receive new data
            let should_be_closed = stinfo_range.to.is_some();

            let out = match (overlap, should_be_closed) {
                (Some(overlap), true) => overlap,
                (Some(overlap), false) => OpenTimerange {
                    from: overlap.from,
                    to: None,
                },
                (None, true) => OpenTimerange {
                    from: data.to,
                    to: data.to,
                },
                (None, false) => OpenTimerange {
                    from: stinfo_range.from.max(stinfo_range.from).max(data.from),
                    to: None,
                },
            };

            Some((label.id, out))
        })
        .collect()
}

async fn fetch_obs_pgm_times(
    levels: LevelTable,
    conn: &Client,
) -> Result<ObsPgmFromTotimeMap, Error> {
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
            MIN(fromtime), \
            (ARRAY_AGG(totime ORDER BY totime DESC NULLS FIRST))[1] \
        FROM obs_pgm \
        GROUP BY stationid, paramid, hlevel, nsensor, priority_messageid \
        HAVING (ARRAY_AGG(totime ORDER BY totime DESC NULLS FIRST))[1] IS NOT NULL";

    let rows = conn.query(OBS_PGM_QUERY, &[]).await?;

    let mut map = ObsPgmFromTotimeMap::new();
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

        let fromtime: NaiveDateTime = row.get(5);
        let totime: NaiveDateTime = row.get(6);
        map.insert(
            key,
            OpenTimerange {
                from: Some(fromtime.and_utc()),
                to: Some(totime.and_utc()),
            },
        );
    }

    Ok(map)
}

async fn fetch_station_times(conn: &Client) -> Result<StationFromTotimeMap, Error> {
    // The funny looking ARRAY_AGG is needed because each station can have multiple from/to times.
    // For example, the timeseries might have been "reset" after a change of the station position,
    // even though the station ID did not change.
    // We order the aggregated array by decreasing totime and only return the latest one (first
    // element in the array)
    // NOTE: we can't use the MAX operator since in Postgres NULLs are excluded
    const STATION_QUERY: &str = "\
        SELECT \
            stationid, \
            MIN(fromtime), \
            (ARRAY_AGG(totime ORDER BY totime DESC NULLS FIRST))[1] \
        FROM station \
        GROUP BY stationid \
        HAVING (ARRAY_AGG(totime ORDER BY totime DESC NULLS FIRST))[1] IS NOT NULL";

    let rows = conn.query(STATION_QUERY, &[]).await?;

    Ok(rows
        .iter()
        .map(|row| {
            let fromtime: NaiveDateTime = row.get(1);
            let totime: NaiveDateTime = row.get(2);

            (
                row.get(0),
                OpenTimerange {
                    from: Some(fromtime.and_utc()),
                    to: Some(totime.and_utc()),
                },
            )
        })
        .collect())
}
