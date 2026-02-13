use chrono::NaiveDateTime;
use futures::future;
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::HashMap;
use tokio::time::Instant;
use tokio_postgres::{Client, NoTls};
use tracing::{error, info, warn};

use crate::{
    stinfofacade::{
        level::{param_get_level, LevelTable},
        param::ParamTables,
        Error,
    },
    DbPools, MetLabel, MetTimeseriesKey, OpenTimerange, PooledPgConn, FROM_TO_FUTURES_FAILURES,
};

// TODO: we're defining these aliases but still mostly directly using the
//       underlying type?
type StationFromTotimeMap = HashMap<i32, OpenTimerange>;
type ObsPgmFromTotimeMap = HashMap<MetTimeseriesKey, OpenTimerange>;

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

/// Fetches the timerange for each ts where data actually exists in the db
async fn fetch_timeranges_data(
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

/// Obs_pgm stands for observation program. This contains information about what is expected to send data.
/// We use it to determine if a timeseries should have a to_time (i.e. is closed) or not.
async fn fetch_timeranges_obs_pgm(
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
            sensor, \
            message_formatid, \
            MIN(fromtime), \
            (ARRAY_AGG(totime ORDER BY totime DESC NULLS FIRST))[1] \
        FROM obspgm_h2 \
        GROUP BY stationid, paramid, hlevel, sensor, message_formatid";

    let rows = conn.query(OBS_PGM_QUERY, &[]).await?;

    let mut map = ObsPgmFromTotimeMap::new();
    for row in rows {
        let param_id: i32 = row.get(1);
        let station_id: i32 = row.get(0);

        let initial_level = row.get(2);
        let level = param_get_level(levels.clone(), param_id, initial_level)?;
        if level.is_none() && initial_level != 0 {
            // skip since this level could not be converted (likely since we had no scale)
            info!("Skipping obspgm_h2 entry for station {}, param {} since level {} could not be converted",
                station_id,
                param_id,
                initial_level,
            );
            continue;
        }

        let key = MetTimeseriesKey {
            station_id,
            param_id,
            level,
            sensor: row.get(3),
            type_id: row.get(4),
        };

        let fromtime: NaiveDateTime = row.get(5);
        let totime: Option<NaiveDateTime> = row.get(6);
        map.insert(
            key,
            OpenTimerange {
                from: Some(fromtime.and_utc()),
                to: totime.map(|t| t.and_utc()),
            },
        );
    }

    Ok(map)
}

/// This is metadata about when a station existed. If the obs_pgm does not have information
/// about a timeseries, we fall back to this information.
async fn fetch_timeranges_station(conn: &Client) -> Result<StationFromTotimeMap, Error> {
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
        GROUP BY stationid";

    let rows = conn.query(STATION_QUERY, &[]).await?;

    Ok(rows
        .iter()
        .map(|row| {
            let fromtime: NaiveDateTime = row.get(1);
            let totime: Option<NaiveDateTime> = row.get(2);
            (
                row.get(0),
                OpenTimerange {
                    from: Some(fromtime.and_utc()),
                    to: totime.map(|t| t.and_utc()),
                },
            )
        })
        .collect())
}

/// Fetch timerange restrictions from obspgm and station
// TODO: is there actually much point in keeping this function?
pub async fn fetch_timeranges_stinfosys(
    stinfo_conn_string: &str,
    levels: LevelTable,
) -> Result<
    (
        HashMap<MetTimeseriesKey, OpenTimerange>,
        HashMap<i32, OpenTimerange>,
    ),
    Error,
> {
    let (client, conn) = tokio_postgres::connect(stinfo_conn_string, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = conn.await {
            error!("connection error: {e}");
        }
    });

    // Fetch all closed timeseries in Stinfosys
    let (obs_pgm_times, station_times) = tokio::try_join!(
        fetch_timeranges_obs_pgm(levels.clone(), &client),
        fetch_timeranges_station(&client),
    )?;

    Ok((obs_pgm_times, station_times))
}

/// merge the timeranges restrictions from stinfosys and the range where data
/// is present in the db, into a single timerange (the overlap-ish between the
/// inputs) that we can then put into public.timeseries's from and to time
pub fn merge_timeranges(
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

            // if the metadata for the timeseries has no to_time, we shouldn't close the ts because it might still
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

    let ts_from_to = fetch_timeranges_data(conn, labels.clone(), params).await?;

    let closed = merge_timeranges(obs_pgm_times, station_times, ts_from_to, labels);
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

pub async fn refresh_from_to_repeatedly(
    stinfo_conn_string: Option<&str>,
    levels: LevelTable,
    params: ParamTables,
    pools: DbPools,
    mut refresh_interval: tokio::time::Interval,
    cancel_token: tokio_util::sync::CancellationToken,
) {
    if let Some(stinfo_conn_string) = stinfo_conn_string {
        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    break;
                }
                _ = refresh_interval.tick() => {

                    info!("Updating timeseries fromtime & totime");

                    let _ = async {
                        let mut open_conn = pools.open.get().await?;
                        let mut restricted_conn = pools.restricted.get().await?;

                        info!("Caching closed stations and observation programs from StInfoSys");
                        let (obs_pgm_times_map, station_times_map) =
                            fetch_timeranges_stinfosys(stinfo_conn_string, levels.clone())
                                .await?;

                        info!("Updating open and restricted database timeseries");
                        let (open_res, restricted_res) = tokio::join!(
                            update_from_to(
                                &mut open_conn,
                                &obs_pgm_times_map,
                                &station_times_map,
                                params.clone()
                            ),
                            update_from_to(
                                &mut restricted_conn,
                                &obs_pgm_times_map,
                                &station_times_map,
                                params.clone()
                            ),
                        );
                        open_res?;
                        restricted_res?;

                        Ok::<(), Error>(())
                    }.await.inspect_err(|err| warn!("failed to refresh from_to_times: {err}"));
                }
            }
        }
    }
}
