use chrono::NaiveDateTime;
use futures::future;
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::time::Instant;
use tokio_postgres::{Client, NoTls};
use tracing::{error, info, warn};

use crate::{
    DbPools, FROM_TO_FUTURES_FAILURES, MetLabel, MetTimeseriesKey, OpenTimerange, PooledPgConn,
    REFRESH_FROM_TO_DURATION_SECONDS,
    stinfofacade::{
        Error,
        level::{LevelTable, param_get_level},
        param::ParamTables,
    },
};

// TODO: we're defining these aliases but still mostly directly using the
//       underlying type?
type StationFromTotimeMap = HashMap<i32, OpenTimerange>;
type ObsPgmFromTotimeMap = HashMap<MetTimeseriesKey, OpenTimerange>;

// TODO: remove the WHERE when we remove/prevent NULL param IDs in the table
const TIMESERIES_QUERY: &str = "\
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
    problems: ArcObsPgmProblemCollector,
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
                    label,
                    conn.query_one(MAX_MIN_TIMESERIES_LEGACY_DATA_QUERY, &[&label.id])
                        .await,
                )
            } else {
                // nonscalar
                (
                    label,
                    conn.query_one(MAX_MIN_TIMESERIES_NONSCALAR_DATA_QUERY, &[&label.id])
                        .await,
                )
            }
        })
        .collect::<FuturesUnordered<_>>()
        .enumerate();

    while let Some((_, res)) = futures_ts_from_to.next().await {
        match res {
            (label, Ok(val)) => {
                ts_from_to.insert(label.id, OpenTimerange::new(val.get(0), val.get(1)))
            }
            (label, Err(_err)) => {
                // log these fails
                metrics::counter!(FROM_TO_FUTURES_FAILURES).increment(1);
                problems.write()?.scalar_nonscalar_inconsistency(label);

                // NOTE: due to issue with scalar vs nonscalar data, we cannot realiably get the timeseries max and min.
                // for now we if the call fails the time range will be None, None
                ts_from_to.insert(label.id, OpenTimerange::new(None, None))
            }
        };
    }
    Ok(ts_from_to)
}

pub trait ObsPgmProblemCollector: Send + Sync {
    fn missing_stinfo_obspgm_h2_level(&mut self, key: &MetTimeseriesKey, initial_level: i32);
    fn scalar_nonscalar_inconsistency(&mut self, label: &MetLabel);
    fn missing_timeseries(&mut self, label: &MetLabel);
    fn unknown_in_stinfo_obspgm(&mut self, label: &MetLabel);
    fn unknown_in_stinfo_station(&mut self, label: &MetLabel);
    fn stinfo_data_timerange_mismatch(
        &mut self,
        label: &MetLabel,
        stinfo_range: &OpenTimerange,
        data_range: &OpenTimerange,
    );

    /// Finished with update run, report aggregated problems.
    // FIXME This should be async.
    fn report(&mut self);
}

type ArcObsPgmProblemCollector = Arc<RwLock<Box<dyn ObsPgmProblemCollector>>>;

#[derive(Default)]
pub struct LogObsPgmProblemCollector {
    _count: usize,
}

impl LogObsPgmProblemCollector {
    pub fn new() -> Self {
        Self { _count: 0 }
    }
}

impl ObsPgmProblemCollector for LogObsPgmProblemCollector {
    fn missing_stinfo_obspgm_h2_level(&mut self, key: &MetTimeseriesKey, initial_level: i32) {
        info!(
            "Skipping obspgm_h2 entry for key={key:?} as initial_level={initial_level} could not be converted"
        );
    }

    fn scalar_nonscalar_inconsistency(&mut self, _label: &MetLabel) {
        // very noisy
        // error!(
        //     "max min for timeseries future failed: {}, for tsid: {}",
        //     err, label.id
        // );
    }

    fn missing_timeseries(&mut self, _label: &MetLabel) {}

    fn unknown_in_stinfo_obspgm(&mut self, _label: &MetLabel) {}

    fn unknown_in_stinfo_station(&mut self, _label: &MetLabel) {}

    fn stinfo_data_timerange_mismatch(
        &mut self,
        _label: &MetLabel,
        _stinfo_range: &OpenTimerange,
        _data_range: &OpenTimerange,
    ) {
    }

    fn report(&mut self) {}
}

/// Obs_pgm stands for observation program. This contains information about what is expected to send data.
/// We use it to determine if a timeseries should have a to_time (i.e. is closed) or not.
async fn fetch_timeranges_obs_pgm(
    levels: LevelTable,
    conn: &Client,
    problems: ArcObsPgmProblemCollector,
) -> Result<ObsPgmFromTotimeMap, Error> {
    // The funny looking ARRAY_AGG is needed because each timeseries can have multiple from/to times.
    // Most likely related to the fact that stations in the `station` tables can also have
    // multiple entries, see [fetch_timeranges_station]
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
        let station_id: i32 = row.get(0);
        let param_id: i32 = row.get(1);
        let initial_level = row.get(2);
        let level = param_get_level(levels.clone(), param_id, initial_level)?;
        let key = MetTimeseriesKey {
            station_id,
            param_id,
            level,
            sensor: row.get(3),
            type_id: row.get(4),
        };

        if level.is_none() && initial_level != 0 {
            // skip since this level could not be converted (likely since we had no scale)
            problems
                .write()?
                .missing_stinfo_obspgm_h2_level(&key, initial_level);
            continue;
        }

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
            let station_id: i32 = row.get(0);
            let fromtime: NaiveDateTime = row.get(1);
            let totime: Option<NaiveDateTime> = row.get(2);
            (
                station_id,
                OpenTimerange {
                    from: Some(fromtime.and_utc()),
                    to: totime.map(|t| t.and_utc()),
                },
            )
        })
        .collect())
}

/// Fetch timerange restrictions from obspgm and station
pub async fn fetch_timeranges_stinfosys(
    stinfo_conn_string: &str,
    levels: LevelTable,
    problems: ArcObsPgmProblemCollector,
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
        fetch_timeranges_obs_pgm(levels.clone(), &client, problems.clone()),
        fetch_timeranges_station(&client),
    )?;

    Ok((obs_pgm_times, station_times))
}

/// Guess if there could be a mismatch between timerange from
/// stinfosys/obs_pgm and the timerange from data actually
/// found in the database.
fn guess_stinfo_data_timerange_mismatch(stinfo: &OpenTimerange, data: &OpenTimerange) -> bool {
    // If stinfo range is None, None, there is no stinfo entry
    // for this timeseries.
    if stinfo.from.is_none() && stinfo.to.is_none() {
        return true;
    }

    let Some(sfrom) = stinfo.from else {
        // It should be known when a station was set up.
        return true;
    };

    // Timerange from data is always closed if observations exist.
    let Some((dfrom, dto)) = data.from.zip(data.to) else {
        return true;
    };

    // Observations found before the station went online.
    if dfrom < sfrom {
        return true;
    }

    // Observations found after the station went offline.
    if let Some(sto) = stinfo.to
        && dto > sto
    {
        return true;
    }

    false
}

/// merge the timeranges restrictions from stinfosys and the range where data
/// is present in the db, into a single timerange (the overlap-ish between the
/// inputs) that we can then put into public.timeseries's from and to time
pub fn merge_timeranges(
    obs_pgm_ranges: &HashMap<MetTimeseriesKey, OpenTimerange>,
    station_ranges: &HashMap<i32, OpenTimerange>,
    data_ranges: HashMap<i64, OpenTimerange>,
    labels: Vec<MetLabel>,
    problems: ArcObsPgmProblemCollector,
) -> Vec<(i64, OpenTimerange)> {
    labels
        .iter()
        .filter_map(|label| {
            // Prefer obs_pgm if available, and only use station if no obs_pgm info exists
            let stinfo_range = *obs_pgm_ranges
                .get(&label.key)
                .or_else(|| {
                    if let Ok(mut p) = problems.write() {
                        p.unknown_in_stinfo_obspgm(label);
                    }
                    station_ranges.get(&label.key.station_id)
                })
                .unwrap_or_else(|| {
                    if let Ok(mut p) = problems.write() {
                        p.unknown_in_stinfo_station(label);
                    }
                    &OpenTimerange {
                        from: None,
                        to: None,
                    }
                });

            // if `data` is None, the ts doesn't exist and we can't update it
            let Some(data) = data_ranges.get(&label.id) else {
                if let Ok(mut p) = problems.write() {
                    p.missing_timeseries(label);
                }
                return None;
            };

            if guess_stinfo_data_timerange_mismatch(&stinfo_range, data)
                && let Ok(mut p) = problems.write()
            {
                p.stinfo_data_timerange_mismatch(label, &stinfo_range, data);
            }

            let overlap = stinfo_range.overlap(*data);

            // if the metadata for the timeseries has no to_time, we shouldn't close the ts because it might still
            // receive new data
            let should_be_closed = stinfo_range.to.is_some();

            let out = match (overlap, should_be_closed) {
                // base case
                (Some(overlap), true) => overlap,
                // explicitly leave to_time open, since it shouldn't be closed
                (Some(overlap), false) => OpenTimerange {
                    from: overlap.from,
                    to: None,
                },
                // there's no overlap, so no valid timerange we can issue data for,
                // so we collapse it such that no data is marked as available.
                // note that the timerange is end-exclusive, so even data at
                // exactly `data.to` will not be issued.
                (None, true) => OpenTimerange {
                    from: data.to,
                    to: data.to,
                },
                // like the above, except since it shouldn't be closed, there might
                // come data in the future that is valid to serve. we set the strictest
                // `from` we can so all current data is excluded, and leave `to` open
                (None, false) => OpenTimerange {
                    from: stinfo_range.from.max(data.from),
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
    problems: ArcObsPgmProblemCollector,
    cancel_token: tokio_util::sync::CancellationToken,
) -> Result<(), Error> {
    let now = Instant::now();

    let rows = conn.query(TIMESERIES_QUERY, &[]).await?;

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

    let ts_from_to = tokio::select! {
        _ = cancel_token.cancelled() => {
            return Err(Error::Cancelled);
        }
        ts_from_to = fetch_timeranges_data(conn, labels.clone(), params, problems.clone()) => ts_from_to,
    }?;

    let closed = merge_timeranges(obs_pgm_times, station_times, ts_from_to, labels, problems);
    info!("Updating from/to for {} timeseries (.len())", closed.len());

    let tx = conn.transaction().await?;

    // Explicitly take the lock so we can prevent concurrent access to the rows we are going to update
    tx.execute(
        "LOCK TABLE public.timeseries IN SHARE ROW EXCLUSIVE MODE",
        &[],
    )
    .await?;

    tokio::select! {
        _ = cancel_token.cancelled() => {
            // tx is implicitly rolled back by being dropped uncommitted
            Err(Error::Cancelled)
        }
        _ = future::join_all(closed.into_iter().map(async |(tsid, timerange)| {
            match tx
                .execute(UPDATE_QUERY, &[&timerange.from, &timerange.to, &tsid])
                .await
            {
                Ok(_) => (), //info!("Tsid {} updated", tsid),
                Err(err) => error!("Could not update tsid {}: {}", tsid, err),
            }
        })) => {
            tx.commit().await?;
            info!(
                "Finished updating from/to for timeseries {:.2?}",
                now.elapsed()
            );

            Ok(())
        }
    }
}

async fn refresh_from_to_once(
    stinfo_conn_string: &str,
    levels: LevelTable,
    params: ParamTables,
    pools: DbPools,
    problems: ArcObsPgmProblemCollector,
    cancel_token: tokio_util::sync::CancellationToken,
) -> Result<(), Error> {
    info!("Updating timeseries fromtime & totime");
    let mut open_conn = pools.open.get().await?;
    let mut restricted_conn = pools.restricted.get().await?;

    info!("Caching closed stations and observation programs from StInfoSys");
    let (obs_pgm_times_map, station_times_map) =
        fetch_timeranges_stinfosys(stinfo_conn_string, levels.clone(), problems.clone()).await?;

    info!("Updating open and restricted database timeseries");
    let (open_res, restricted_res) = tokio::join!(
        update_from_to(
            &mut open_conn,
            &obs_pgm_times_map,
            &station_times_map,
            params.clone(),
            problems.clone(),
            cancel_token.clone()
        ),
        update_from_to(
            &mut restricted_conn,
            &obs_pgm_times_map,
            &station_times_map,
            params.clone(),
            problems.clone(),
            cancel_token.clone()
        ),
    );
    open_res?;
    restricted_res?;

    Ok(())
}

async fn refresh_from_to_once_with_metrics(
    stinfo_conn_string: &str,
    levels: LevelTable,
    params: ParamTables,
    pools: DbPools,
    cancel_token: tokio_util::sync::CancellationToken,
) {
    let problems = LogObsPgmProblemCollector::default();
    let problems: Box<dyn ObsPgmProblemCollector> = Box::new(problems);
    let problems = Arc::new(RwLock::new(problems));

    let start = Instant::now();
    let refresh_result = refresh_from_to_once(
        stinfo_conn_string,
        levels,
        params,
        pools,
        problems.clone(),
        cancel_token,
    )
    .await;
    let duration = start.elapsed().as_secs_f64();

    match refresh_result {
        Ok(_) => metrics::histogram!(REFRESH_FROM_TO_DURATION_SECONDS, "status" => "success")
            .record(duration),
        Err(err) => {
            metrics::histogram!(REFRESH_FROM_TO_DURATION_SECONDS, "status" => "failure")
                .record(duration);
            warn!("failed to refresh from_to_times: {err}")
        }
    }

    if let Ok(mut p) = problems.write() {
        p.report();
    };
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
                    refresh_from_to_once_with_metrics(
                        stinfo_conn_string,
                        levels.clone(),
                        params.clone(),
                        pools.clone(),
                        cancel_token.clone()
                    ).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, TimeZone, Utc};

    use super::*;

    fn t(year: i32, month: u32, day: u32) -> Option<DateTime<Utc>> {
        Utc.with_ymd_and_hms(year, month, day, 0, 0, 0).single()
    }

    #[test]
    fn test_guess_stinfo_data_timerange_mismatch() {
        assert!(guess_stinfo_data_timerange_mismatch(
            &OpenTimerange {
                from: None,
                to: None
            },
            &OpenTimerange {
                from: t(2026, 1, 1),
                to: t(2026, 2, 1),
            }
        ));
        assert!(guess_stinfo_data_timerange_mismatch(
            &OpenTimerange {
                from: t(2026, 1, 15),
                to: None
            },
            &OpenTimerange {
                from: t(2026, 1, 1),
                to: t(2026, 2, 1),
            }
        ));
        assert!(guess_stinfo_data_timerange_mismatch(
            &OpenTimerange {
                from: t(2026, 1, 1),
                to: t(2026, 2, 1)
            },
            &OpenTimerange {
                from: t(2026, 1, 1),
                to: t(2026, 2, 15),
            }
        ));

        assert!(!guess_stinfo_data_timerange_mismatch(
            &OpenTimerange {
                from: t(2026, 1, 1),
                to: None
            },
            &OpenTimerange {
                from: t(2026, 1, 1),
                to: t(2026, 2, 1),
            }
        ));
    }
}
