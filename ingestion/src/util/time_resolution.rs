use crate::Error;
use pg_interval::Interval;
use tracing::{info, warn};
use util::{DbPools, PooledPgConn};

// TODO: do we care if its deactivated?
// do we want to take into account the from/to time?
const ALL_TIMESERIES_QUERY: &str = "\
    SELECT \
        timeseries.id, \
    FROM timeseries";

const SET_TIMERESOLUTION_QUERY: &str = "\
    UPDATE timeseries \
    SET timeresolution = $1 \
    WHERE id = $2";

pub async fn find_time_resolution_of_timeseries_recent_or_all(
    conn: &PooledPgConn<'_>,
    ts: i64,
    last_obstime: Option<chrono::NaiveDateTime>,
) -> Result<Vec<(Interval, i64)>, Error> {
    let resolution_results = if let Some(last_obstime) = last_obstime {
        // query with time filter, so we only look at the last 7 days of data
        // TODO: determine if 7 days is a good time range?
        conn.query("WITH data AS (                                                                                                                                                 
                SELECT
                    date_trunc('minute', obstime + interval '30 second') as obs_time,
                    LAG(date_trunc('minute', obstime + interval '30 second')) OVER (ORDER BY obstime) as prev_obs_time
                FROM legacy.data
                WHERE timeseries=$1
                AND obstime >= $2 - interval '7 day'
            ),
            gaps AS (
                SELECT
                    (obs_time - prev_obs_time) as resolution
                FROM data
                WHERE prev_obs_time IS NOT NULL
            )
            SELECT
                resolution,
                COUNT(*) as frequency
            FROM gaps
            GROUP BY resolution
            ORDER BY frequency DESC
            LIMIT 3;", &[&ts, &last_obstime]).await?
    } else {
        // query without time filter, so we look at all data
        // TODO: we also want to look at the offset?
        conn.query("WITH data AS (                                                                                                                                                 
                    SELECT
                        date_trunc('minute', obstime + interval '30 second') as obs_time,
                        LAG(date_trunc('minute', obstime + interval '30 second')) OVER (ORDER BY obstime) as prev_obs_time
                    FROM legacy.data
                    WHERE timeseries=$1
                ),
                gaps AS (
                    SELECT
                        (obs_time - prev_obs_time) as resolution
                    FROM data
                    WHERE prev_obs_time IS NOT NULL
                )
                SELECT
                    resolution,
                    COUNT(*) as frequency
                FROM gaps
                GROUP BY resolution
                ORDER BY frequency DESC
                LIMIT 3;", &[&ts]).await?
    };

    let resolutions = resolution_results
        .iter()
        .map(|row| {
            let resolution: Interval = row.get("resolution");
            let frequency: i64 = row.get("frequency");
            (resolution, frequency)
        })
        .collect();
    Ok(resolutions)
}

async fn last_obstime_ts(
    conn: &PooledPgConn<'_>,
    ts: i64,
) -> Result<Option<chrono::NaiveDateTime>, Error> {
    let possible_last_obstime = conn
        .query(
            "SELECT MAX(obstime) \
                    FROM legacy.data \
                    WHERE timeseries = $1",
            &[&ts],
        )
        .await?;
    if let Some(row) = possible_last_obstime.first() {
        let last_obstime: Option<chrono::NaiveDateTime> = row.get(0);
        Ok(last_obstime)
    } else {
        Ok(None)
    }
}

pub async fn determine_time_resolution_of_timeseries(
    conn: &PooledPgConn<'_>,
    ts: i64,
) -> Result<Interval, Error> {
    let last_obstime = last_obstime_ts(conn, ts).await?;
    let resolution_results_recent = if let Some(last_obstime) = last_obstime {
        find_time_resolution_of_timeseries_recent_or_all(conn, ts, Some(last_obstime)).await?
    } else {
        // skip this timeseries, it appears to have no data?
        return Err(Error::Timeresolution("no data".to_string()));
    };
    let resolution_results_all =
        find_time_resolution_of_timeseries_recent_or_all(conn, ts, None).await?;

    if resolution_results_all.is_empty() {
        // Don't know the time resolution, this probably shouldn't happen
        // since we already checked there is data but just in case, we can return unknown
        Err(Error::Timeresolution("unknown".to_string()))
    } else {
        // at least 1, possibly a 2 or 3 (since Limit 3)
        let resolution1: Interval = resolution_results_all[0].0;
        let resolution2: Option<Interval> = resolution_results_all.get(1).map(|row| row.0);

        // check the main resolution is the same in the recent and all data, if not ... its problematic
        // maybe the timeseries needs to be broken up?
        if resolution_results_recent.is_empty() {
            // if there is no data near the last obstime (this is unlikely to happen?)
            // TODO: report in for problem collection?
            return Err(Error::Timeresolution("unknown".to_string()));
        } else if resolution1 != resolution_results_recent[0].0 {
            // if the most common resolution in the recent data is different from the most common resolution in all data, we have a problem
            return Err(Error::Timeresolution(
                "inconsistent".to_string()
                    + &format!(
                        " (most common resolution in recent data: {})",
                        resolution_results_recent[0].0.to_iso_8601()
                            + &format!(
                                " (most common resolution in all data: {})",
                                resolution1.to_iso_8601()
                            ),
                    ),
            ));
        }
        let frequency1: i64 = resolution_results_all[0].1;
        let frequency2: Option<i64> = resolution_results_all.get(1).map(|row| row.1);
        let frequency3: Option<i64> = resolution_results_all.get(2).map(|row| row.1);
        // If the most common resolution is at least 100 times more frequent than the second and third most common,
        // we consider it the main resolution
        if frequency1 >= 100 * (frequency2.unwrap_or(0) + frequency3.unwrap_or(0)) {
            // we also know at this point that it agrees with the resolution in the recent data
            // TODO: check for offset here???
            Ok(resolution1)
        } else {
            // could just say its unknown, do we want to differentiate?
            Err(Error::Timeresolution(
                "irregular".to_string()
                    + &format!(
                        " (top 2 resolutions in all data: {} {})",
                        resolution1.to_iso_8601(),
                        resolution2.map_or("unknown".to_string(), |r| r.to_iso_8601())
                    ),
            ))
        }
    }
}

async fn refresh_timeresolutions(
    conn: &PooledPgConn<'_>,
) -> Result<(std::collections::HashMap<i64, String>, i32), Error> {
    let timeseries_rows = conn.query(ALL_TIMESERIES_QUERY, &[]).await?;
    // keep a hashmap of the issues we encounter, so we can log them at the end of the process
    let mut timeresolution_issues: std::collections::HashMap<i64, String> =
        std::collections::HashMap::new();
    let mut count = 0;

    for x in timeseries_rows {
        let ts_id: i64 = x.get("id");
        let timeresolution = determine_time_resolution_of_timeseries(conn, ts_id).await;
        if let Ok(timeresolution) = timeresolution {
            conn.execute(SET_TIMERESOLUTION_QUERY, &[&timeresolution, &ts_id])
                .await?;
            count += 1;
        } else {
            warn!(
                "Failed to find timeresolution for timeseries {ts_id}, error message: {timeresolution:?}"
            );
            timeresolution_issues.insert(ts_id, format!("{timeresolution:?}"));
        }
    }
    Ok((timeresolution_issues, count))
}

pub async fn refresh_timeresolution_repeatedly(
    pools: DbPools,
    mut refresh_interval: tokio::time::Interval,
    cancel_token: tokio_util::sync::CancellationToken,
) {
    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                break;
            }
            _ = refresh_interval.tick() => {

                info!("Updating timeresolution...");

                let _ = async {
                    let open_conn = pools.open.get().await?;
                    let restricted_conn = pools.restricted.get().await?;
                    // update open
                    let (open_timeresolution_issues, open_count) = refresh_timeresolutions(&open_conn).await?;
                    info!("Finished updating timeresolution in open db");
                    info!("Updated timeresolution for {open_count} timeseries in open db, failed to find timeresolution for {} timeseries", open_timeresolution_issues.len());
                    info!("Issues encountered for timeseries in open db: {:?}", open_timeresolution_issues);

                    // update restricted
                    let (restricted_timeresolution_issues, restricted_count) = refresh_timeresolutions(&restricted_conn).await?;
                    info!("Finished updating timeresolution in restricted db");
                    info!("Updated timeresolution for {restricted_count} timeseries in restricted db, failed to find timeresolution for {} timeseries", restricted_timeresolution_issues.len());
                    info!("Issues encountered for timeseries in restricted db: {:?}", restricted_timeresolution_issues);

                    Ok::<(), Error>(())
                }.await.inspect_err(|err| warn!("failed to refresh timeresolution: {err}"));
            }
        }
    }
}
