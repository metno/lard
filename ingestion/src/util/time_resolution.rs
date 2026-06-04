use crate::Error;
use chrono::{TimeDelta, Utc};
use pg_interval::Interval;
use tracing::{info, warn};
use util::{DbPools, PooledPgConn};

// TODO: do we care if a timeseries is deactivated?
// do we want to take into account the from/to time?

// Finds all timeseries
const ALL_TIMESERIES_QUERY: &str = r#"
    SELECT timeseries.id,
        timeseries.timeresolution
    FROM timeseries"#;

// Finds all timeseries were there is no timeresolution already set
// and they are active (as in have no totime set)
const ALL_ACTIVE_TIMESERIES_WITHOUT_TIMERESOLUTION_QUERY: &str = r#"
    SELECT timeseries.id
    FROM timeseries 
    WHERE timeresolution IS NULL
    AND totime IS NULL"#;

// Query used to set timeresolution
const SET_TIMERESOLUTION_QUERY: &str = r#"
    UPDATE timeseries
    SET timeresolution = $1
    WHERE id = $2"#;

// query used to set timeresolution to null
const SET_TIMERESOLUTION_NULL_QUERY: &str = r#"
    UPDATE timeseries
    SET timeresolution = NULL
    WHERE id = $1"#;

fn pg_interval_to_chrono(interval: Interval) -> TimeDelta {
    let mut total_duration = TimeDelta::zero();
    if interval.months != 0 {
        total_duration += TimeDelta::days(interval.months as i64 * 30);
    }
    total_duration += TimeDelta::days(interval.days as i64);
    total_duration += TimeDelta::microseconds(interval.microseconds);

    total_duration
}

async fn last_obstime_ts(
    conn: &PooledPgConn<'_>,
    ts: i64,
) -> Result<Option<chrono::DateTime<Utc>>, Error> {
    // get the last obstime of the timeseries
    // will error if nothing found from the db
    let possible_last_obstime = conn
        .query_one(
            "SELECT MAX(obstime)
                    FROM legacy.data
                    WHERE timeseries = $1;",
            &[&ts],
        )
        .await?;
    let last_obstime: Option<chrono::DateTime<Utc>> = possible_last_obstime.get(0);
    Ok(last_obstime)
}

/// This function takes the ts id as well as the most recent obstime and tries to find the
/// resolution of the recent part of the timeseries by examining the most frequent gap size between observation times
pub async fn find_time_resolution_of_timeseries_recent(
    conn: &PooledPgConn<'_>,
    ts: i64,
    first_guess_resolution: &Interval,
    last_obstime: chrono::DateTime<Utc>,
) -> Result<Vec<(Interval, i64)>, Error> {
    // TODO: is multiplying by 100 sensible, if daily data would be last 100 days, if hourly would be last 100 hours...
    let resolution_ago = last_obstime - (pg_interval_to_chrono(*first_guess_resolution) * 100);
    // query with time filter, so we only look at recent data (need last obstime)
    let resolution_results = conn.query("WITH data AS (                                                                                                                                                 
                SELECT
                    obstime as obs_time,
                    LAG(obstime) OVER (ORDER BY obstime) as prev_obs_time
                FROM legacy.data
                WHERE timeseries=$1
                AND obstime >= $2
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
            LIMIT 3;", &[&ts, &resolution_ago]).await?;

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

/// This function takes the ts id and tries to find the resolution of the whole timeseries
/// by examining the most frequent gap size between observation times
pub async fn find_time_resolution_of_timeseries_all(
    conn: &PooledPgConn<'_>,
    ts: i64,
) -> Result<Vec<(Interval, i64)>, Error> {
    // query without time filter, so we look at all data
    // TODO: we also want to look at the offset?
    let resolution_results = conn.query("WITH data AS (                                                                                                                                                 
                    SELECT
                        obstime as obs_time,
                        LAG(obstime) OVER (ORDER BY obstime) as prev_obs_time
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
                LIMIT 3;", &[&ts]).await?;

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

/// Checks the overall timeresolution of a timeseries
pub async fn determine_time_resolution_of_timeseries(
    conn: &PooledPgConn<'_>,
    ts: i64,
) -> Result<Interval, Error> {
    let overall_resolutions = find_time_resolution_of_timeseries_all(conn, ts).await?;
    match overall_resolutions.first() {
        // have the overall resolution...
        Some((overall_resolution, overall_frequency)) => {
            // we can actually decide on the time resolution, unless the spread of resolutions is large
            let frequency2: Option<i64> = overall_resolutions.get(1).map(|row| row.1);
            let frequency3: Option<i64> = overall_resolutions.get(2).map(|row| row.1);
            let resolution2: Option<Interval> = overall_resolutions.get(1).map(|row| row.0);
            // the second and third place frequencies must be 10 times less than the first one
            if *overall_frequency >= 10 * (frequency2.unwrap_or(0) + frequency3.unwrap_or(0)) {
                Ok(*overall_resolution)
            } else {
                // could just say its unknown, do we want to differentiate?
                Err(Error::Timeresolution(
                    "irregular".to_string()
                        + &format!(
                            " (top 2 resolutions in all data: {} {})",
                            overall_resolution.to_iso_8601(),
                            resolution2.map_or("unknown".to_string(), |r| r.to_iso_8601())
                        ),
                ))
            }
        }
        // if we don't have a .first() it means no data?
        // no statistics found, so we can't determine the time resolution
        None => Err(Error::Timeresolution("unknown".to_string())),
    }
}

/// Checks the timeresolution of only the recent data for a timeseries
pub async fn check_recent_time_resolution_of_timeseries(
    conn: &PooledPgConn<'_>,
    ts: i64,
    timeresolution: Interval,
) -> Result<Interval, Error> {
    let last_obstime = last_obstime_ts(conn, ts).await?;
    match last_obstime {
        // if there is a last_obstime then we try to determine the time resolution
        Some(last_obstime) => {
            // compare the recent timeresolution to the overall / old one
            let recent_resolutions =
                find_time_resolution_of_timeseries_recent(conn, ts, &timeresolution, last_obstime)
                    .await?;
            match recent_resolutions.first() {
                Some((recent_resolution, _)) => {
                    if *recent_resolution != timeresolution {
                        // TODO: add to problems list for CM review
                        warn!(
                            "Most recent time resolution {recent_resolution:?} for timeseries {ts} does not match time resolution {timeresolution:?}"
                        );
                        Err(Error::Timeresolution("unknown".to_string()))
                    } else {
                        // this is in agreement with the overall, so do nothing
                        Ok(timeresolution)
                    }
                }
                // no recent resolution, so we can't determine the time resolution
                // this should probably not happen if there is data? (would get to outer error)
                None => Err(Error::Timeresolution("unknown".to_string())),
            }
        }
        // if there is no last_obstime and therefore no data, then we can't determine the time resolution
        None => Err(Error::Timeresolution("no data".to_string())),
    }
}

/// This function goes over all the timeseries that have no set timeresolution, and tries to find them
/// from the overall timeseries. It sets them if it can determine the timeresolution.
/// The function keeps track of the errors it receives about why the timeresolution could not be determined.
async fn set_timeresolutions(
    conn: &PooledPgConn<'_>,
) -> Result<
    (
        std::collections::HashMap<i64, String>,
        std::collections::HashMap<i64, String>,
        i32,
    ),
    Error,
> {
    // Go over all the timeseries that have no resolution set at all
    let timeseries_rows_no_timeresolution = conn
        .query(ALL_ACTIVE_TIMESERIES_WITHOUT_TIMERESOLUTION_QUERY, &[])
        .await?;
    // keep a hashmap of the issues we encounter, so we can log them at the end of the process
    let mut unknown_timeresolution_issues: std::collections::HashMap<i64, String> =
        std::collections::HashMap::new();
    let mut irregular_timeresolution_issues: std::collections::HashMap<i64, String> =
        std::collections::HashMap::new();
    let mut count: i32 = 0;

    for x in timeseries_rows_no_timeresolution {
        let ts_id: i64 = x.get("id");
        let timeresolution = determine_time_resolution_of_timeseries(conn, ts_id).await;
        if let Ok(timeresolution) = timeresolution {
            // set the timeresolution for the timeseries
            conn.execute(SET_TIMERESOLUTION_QUERY, &[&timeresolution, &ts_id])
                .await?;
            count += 1;
        } else if let Err(timeresolution) = timeresolution {
            warn!(
                "Failed to find timeresolution for timeseries {ts_id}, error message: {timeresolution:?}"
            );
            // filter for unknown timeresolution
            if timeresolution.to_string().contains("unknown") {
                unknown_timeresolution_issues.insert(ts_id, format!("{timeresolution:?}"));
            }
            // filter to only keep the messages that mention irregular
            if timeresolution.to_string().contains("irregular") {
                irregular_timeresolution_issues.insert(ts_id, format!("{timeresolution:?}"));
            }
        }
    }
    Ok((
        unknown_timeresolution_issues,
        irregular_timeresolution_issues,
        count,
    ))
}

/// This function goes over all the timeseries, and tries to assess just the recent timeresolution.
/// If that resolution is not in agreement with the one set in the db, then it will set timeresolution
/// back to NULL and add it to the issues list returned from the function.
async fn refresh_timeresolutions(
    conn: &PooledPgConn<'_>,
) -> Result<(std::collections::HashMap<i64, String>, i32), Error> {
    let timeseries_rows = conn.query(ALL_TIMESERIES_QUERY, &[]).await?;
    // keep a hashmap of the issues we encounter, so we can log them at the end of the process
    let mut timeresolution_issues: std::collections::HashMap<i64, String> =
        std::collections::HashMap::new();
    let mut count: i32 = 0;

    for x in timeseries_rows {
        let ts_id: i64 = x.get("id");
        let ts_resolution: Interval = x.get("timeresolution");
        let timeresolution =
            check_recent_time_resolution_of_timeseries(conn, ts_id, ts_resolution).await;
        if let Ok(timeresolution) = timeresolution {
            // unset the timeresolution if does not agree with recent
            if ts_resolution != timeresolution {
                // keep information in the issues hashmap
                timeresolution_issues.insert(
                    ts_id,
                    format!("Recent {timeresolution:?} not the same as overall {ts_resolution:?}"),
                );
                // set timeresolution to NULL
                conn.execute(SET_TIMERESOLUTION_NULL_QUERY, &[&ts_id])
                    .await?;
            } else {
                count += 1;
            }
        } else if let Err(timeresolution) = timeresolution {
            warn!(
                "Failed to find timeresolution for timeseries {ts_id}, error message: {timeresolution:?}"
            );
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
                    // set open (on ts that have no existing resolution)
                    let (set_open_unknown_timeresolution_issues, set_open_irregular_timeresolution_issues, set_open_count) = set_timeresolutions(&open_conn).await?;
                    info!("Finished setting timeresolution in open db");
                    info!("Set timeresolution for {set_open_count} timeseries in open db, unknown timeresolution for {} timeseries", set_open_unknown_timeresolution_issues.len());
                    info!("Irregular timeresolution for {} timeseries", set_open_irregular_timeresolution_issues.len());
                    info!("Issues encountered for timeseries in open db: {:?}", set_open_irregular_timeresolution_issues);

                    // update open (based on latest data)
                    let (open_timeresolution_issues, open_count) = refresh_timeresolutions(&open_conn).await?;
                    info!("Finished refreshing timeresolution in open db");
                    info!("Checked timeresolution for {open_count} timeseries in open db, inconsistent timeresolution for {} timeseries", open_timeresolution_issues.len());
                    info!("Issues encountered for timeseries in open db: {:?}", open_timeresolution_issues);

                    // set restricted (on ts that have no existing resolution)
                    let (set_restricted_unknown_timeresolution_issues, set_restricted_irregular_timeresolution_issues, set_restricted_count) = set_timeresolutions(&restricted_conn).await?;
                    info!("Finished setting timeresolution in open db");
                    info!("Set timeresolution for {set_restricted_count} timeseries in open db, unknown timeresolution for {} timeseries", set_restricted_unknown_timeresolution_issues.len());
                    info!("Irregular timeresolution for {} timeseries", set_restricted_irregular_timeresolution_issues.len());
                    info!("Issues encountered for timeseries in open db: {:?}", set_restricted_irregular_timeresolution_issues);

                    // update restricted (based on latest data)
                    let (restricted_timeresolution_issues, restricted_count) = refresh_timeresolutions(&restricted_conn).await?;
                    info!("Finished refreshing timeresolution in restricted db");
                    info!("Checked timeresolution for {restricted_count} timeseries in restricted db, inconsistent timeresolution for {} timeseries", restricted_timeresolution_issues.len());
                    info!("Issues encountered for timeseries in restricted db: {:?}", restricted_timeresolution_issues);

                    Ok::<(), Error>(())
                }.await.inspect_err(|err| warn!("failed to refresh timeresolution: {err}"));
            }
        }
    }
}
