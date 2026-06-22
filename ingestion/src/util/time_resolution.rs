use crate::Error;
use chrono::{TimeDelta, Utc};
use pg_interval::Interval;
use std::time::Instant;
use tracing::{info, warn};
use util::{DbPools, PooledPgConn};

// TODO: do we care if a timeseries is deactivated?
// do we want to take into account the from/to time?

// Finds all active timeseries
// that have a timeresolution
const ALL_ACTIVE_TIMESERIES_WITH_TIMERESOLUTION_QUERY: &str = r#"
    SELECT timeseries.id,
        timeseries.timeresolution
    FROM timeseries
    WHERE timeresolution IS NOT NULL
    AND totime is NULL"#;

// Finds all timeseries where timeresolution has not been assessed
const ALL_TIMESERIES_WITHOUT_TIMERESOLUTION_ASSESSED_QUERY: &str = r#"
    SELECT timeseries.id
    FROM timeseries 
    WHERE timeresolution_assessed IS NULL"#;

// Query used to set timeresolution (and timeresolution_assessed to true)
const SET_TIMERESOLUTION_QUERY: &str = r#"
    UPDATE timeseries
    SET timeresolution = $1, timeresolution_assessed = TRUE
    WHERE id = $2"#;

// Query used to set timeresolution_assessed to true
const SET_TIMERESOLUTION_ASSESSED_QUERY: &str = r#"
    UPDATE timeseries
    SET timeresolution_assessed = TRUE
    WHERE id = $1"#;

// NOTE: currently unused since not allowing process to automaticaly unset timeresolution
// query used to set timeresolution to null
//const SET_TIMERESOLUTION_NULL_QUERY: &str = r#"
//    UPDATE timeseries
//    SET timeresolution = NULL
//    WHERE id = $1"#;

// This is a rough conversion from pg_interval to chrono::Duration,
// but since its just used for knowing how many days back approximately to look for data
// when looking for recent timeresolution... it should be sufficient.
fn pg_interval_to_chrono(interval: Interval) -> TimeDelta {
    let mut total_duration = TimeDelta::zero();
    // conversion from months is impresice
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
                COUNT(*) as occurrence
            FROM gaps
            GROUP BY resolution
            ORDER BY occurrence DESC
            LIMIT 3;", &[&ts, &resolution_ago]).await?;

    let resolutions = resolution_results
        .iter()
        .map(|row| {
            let resolution: Interval = row.get("resolution");
            let occurrence: i64 = row.get("occurrence");
            (resolution, occurrence)
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
                    COUNT(*) as occurrence
                FROM gaps
                GROUP BY resolution
                ORDER BY occurrence DESC
                LIMIT 3;", &[&ts]).await?;

    let resolutions = resolution_results
        .iter()
        .map(|row| {
            let resolution: Interval = row.get("resolution");
            let occurrence: i64 = row.get("occurrence");
            (resolution, occurrence)
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
        Some((overall_resolution, overall_occurrence)) => {
            // we can actually decide on the time resolution, unless the spread of resolutions is large
            let occurrence2: Option<i64> = overall_resolutions.get(1).map(|row| row.1);
            let occurrence3: Option<i64> = overall_resolutions.get(2).map(|row| row.1);
            let resolution2: Option<Interval> = overall_resolutions.get(1).map(|row| row.0);
            // check if have enough data to make any conclusion...
            // NOTE: this cutoff should be higher than the one used to choose the length of recent data (100)
            if (*overall_occurrence + occurrence2.unwrap_or(0) + occurrence3.unwrap_or(0)) < 200 {
                return Err(Error::Timeresolution("not enough data".to_string()));
            }
            // the second and third place occurrences must be 2 times less than the first one
            if *overall_occurrence >= 2 * (occurrence2.unwrap_or(0) + occurrence3.unwrap_or(0)) {
                info!(
                    "Overall resolution found for {}: (top 3 resolutions and occurences in all data: {} {} {} {} {} {})",
                    ts,
                    overall_resolution.to_iso_8601(),
                    *overall_occurrence,
                    resolution2.map_or("unknown".to_string(), |r| r.to_iso_8601()),
                    occurrence2.unwrap_or(0),
                    overall_resolutions
                        .get(2)
                        .map_or("unknown".to_string(), |r| r.0.to_iso_8601()),
                    occurrence3.unwrap_or(0),
                );
                Ok(*overall_resolution)
            } else {
                Err(Error::Timeresolution(
                    "undefined".to_string()
                        + &format!(
                            " (top 3 resolutions and occurences in all data: {} {} {} {} {} {})",
                            overall_resolution.to_iso_8601(),
                            overall_occurrence,
                            resolution2.map_or("unknown".to_string(), |r| r.to_iso_8601()),
                            occurrence2.unwrap_or(0),
                            overall_resolutions
                                .get(2)
                                .map_or("unknown".to_string(), |r| r.0.to_iso_8601()),
                            occurrence3.unwrap_or(0),
                        ),
                ))
            }
        }
        // if we don't have a .first() it means no data?
        // no statistics found, so we can't determine the time resolution
        None => Err(Error::Timeresolution("no data".to_string())),
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
                            "Most recent time resolution {} for timeseries {} does not match time resolution {}",
                            recent_resolution.to_iso_8601(),
                            ts,
                            timeresolution.to_iso_8601()
                        );
                        Err(Error::Timeresolution("undefined".to_string()))
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
    // Go over all the timeseries that have no timeresolution assessed
    let timeseries_rows_no_timeresolution = conn
        .query(ALL_TIMESERIES_WITHOUT_TIMERESOLUTION_ASSESSED_QUERY, &[])
        .await?;
    // keep a hashmap of the issues we encounter, so we can log them at the end of the process
    let mut unknown_timeresolution_issues: std::collections::HashMap<i64, String> =
        std::collections::HashMap::new();
    let mut undefined_timeresolution_issues: std::collections::HashMap<i64, String> =
        std::collections::HashMap::new();
    let mut count: i32 = 0;

    for x in timeseries_rows_no_timeresolution {
        let ts_id: i64 = x.get("id");
        let timeresolution = determine_time_resolution_of_timeseries(conn, ts_id).await;
        match timeresolution {
            Ok(timeresolution) => {
                // we also want to check that the recent data is in agreement with the overall timeseries, before setting the timeresolution
                let recent_timeresolution =
                    check_recent_time_resolution_of_timeseries(conn, ts_id, timeresolution).await;

                match recent_timeresolution {
                    Ok(recent_timeresolution) => {
                        // this is in agreement with the overall, so we can set the timeresolution
                        if recent_timeresolution != timeresolution {
                            warn!(
                                "Recent time resolution {} for timeseries {} does not match overall time resolution {}, not setting timeresolution",
                                recent_timeresolution.to_iso_8601(),
                                ts_id,
                                timeresolution.to_iso_8601()
                            );
                            undefined_timeresolution_issues.insert(
                                ts_id,
                                format!(
                                    "Recent {:?} not the same as overall {:?}",
                                    recent_timeresolution.to_iso_8601(),
                                    timeresolution.to_iso_8601()
                                ),
                            );
                            // note that it has been assessed but we could not set it
                            conn.execute(SET_TIMERESOLUTION_ASSESSED_QUERY, &[&ts_id])
                                .await?;
                            continue;
                        } else {
                            // set the timeresolution for the timeseries
                            conn.execute(SET_TIMERESOLUTION_QUERY, &[&timeresolution, &ts_id])
                                .await?;
                        }
                        count += 1;
                    }
                    Err(recent_timeresolution) => {
                        // error case unlikely to occur here, since we have managed to find the overall timeresolution
                        warn!(
                            "Failed to find recent timeresolution for timeseries {ts_id}, error message: {recent_timeresolution:?}, not setting timeresolution"
                        );
                        // filter for unknown timeresolution
                        if recent_timeresolution.to_string().contains("unknown") {
                            unknown_timeresolution_issues
                                .insert(ts_id, format!("{recent_timeresolution:?}"));
                        }
                        // filter to only keep the messages that mention undefined
                        if recent_timeresolution.to_string().contains("undefined") {
                            undefined_timeresolution_issues
                                .insert(ts_id, format!("{recent_timeresolution:?}"));
                        }
                        // NOTE: this case is unlikely to occur since we have managed to find the overall timeresolution
                        // unsure what it would mean, so unsure if we should set it to assessed or not?
                        conn.execute(SET_TIMERESOLUTION_ASSESSED_QUERY, &[&ts_id])
                            .await?;
                    }
                }
            }
            Err(timeresolution) => {
                warn!(
                    "Failed to find timeresolution for timeseries {ts_id}, error message: {timeresolution:?}"
                );
                // filter for unknown timeresolution
                if timeresolution.to_string().contains("unknown") {
                    unknown_timeresolution_issues.insert(ts_id, format!("{timeresolution:?}"));
                }
                // filter to only keep the messages that mention undefined
                if timeresolution.to_string().contains("undefined") {
                    undefined_timeresolution_issues.insert(ts_id, format!("{timeresolution:?}"));
                }
                // NOTE: do we want to say its been assessed?
                if !timeresolution.to_string().contains("data") {
                    // set that it has been assessed as long as the error is not that there is no data or not enough data...
                    conn.execute(SET_TIMERESOLUTION_ASSESSED_QUERY, &[&ts_id])
                        .await?;
                }
            }
        }
    }
    Ok((
        unknown_timeresolution_issues,
        undefined_timeresolution_issues,
        count,
    ))
}

/// This function goes over all the timeseries, and tries to assess just the recent timeresolution.
/// If that resolution is not in agreement with the one set in the db, then it will add a warning to
/// the list of issues it returns. This is for future use in a CMS.
async fn check_recent_timeresolutions(
    conn: &PooledPgConn<'_>,
) -> Result<(std::collections::HashMap<i64, String>, i32), Error> {
    let timeseries_rows = conn
        .query(ALL_ACTIVE_TIMESERIES_WITH_TIMERESOLUTION_QUERY, &[])
        .await?;
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
                    format!(
                        "Recent {} not the same as overall {}",
                        timeresolution.to_iso_8601(),
                        ts_resolution.to_iso_8601()
                    ),
                );
                // NOTE: we are choosing not to set the timeresolution back to null, but just report the issue
                // a CMS should be used to review these errors and reset / fix the timeresolution.
                // conn.execute(SET_TIMERESOLUTION_NULL_QUERY, &[&ts_id])
                //     .await?;
            } else {
                count += 1;
            }
        } else if let Err(timeresolution) = timeresolution {
            warn!(
                "Failed to find timeresolution for timeseries {}, error message: {:?}",
                ts_id, timeresolution
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
                    // set open (on ts that have no existing resolution, and have not been assessed)
                    let start_set_open = Instant::now();
                    let (set_open_unknown_timeresolution_issues, set_open_undefined_timeresolution_issues, set_open_count) = set_timeresolutions(&open_conn).await?;
                    info!("Finished setting timeresolution in open db");
                    let duration_set_open = start_set_open.elapsed();
                    info!("Time elapsed: {:?}", duration_set_open);
                    info!("Set timeresolution for {set_open_count} timeseries in open db, unknown timeresolution for {} timeseries", set_open_unknown_timeresolution_issues.len());
                    info!("Undefined timeresolution for {} timeseries", set_open_undefined_timeresolution_issues.len());

                    // check open timeseries with existing resolution (based on latest data)
                    let start_check_open = Instant::now();
                    let (open_timeresolution_issues, open_count) = check_recent_timeresolutions(&open_conn).await?;
                    info!("Finished checking recent timeresolution in open db");
                    let duration_check_open = start_check_open.elapsed();
                    info!("Time elapsed: {:?}", duration_check_open);
                    info!("Checked timeresolution for {open_count} timeseries in open db, inconsistent timeresolution for {} timeseries", open_timeresolution_issues.len());

                    // set restricted (on ts that have no existing resolution, and have not been assessed)
                    let start_set_restricted = Instant::now();
                    let (set_restricted_unknown_timeresolution_issues, set_restricted_undefined_timeresolution_issues, set_restricted_count) = set_timeresolutions(&restricted_conn).await?;
                    info!("Finished setting timeresolution in restricted db");
                    let duration_set_restricted = start_set_restricted.elapsed();
                    info!("Time elapsed: {:?}", duration_set_restricted);
                    info!("Set timeresolution for {set_restricted_count} timeseries in restricted db, unknown timeresolution for {} timeseries", set_restricted_unknown_timeresolution_issues.len());
                    info!("Undefined timeresolution for {} timeseries", set_restricted_undefined_timeresolution_issues.len());

                    // check restricted timeseries with existing resolution (based on latest data)
                    let start_check_restricted = Instant::now();
                    let (restricted_timeresolution_issues, restricted_count) = check_recent_timeresolutions(&restricted_conn).await?;
                    info!("Finished checking recent timeresolution in restricted db");
                    let duration_check_restricted = start_check_restricted.elapsed();
                    info!("Time elapsed: {:?}", duration_check_restricted);
                    info!("Checked timeresolution for {restricted_count} timeseries in restricted db, inconsistent timeresolution for {} timeseries", restricted_timeresolution_issues.len());

                    Ok::<(), Error>(())
                }.await.inspect_err(|err| warn!("failed to refresh timeresolution: {err}"));
            }
        }
    }
}
