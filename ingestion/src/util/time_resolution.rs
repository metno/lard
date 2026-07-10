use crate::Error;
use chrono::Utc;
use chronoutil::RelativeDuration;
use pg_interval::Interval;
use std::time::Instant;
use thiserror::Error as ThisError;
use tracing::{info, warn};
use util::{DbPools, PooledPgConn};

// define some constants used for fudge factors in cutoffs and thresholds for determining time resolution
const MIN_OCCURENCE_POINTS: i64 = 200;
const RECENT_DATA_POINTS: i32 = 100;
const WIN_OCCURENCE_FACTOR: i64 = 2;

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
    WHERE timeresolution_assessed IS FALSE"#;

// Query used to set timeresolution (and timeresolution_assessed to true)
const SET_TIMERESOLUTION_QUERY: &str = r#"
    UPDATE timeseries
    SET timeresolution = $1, timeresolution_assessed = TRUE
    WHERE id = $2"#;

// NOTE: currently unused since not allowing process to automaticaly unset timeresolution
// query used to set timeresolution to null
//const SET_TIMERESOLUTION_NULL_QUERY: &str = r#"
//    UPDATE timeseries
//    SET timeresolution = NULL
//    WHERE id = $1"#;

#[derive(Debug, ThisError)]
pub enum TimeResolutionError {
    #[error("timeseries did not have enough data to determine a timeresolution")]
    NotEnoughData,
    #[error(
        "timeseries does not have a clear mode of timeresolution. Top 3 with occurence counts: {0:?}"
    )]
    Unclear(Box<([Option<Interval>; 3], [i64; 3])>),
    #[error("Most recent time resolution does not match overall time resolution")]
    Mismatch((Interval, Interval)),
    #[error("postgres returned an error: {0}")]
    Database(#[from] tokio_postgres::Error),
}

// Convert from pg_interval::Interval to chronoutil::RelativeDuration
// This is used for knowing how many days back approximately to look for data
// when checking the recent time resolution of a timeseries
fn pg_interval_to_chrono(interval: Interval) -> RelativeDuration {
    RelativeDuration::months(interval.months)
        + RelativeDuration::days(interval.days as i64)
        + RelativeDuration::microseconds(interval.microseconds)
}

async fn last_obstime_ts(
    conn: &PooledPgConn<'_>,
    ts: i64,
) -> Result<Option<chrono::DateTime<Utc>>, tokio_postgres::Error> {
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
    Ok(possible_last_obstime.get(0))
}

/// This function is used to find the time resolution of the most recent part of a timeseries
/// backwards from the most recent obstime, using a the assumed resolution to determine how far back to look.
pub async fn find_time_resolution_of_timeseries_recent(
    conn: &PooledPgConn<'_>,
    ts: i64,
    first_guess_resolution: &Interval,
    last_obstime: chrono::DateTime<Utc>,
) -> Result<([Option<Interval>; 3], [i64; 3]), tokio_postgres::Error> {
    // NOTE: if daily data would be last 100 days, if hourly would be last 100 hours...
    let resolution_ago =
        last_obstime - (pg_interval_to_chrono(*first_guess_resolution) * RECENT_DATA_POINTS);
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

    let mut resolutions_array: [Option<Interval>; 3] = [None, None, None];
    let mut occurrences_array: [i64; 3] = [0, 0, 0]; // default to 0 here

    for (i, row) in resolution_results.iter().enumerate() {
        if i >= 3 {
            break;
        }
        resolutions_array[i] = Some(row.get("resolution"));
        occurrences_array[i] = row.get("occurrence");
    }

    Ok((resolutions_array, occurrences_array))
}

/// This function takes the ts id and tries to find the resolution of the whole timeseries
/// by examining the most frequent gap size between observation times
pub async fn find_time_resolution_of_timeseries_all(
    conn: &PooledPgConn<'_>,
    ts: i64,
) -> Result<([Option<Interval>; 3], [i64; 3]), tokio_postgres::Error> {
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

    let mut resolutions_array: [Option<Interval>; 3] = [None, None, None];
    let mut occurrences_array: [i64; 3] = [0, 0, 0]; // default to 0 here

    for (i, row) in resolution_results.iter().enumerate() {
        if i >= 3 {
            break;
        }
        resolutions_array[i] = Some(row.get("resolution"));
        occurrences_array[i] = row.get("occurrence");
    }

    Ok((resolutions_array, occurrences_array))
}

/// Checks the overall timeresolution of a timeseries
pub async fn determine_time_resolution_of_timeseries(
    conn: &PooledPgConn<'_>,
    ts: i64,
) -> Result<Interval, TimeResolutionError> {
    let (resolutions, occurrences) = find_time_resolution_of_timeseries_all(conn, ts).await?;

    // check if there is enough data to determine the time resolution
    if occurrences.iter().sum::<i64>() < MIN_OCCURENCE_POINTS {
        return Err(TimeResolutionError::NotEnoughData);
    }

    // check if the most common resolution is significantly more common than the next two
    if occurrences[0] >= WIN_OCCURENCE_FACTOR * (occurrences[1] + occurrences[2]) {
        Ok(resolutions[0].unwrap())
    } else {
        // could not determine the resolution, so return an error with the top 3 resolutions and occurrences for logging
        Err(TimeResolutionError::Unclear(Box::new((
            resolutions,
            occurrences,
        ))))
    }
}

/// Checks the timeresolution of only the recent data for a timeseries
pub async fn check_recent_time_resolution_of_timeseries(
    conn: &PooledPgConn<'_>,
    ts: i64,
    timeresolution: Interval,
) -> Result<Interval, TimeResolutionError> {
    let last_obstime = last_obstime_ts(conn, ts)
        .await?
        .ok_or(TimeResolutionError::NotEnoughData)?;

    let (resolutions, _occurrences) =
        find_time_resolution_of_timeseries_recent(conn, ts, &timeresolution, last_obstime).await?;

    // no recent resolution means only 1 data point in the window
    let recent_resolution = resolutions[0].ok_or(TimeResolutionError::NotEnoughData)?;

    if recent_resolution != timeresolution {
        // TODO: add to problems list for CM review
        warn!(
            "Most recent time resolution {} for timeseries {} does not match time resolution {}",
            recent_resolution.to_iso_8601(),
            ts,
            timeresolution.to_iso_8601()
        );
        Err(TimeResolutionError::Mismatch((
            timeresolution,
            recent_resolution,
        )))
    } else {
        Ok(timeresolution)
    }
}

async fn check_timeresolution(
    conn: &PooledPgConn<'_>,
    ts: i64,
) -> Result<Interval, TimeResolutionError> {
    let timeresolution = determine_time_resolution_of_timeseries(conn, ts).await?;
    // we also want to check that the recent data is in agreement with the overall timeseries, before setting the timeresolution
    check_recent_time_resolution_of_timeseries(conn, ts, timeresolution).await
}

/// This function goes over all the timeseries that have no set timeresolution, and tries to find them
/// from the overall timeseries. It sets them if it can determine the timeresolution.
/// The function keeps track of the errors it receives about why the timeresolution could not be determined.
async fn set_timeresolutions(
    conn: &PooledPgConn<'_>,
) -> Result<
    (
        std::collections::HashMap<i64, ([Option<Interval>; 3], [i64; 3])>,
        std::collections::HashMap<i64, (Interval, Interval)>,
        i32,
    ),
    TimeResolutionError,
> {
    // Go over all the timeseries that have no timeresolution assessed
    let timeseries_rows_no_timeresolution = conn
        .query(ALL_TIMESERIES_WITHOUT_TIMERESOLUTION_ASSESSED_QUERY, &[])
        .await?;
    // keep a hashmap of the issues we encounter, so we can log them at the end of the process
    let mut unclear_timeresolution_issues: std::collections::HashMap<
        i64,
        ([Option<Interval>; 3], [i64; 3]),
    > = std::collections::HashMap::new();
    let mut mismatched_timeresolution_issues: std::collections::HashMap<i64, (Interval, Interval)> =
        std::collections::HashMap::new();
    let mut count: i32 = 0;

    for ts_id in timeseries_rows_no_timeresolution
        .into_iter()
        .map(|row| row.get("id"))
    {
        match check_timeresolution(conn, ts_id).await {
            Ok(timeresolution) => {
                // set the timeresolution for the timeseries
                conn.execute(SET_TIMERESOLUTION_QUERY, &[&timeresolution, &ts_id])
                    .await?;
                count += 1;
            }
            Err(error) => {
                // most likely the error was "unclear" or "mismatched", then it gets marked as assessed, but not set
                // if it was not enough data or a database error then it will not be marked as assessed
                let should_mark_assessed = match error {
                    TimeResolutionError::NotEnoughData => false,
                    TimeResolutionError::Unclear(candidates) => {
                        unclear_timeresolution_issues.insert(ts_id, *candidates);
                        true
                    }
                    TimeResolutionError::Mismatch((expected, found)) => {
                        mismatched_timeresolution_issues.insert(ts_id, (expected, found));
                        true
                    }
                    // unlike the others, this should be considered an application failure, and so
                    // forwarded to the caller
                    TimeResolutionError::Database(e) => {
                        return Err(TimeResolutionError::Database(e));
                    }
                };

                if should_mark_assessed {
                    conn.execute(SET_TIMERESOLUTION_QUERY, &[&None::<Interval>, &ts_id])
                        .await?;
                }
            }
        }
    }
    Ok((
        unclear_timeresolution_issues,
        mismatched_timeresolution_issues,
        count,
    ))
}

/// This function goes over all the timeseries, and tries to assess just the recent timeresolution.
/// If that resolution is not in agreement with the one set in the db, then it will add a warning to
/// the list of issues it returns. This is for future use in a CMS.
async fn check_recent_timeresolutions(
    conn: &PooledPgConn<'_>,
) -> Result<(std::collections::HashMap<i64, String>, i32), TimeResolutionError> {
    let timeseries_rows = conn
        .query(ALL_ACTIVE_TIMESERIES_WITH_TIMERESOLUTION_QUERY, &[])
        .await?;
    // keep a hashmap of the issues we encounter, so we can log them at the end of the process
    let mut timeresolution_issues: std::collections::HashMap<i64, String> =
        std::collections::HashMap::new();
    let mut count: i32 = 0;

    for (ts_id, ts_resolution) in timeseries_rows
        .into_iter()
        .map(|row| (row.get("id"), row.get("timeresolution")))
    {
        match check_recent_time_resolution_of_timeseries(conn, ts_id, ts_resolution).await {
            Ok(_) => {
                count += 1;
            }
            Err(error) => {
                if let TimeResolutionError::Mismatch((expected, found)) = error {
                    // add to hashmap of issues
                    timeresolution_issues.insert(
                        ts_id,
                        format!("Expected: {:?}, Found: {:?}", expected, found),
                    );
                }
            }
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
                    let (set_open_unclear_timeresolution_issues, set_open_mismatched_timeresolution_issues, set_open_count) = set_timeresolutions(&open_conn).await?;
                    info!("Finished setting timeresolution in open db");
                    let duration_set_open = start_set_open.elapsed();
                    info!("Time elapsed: {:?}", duration_set_open);
                    info!("Set timeresolution for {set_open_count} timeseries in open db, unclear timeresolution for {} timeseries", set_open_unclear_timeresolution_issues.len());
                    info!("Mismatched timeresolution for {} timeseries", set_open_mismatched_timeresolution_issues.len());

                    // check open timeseries with existing resolution (based on latest data)
                    let start_check_open = Instant::now();
                    let (open_timeresolution_issues, open_count) = check_recent_timeresolutions(&open_conn).await?;
                    info!("Finished checking recent timeresolution in open db");
                    let duration_check_open = start_check_open.elapsed();
                    info!("Time elapsed: {:?}", duration_check_open);
                    info!("Checked timeresolution for {open_count} timeseries in open db, inconsistent timeresolution for {} timeseries", open_timeresolution_issues.len());

                    // set restricted (on ts that have no existing resolution, and have not been assessed)
                    let start_set_restricted = Instant::now();
                    let (set_restricted_unclear_timeresolution_issues, set_restricted_mismatched_timeresolution_issues, set_restricted_count) = set_timeresolutions(&restricted_conn).await?;
                    info!("Finished setting timeresolution in restricted db");
                    let duration_set_restricted = start_set_restricted.elapsed();
                    info!("Time elapsed: {:?}", duration_set_restricted);
                    info!("Set timeresolution for {set_restricted_count} timeseries in restricted db, unclear timeresolution for {} timeseries", set_restricted_unclear_timeresolution_issues.len());
                    info!("Mismatched timeresolution for {} timeseries", set_restricted_mismatched_timeresolution_issues.len());

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
