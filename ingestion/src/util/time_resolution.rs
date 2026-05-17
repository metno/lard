use crate::Error;
use chrono::{TimeDelta, Utc};
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

fn pg_interval_to_chrono(interval: Interval) -> TimeDelta {
    let mut total_duration = TimeDelta::zero();
    if interval.months != 0 {
        total_duration += TimeDelta::days(interval.months as i64 * 30);
    }
    total_duration += TimeDelta::days(interval.days as i64);
    total_duration += TimeDelta::microseconds(interval.microseconds);

    total_duration
}

pub async fn find_time_resolution_of_timeseries_recent_or_all(
    conn: &PooledPgConn<'_>,
    ts: i64,
    last_obstime: Option<chrono::DateTime<Utc>>,
    first_guess_resolution: Option<&Interval>,
) -> Result<Vec<(Interval, i64)>, Error> {
    let resolution_results = if let (Some(last_obstime), Some(first_guess_resolution)) =
        (last_obstime, first_guess_resolution)
    {
        // query with time filter, so we only look at recent data
        // TODO: is multiplying by 7 sensible, if daily data would be last 7 days, if hourly would be last 7 hours...
        let resolution_ago = last_obstime - (pg_interval_to_chrono(*first_guess_resolution) * 7);
        // TODO: determine if need to multiply the first_guess_resolution?
        conn.query("WITH data AS (                                                                                                                                                 
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
            LIMIT 3;", &[&ts, &resolution_ago]).await?
    } else {
        // query without time filter, so we look at all data
        // TODO: we also want to look at the offset?
        conn.query("WITH data AS (                                                                                                                                                 
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
) -> Result<Option<chrono::DateTime<Utc>>, Error> {
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

pub async fn determine_time_resolution_of_timeseries(
    conn: &PooledPgConn<'_>,
    ts: i64,
) -> Result<Interval, Error> {
    let last_obstime = last_obstime_ts(conn, ts).await?;
    // if there is a last_obstime then we try to determine the time resolution
    match last_obstime {
        Some(last_obstime) => {
            // find the overall time resolution
            let overall_resolutions =
                find_time_resolution_of_timeseries_recent_or_all(conn, ts, None, None).await?;
            match overall_resolutions.first() {
                // have the overall resolution...
                Some((overall_resolution, overall_frequency)) => {
                    // find the recent resolution
                    let recent_resolutions = find_time_resolution_of_timeseries_recent_or_all(
                        conn,
                        ts,
                        Some(last_obstime),
                        Some(overall_resolution),
                    )
                    .await?;
                    match recent_resolutions.first() {
                        Some((recent_resolution, _)) => {
                            if recent_resolution != overall_resolution {
                                // TODO: add to problems list for CM review
                                warn!(
                                    "Most recent time resolution {recent_resolution:?} for timeseries {ts} does not match overall time resolution {overall_resolution:?}"
                                );
                                Err(Error::Timeresolution("unknown".to_string()))
                            } else {
                                // we can actually decide on the time resolution, unless the spread of resolutions is large
                                let frequency2: Option<i64> =
                                    overall_resolutions.get(1).map(|row| row.1);
                                let frequency3: Option<i64> =
                                    overall_resolutions.get(2).map(|row| row.1);
                                let resolution2: Option<Interval> =
                                    overall_resolutions.get(1).map(|row| row.0);
                                if *overall_frequency
                                    >= 100 * (frequency2.unwrap_or(0) + frequency3.unwrap_or(0))
                                {
                                    Ok(*overall_resolution)
                                } else {
                                    // could just say its unknown, do we want to differentiate?
                                    Err(Error::Timeresolution(
                                        "irregular".to_string()
                                            + &format!(
                                                " (top 2 resolutions in all data: {} {})",
                                                overall_resolution.to_iso_8601(),
                                                resolution2.map_or("unknown".to_string(), |r| r
                                                    .to_iso_8601())
                                            ),
                                    ))
                                }
                            }
                        }
                        // no recent resolution, so we can't determine the time resolution
                        None => Err(Error::Timeresolution("unknown".to_string())),
                    }
                }
                // no overall resolution, so we can't determine the time resolution
                // this probaby won't happen, and maybe means there is no data?
                None => Err(Error::Timeresolution("unknown".to_string())),
            }
        }
        // if there is no last_obstime and therefore no data, then we can't determine the time resolution
        None => Err(Error::Timeresolution("no data".to_string())),
    }
}

async fn refresh_timeresolutions(
    conn: &PooledPgConn<'_>,
) -> Result<
    (
        std::collections::HashMap<i64, String>,
        std::collections::HashMap<i64, String>,
        i32,
    ),
    Error,
> {
    // TODO: deterimine if we need to expand this query to filter out timeseries that we don't want to check
    // for example perhaps we don't want ts with a closed totime that already have a timeresolution?
    let timeseries_rows = conn.query(ALL_TIMESERIES_QUERY, &[]).await?;
    // keep a hashmap of the issues we encounter, so we can log them at the end of the process
    let mut unknown_timeresolution_issues: std::collections::HashMap<i64, String> =
        std::collections::HashMap::new();
    let mut irregular_timeresolution_issues: std::collections::HashMap<i64, String> =
        std::collections::HashMap::new();
    let mut count = 0;

    for x in timeseries_rows {
        let ts_id: i64 = x.get("id");
        let timeresolution = determine_time_resolution_of_timeseries(conn, ts_id).await;
        if let Ok(timeresolution) = timeresolution {
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
            // ignoring those where we assume there is no data...
        }
    }
    Ok((
        unknown_timeresolution_issues,
        irregular_timeresolution_issues,
        count,
    ))
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
                    let (open_unknown_timeresolution_issues, open_irregular_timeresolution_issues, open_count) = refresh_timeresolutions(&open_conn).await?;
                    info!("Finished updating timeresolution in open db");
                    info!("Updated timeresolution for {open_count} timeseries in open db, unknown timeresolution for {} timeseries", open_unknown_timeresolution_issues.len());
                    info!("Irregular timeresolution for {} timeseries", open_irregular_timeresolution_issues.len());
                    info!("Issues encountered for timeseries in open db: {:?}", open_irregular_timeresolution_issues);

                    // update restricted
                    let (restricted_unknown_timeresolution_issues, restricted_irregular_timeresolution_issues, restricted_count) = refresh_timeresolutions(&restricted_conn).await?;
                    info!("Finished updating timeresolution in restricted db");
                    info!("Updated timeresolution for {restricted_count} timeseries in restricted db, unknown timeresolution for {} timeseries", restricted_unknown_timeresolution_issues.len());
                    info!("Irregular timeresolution for {} timeseries", restricted_irregular_timeresolution_issues.len());
                    info!("Issues encountered for timeseries in restricted db: {:?}", restricted_irregular_timeresolution_issues);

                    Ok::<(), Error>(())
                }.await.inspect_err(|err| warn!("failed to refresh timeresolution: {err}"));
            }
        }
    }
}
