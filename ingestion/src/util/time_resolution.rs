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

pub async fn find_timeresolution_of_timeseries(
    conn: &PooledPgConn<'_>,
    ts: i64,
) -> Result<Interval, Error> {
    let resolution_results = conn.query("WITH data AS (                                                                                                                                                 
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
            LIMIT 3;", &[&ts]).await?;

    if resolution_results.is_empty() {
        // Don't know the time resolution
        Err(Error::Timeresolution("unknown".to_string()))
    } else if resolution_results.len() == 1 {
        // There is only one resolution, so its the timeresolution of the timeseries
        let resolution: Interval = resolution_results[0].get("resolution");
        Ok(resolution)
    } else {
        // at least 2, possibly a 3rd (since Limit 3)
        let resolution1: Interval = resolution_results[0].get("resolution");
        let resolution2: Interval = resolution_results[1].get("resolution");
        let frequency1: i64 = resolution_results[0].get("frequency");
        let frequency2: i64 = resolution_results[1].get("frequency");
        let frequency3: Option<i64> = resolution_results.get(2).map(|row| row.get("frequency"));

        // If the most common resolution is at least 100 times more frequent than the second and third most common,
        // we consider it the main resolution
        if frequency1 >= 100 * (frequency2 + frequency3.unwrap_or(0)) {
            Ok(resolution1)
        } else {
            // could just say its unknown, do we want to differentiate?
            Err(Error::Timeresolution(
                "irregular".to_string()
                    + &format!(
                        " (top 2 resolutions: {} {})",
                        resolution1.to_iso_8601(),
                        resolution2.to_iso_8601()
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
        let timeresolution = find_timeresolution_of_timeseries(conn, ts_id).await;
        if let Ok(timeresolution) = timeresolution {
            conn.execute(SET_TIMERESOLUTION_QUERY, &[&timeresolution, &ts_id])
                .await?;
            count += 1;
        } else {
            warn!(
                "Failed to find timeresolution for timeseries {ts_id} in open db: {timeresolution:?}"
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
