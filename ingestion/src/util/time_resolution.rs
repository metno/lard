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

    if resolution_results.len() == 1 {
        // There is only one resolution, so its the timeresolution of the timeseries
        let resolution: Interval = resolution_results[0].get("resolution");
        Ok(resolution)
    } else if resolution_results.len() <= 2 {
        let resolution: Interval = resolution_results[0].get("resolution");
        let frequency1: i64 = resolution_results[0].get("frequency");
        let frequency2: i64 = resolution_results[1].get("frequency");
        let frequency3: Option<i64> = resolution_results.get(2).map(|row| row.get("frequency"));

        // If the most common resolution is at least 100 times more frequent than the second and third most common, we consider it the main resolution
        if frequency1 >= 100 * (frequency2 + frequency3.unwrap_or(0)) {
            Ok(resolution)
        } else {
            // could just say its unknown, do we want to differentiate?
            Err(Error::Timeresolution("irregular".to_string()))
        }
    } else {
        // Else we don't know the timeresolution
        Err(Error::Timeresolution("unknown".to_string()))
    }
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
                    let open_timeseries_rows = open_conn.query(ALL_TIMESERIES_QUERY, &[]).await?;
                    for x in open_timeseries_rows {
                        let ts_id: i64 = x.get("id");
                        let timeresolution = find_timeresolution_of_timeseries(&open_conn, ts_id).await;
                        if let Ok(timeresolution) = timeresolution {
                            open_conn.execute(SET_TIMERESOLUTION_QUERY, &[&timeresolution, &ts_id]).await?;
                        } else {
                            warn!("Failed to find timeresolution for timeseries {ts_id} in open db: {timeresolution:?}");
                        }
                    }
                    info!("Finished updating timeresolution in open db");

                    let restricted_timeseries_rows = restricted_conn.query(ALL_TIMESERIES_QUERY, &[]).await?;
                    for x in restricted_timeseries_rows {
                        let ts_id: i64 = x.get("id");
                        let timeresolution = find_timeresolution_of_timeseries(&restricted_conn, ts_id).await;
                        if let Ok(timeresolution) = timeresolution {
                            restricted_conn.execute(SET_TIMERESOLUTION_QUERY, &[&timeresolution, &ts_id]).await?;
                        } else {
                            warn!("Failed to find timeresolution for timeseries {ts_id} in restricted db: {timeresolution:?}");
                        }
                    }
                    info!("Finished updating timeresolution in restricted db");

                    Ok::<(), Error>(())
                }.await.inspect_err(|err| warn!("failed to refresh timeresolution: {err}"));
            }
        }
    }
}
