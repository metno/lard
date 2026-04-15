use pg_interval::Interval;
use thiserror::Error;
use util::PooledPgConn;

#[derive(Error, Debug)]
pub enum TimeResolutionError {
    #[error("Database error: {0}")]
    DbError(#[from] tokio_postgres::Error),
}

pub async fn find_timeresolution_of_timeseries(
    conn: &PooledPgConn<'_>,
    ts: i64,
) -> Result<String, TimeResolutionError> {
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
        Ok(resolution.to_iso_8601())
    } else if resolution_results.len() <= 2 {
        let resolution1: Interval = resolution_results[0].get("resolution");
        let frequency1: i64 = resolution_results[0].get("frequency");
        let frequency2: i64 = resolution_results[1].get("frequency");
        let frequency3: Option<i64> = resolution_results.get(2).map(|row| row.get("frequency"));

        // If the most common resolution is at least 100 times more frequent than the second and third most common, we consider it the main resolution
        if frequency1 >= 100 * (frequency2 + frequency3.unwrap_or(0)) {
            Ok(resolution1.to_iso_8601())
        } else {
            Ok("variable".to_string())
        }
    } else {
        // Else we don't know the timeresolution
        Ok("unknown".to_string())
    }
}
