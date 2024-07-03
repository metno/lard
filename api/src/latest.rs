use crate::util::{Location, PooledPgConn};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct LatestElem {
    value: f32,
    timestamp: DateTime<Utc>,
    station_id: i32,
    loc: Option<Location>,
}

pub async fn get_latest(
    conn: &PooledPgConn<'_>,
    latest_max_age: DateTime<Utc>,
) -> Result<Vec<LatestElem>, tokio_postgres::Error> {
    let data_results = conn
        .query(
            "SELECT data.obsvalue, data.obstime, met.station_id, timeseries.loc \
                FROM (SELECT DISTINCT ON (timeseries) timeseries, obstime, obsvalue \
                        FROM data \
                            WHERE obstime > $1 \
                            ORDER BY timeseries, obstime DESC) \
                    AS data
                NATURAL JOIN labels.met \
                JOIN timeseries ON data.timeseries = timeseries.id",
            &[&latest_max_age],
        )
        .await?;

    let data = {
        let mut data = Vec::with_capacity(data_results.len());

        for row in data_results {
            data.push(LatestElem {
                value: row.get(0),
                timestamp: row.get(1),
                station_id: row.get(2),
                loc: row.get(3),
            });
        }

        data
    };

    Ok(data)
}
