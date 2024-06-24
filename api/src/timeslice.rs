use crate::util::{Location, PooledPgConn};
use chrono::{DateTime, Utc};
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct TimesliceElem {
    value: f32,
    station_id: i32,
    // TODO: this shouldn't be an Option, but it avoids panics if location is somehow
    // not found in the database
    loc: Option<Location>,
}

// TODO: consider whether this should be object-of-arrays style
#[derive(Debug, Serialize)]
pub struct Timeslice {
    timestamp: DateTime<Utc>,
    param_id: i32,
    data: Vec<TimesliceElem>,
}

pub async fn get_timeslice(
    conn: &PooledPgConn<'_>,
    timestamp: DateTime<Utc>,
    param_id: i32,
) -> Result<Timeslice, tokio_postgres::Error> {
    let data_results = conn
        .query(
            "SELECT data.obsvalue, met.station_id, timeseries.loc \
                FROM data \
                    NATURAL JOIN labels.met \
                    JOIN timeseries ON data.timeseries = timeseries.id \
                WHERE data.obstime = $1 \
                    AND met.param_id = $2",
            &[&timestamp, &param_id],
        )
        .await?;

    let slice = {
        let mut data = Vec::with_capacity(data_results.len());

        for row in data_results {
            data.push(TimesliceElem {
                value: row.get(0),
                station_id: row.get(1),
                loc: row.get(2),
            });
        }

        Timeslice {
            timestamp,
            param_id,
            data,
        }
    };

    Ok(slice)
}
