use crate::util::{Location, PooledPgConn};
use chrono::{DateTime, Utc};
use serde::Serialize;

#[derive(Debug, Serialize)]
pub struct TimesliceElem {
    value: f32,
    station_id: i32,
    loc: Location,
}

// TODO: consider whether this should be object-of-arrays style
#[derive(Debug, Serialize)]
pub struct Timeslice {
    timestamp: DateTime<Utc>,
    element_id: String,
    data: Vec<TimesliceElem>,
}

pub async fn get_timeslice(
    conn: &PooledPgConn<'_>,
    timestamp: DateTime<Utc>,
    element_id: String,
) -> Result<Timeslice, tokio_postgres::Error> {
    let data_results = conn
        .query(
            "SELECT data.obsvalue, filter.station_id, timeseries.loc \
                FROM data \
                    NATURAL JOIN labels.filter \
                    JOIN timeseries ON data.timeseries = timeseries.id \
                WHERE data.obstime = $1 \
                    AND filter.element_id = $2",
            &[&timestamp, &element_id],
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
            element_id,
            data,
        }
    };

    Ok(slice)
}
