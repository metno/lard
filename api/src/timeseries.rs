use crate::util::{Location, PooledPgConn};
use chrono::{DateTime, Utc};
use serde::Serialize;

// TODO: this should be more comprehensive once the schema supports it
// TODO: figure out what should be wrapped in Option here
#[derive(Debug, Serialize)]
pub struct TimeseriesInfo {
    pub ts_id: i32,
    pub fromtime: DateTime<Utc>,
    pub totime: DateTime<Utc>,
    station_id: i32,
    element_id: String,
    lvl: i32,
    sensor: i32,
    location: Location,
}

#[derive(Debug, Serialize)]
pub struct TimeseriesIrregular {
    header: TimeseriesInfo,
    data: Vec<f32>,
    timestamps: Vec<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
pub struct TimeseriesRegular {
    header: TimeseriesInfo,
    data: Vec<Option<f32>>,
    start_time: DateTime<Utc>,
    time_resolution: String,
}

#[derive(Debug, Serialize)]
#[serde(tag = "regularity")]
pub enum Timeseries {
    Regular(TimeseriesRegular),
    Irregular(TimeseriesIrregular),
}

pub async fn get_timeseries_info(
    conn: &PooledPgConn<'_>,
    station_id: i32,
    element_id: String,
) -> Result<TimeseriesInfo, tokio_postgres::Error> {
    let ts_result = conn
        .query_one(
            "SELECT timeseries.id, \
                COALESCE(timeseries.fromtime, '1950-01-01 00:00:00+00'), \
                COALESCE(timeseries.totime, NOW()::timestamptz), \
                filter.lvl, \
                filter.sensor, \
                timeseries.loc \
                FROM timeseries JOIN labels.filter \
                    ON timeseries.id = filter.timeseries \
                WHERE filter.station_id = $1 AND filter.element_id = $2 \
                LIMIT 1", // TODO: we should probably do something smarter than LIMIT 1
            &[&station_id, &element_id],
        )
        .await?;

    let ts_id: i32 = ts_result.get(0);
    let fromtime: DateTime<Utc> = ts_result.get(1);
    // TODO: there might be a better way to deal with totime than that COALESCE
    let totime: DateTime<Utc> = ts_result.get(2);

    Ok(TimeseriesInfo {
        ts_id,
        fromtime,
        totime,
        station_id,
        element_id,
        lvl: ts_result.get(3),
        sensor: ts_result.get(4),
        location: ts_result.get(5),
    })
}

pub async fn get_timeseries_data_irregular(
    conn: &PooledPgConn<'_>,
    header: TimeseriesInfo,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> Result<TimeseriesIrregular, tokio_postgres::Error> {
    let data_results = conn
        .query(
            "SELECT obsvalue, obstime FROM data \
                WHERE timeseries = $1 \
                    AND obstime BETWEEN $2 AND $3",
            &[&header.ts_id, &start_time, &end_time],
        )
        .await?;

    let ts = {
        let mut data = Vec::with_capacity(data_results.len());
        let mut timestamps = Vec::with_capacity(data_results.len());

        for row in data_results {
            data.push(row.get(0));
            timestamps.push(row.get(1));
        }

        TimeseriesIrregular {
            header,
            data,
            timestamps,
        }
    };

    Ok(ts)
}

pub async fn get_timeseries_data_regular(
    conn: &PooledPgConn<'_>,
    header: TimeseriesInfo,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
    time_resolution: String,
) -> Result<TimeseriesRegular, tokio_postgres::Error> {
    // TODO: string matching intervals like this is a hack, but currently necessary to avoid
    // SQL injection. Ideally we could pass an interval type as a query param, which would
    // also save us the query_string allocation, but no ToSql implementations for intervals
    // currently exist in tokio_postgres, so we need to implement it ourselves.
    let interval = match time_resolution.as_str() {
        "PT1M" => "1 minute",
        "PT1H" => "1 hour",
        "P1D" => "1 day",
        _ => "1 minute", // FIXME: this should error instead of falling back to a default
    };
    let query_string = format!("SELECT data.obsvalue, ts_rule.timestamp \
                FROM (SELECT data.obsvalue, data.obstime FROM data WHERE data.timeseries = $1) as data 
                    RIGHT JOIN generate_series($2::timestamptz, $3::timestamptz, interval '{}') AS ts_rule(timestamp) \
                        ON data.obstime = ts_rule.timestamp", interval);

    let data_results = conn
        .query(
            query_string.as_str(),
            &[&header.ts_id, &start_time, &end_time],
        )
        .await?;

    let ts = {
        let mut data = Vec::with_capacity(data_results.len());

        for row in data_results {
            data.push(row.get(0));
        }

        TimeseriesRegular {
            header,
            data,
            start_time,
            time_resolution,
        }
    };

    Ok(ts)
}
