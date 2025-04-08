use axum::{extract::State, http::StatusCode, Json};
use bb8_postgres::PostgresConnectionManager;
use serde::{Deserialize, Serialize};
use tokio_postgres::NoTls;

pub mod types;
use types::{obscount, sinewave};

type PgConnection<'a> = bb8::PooledConnection<'a, PostgresConnectionManager<NoTls>>;
type PgConnectionPool = bb8::Pool<PostgresConnectionManager<NoTls>>;

/// Identifies a time series.
#[derive(Debug, Serialize, Deserialize)]
pub struct TimeSeriesKey {
    station_id: i32,
    param_id: i32,
    type_id: i32,
    sensor: Option<i32>,
    level: Option<i32>,
}

impl TimeSeriesKey {
    /// Creates a new TimeSeriesKey instance.
    pub fn new(
        station_id: i32,
        param_id: i32,
        type_id: i32,
        sensor: Option<i32>,
        level: Option<i32>,
    ) -> Self {
        Self {
            station_id,
            param_id,
            type_id,
            sensor,
            level,
        }
    }
}

/// Represents the occurrence of a change to at least one original or updated observations in a
/// time range of a time series. The specific change (i.e. which observations got inserted,
/// updated, or deleted) is for now assumed to be irrelevant.
pub struct ObsChange {
    // TODO: remove below underscore prefixes (that prevent dead code warnings) once these fields
    // are accessed
    _tskey: TimeSeriesKey,
    _from_time: i64, // UNIX timestamp, inclusive
    _to_time: i64,   // UNIX timestamp, inclusive
}

/// Notifies about new, updated, or deleted observations in a set of time series. These events are
/// typically used by product types to update precomputed data on external storage.
pub fn obs_change_notify(
    db_conn: bb8::PooledConnection<'_, PostgresConnectionManager<NoTls>>,
    changes: &[ObsChange],
) -> Result<(), String> {
    // inform product types that make use of this event

    // --- BEGIN SineWave ------------------
    match types::sinewave::handle_obs_changes(db_conn, changes) {
        Ok(_) => (),
        Err(e) => _ = e, // TODO: handle error
    }
    // --- END SineWave ------------------

    // TODO: add more product types for which obs changes are applicable

    Ok(()) // for now
}

/// Notifies about a timer event. Used by product types for running regular tasks, like creating
/// snapshots.
pub fn timer_event_notify(
    db_conn: bb8::PooledConnection<'_, PostgresConnectionManager<NoTls>>,
    time: i64,
) -> Result<(), String> {
    // inform product types that make use of this event

    // --- BEGIN SineWave ------------------
    match sinewave::handle_timer_event(db_conn, time) {
        Ok(_) => (),
        Err(e) => _ = e, // TODO: handle error
    }
    // --- END SineWave ------------------

    // TODO: add more product types for which timer events are applicable

    Ok(()) // for now
}

/// Gets the availability of all product types.
///
/// On success the returned response body function returns a JSON array that contains standard
/// availability information for each supported product type.
pub async fn availability_handler(
    State(db_pool): State<PgConnectionPool>,
) -> Result<Json<Vec<Availability>>, (StatusCode, String)> {
    let mut pas: Vec<Availability> = vec![];

    // SineWave
    match sinewave::availability(db_pool.clone()).await {
        Ok(v) => pas.push(v),
        Err(e) => _ = e, // TODO: handle error
    };

    // ObsCount
    match obscount::availability(db_pool.clone()).await {
        Ok(v) => pas.push(v),
        Err(e) => _ = e, // TODO: handle error
    };

    // TODO: add more product types

    Ok(Json(pas))
}

/// Standard availability for a product type. There will be one input instance for each available
/// combination of non-range fields (like 'station ID'), and where each range field in the instance
/// contains the available extreme value (for example a 'from_time' that contains the oldest
/// available time).
#[derive(Debug, Serialize, Deserialize)]
pub struct Availability {
    name: String,
    description: String,
    input_schema: serde_json::Value,
    output_schema: serde_json::Value,
    input_instances: Vec<String>,
}

#[cfg(test)]
mod tests {
    // TODO
}
