use axum::{
    extract::State,
    http::{header, StatusCode},
    response::IntoResponse,
};
use bb8_postgres::PostgresConnectionManager;
use serde_json::Value;
use tokio_postgres::NoTls;

pub mod types;
use types::sinewave;

type PgConnection<'a> = bb8::PooledConnection<'a, PostgresConnectionManager<NoTls>>;
type PgConnectionPool = bb8::Pool<PostgresConnectionManager<NoTls>>;

/// Identifies a time series.
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
    tskey: TimeSeriesKey,
    from_time: i64, // UNIX timestamp, inclusive
    to_time: i64,   // UNIX timestamp, inclusive
}

/// Notifies about new, updated, or deleted observations in a set of time series.
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

/// Notifies about a timer event.
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
/// On success the returned response body function returns a JSON array that contains the
/// availability information for each supported product type.
///
/// TODO: consider to what extent we need to ensure (trough a trait or something) that each
/// product type provides the availability info in some standard form. This needs to be balanced
/// against the flexibility of expressing availability info in type-specific ways.
/// A standard form could typically be defined as a JSON object that contains name, description,
/// input schema, output schema, and available input instances for the product type. There will be
/// one input instance for each available combination of non-range fields (like 'station ID'), and
/// where each range field in the instance contains the available extreme value (for example a
/// 'from_time' that contains the oldest available time).
pub async fn availability_handler(State(db_pool): State<PgConnectionPool>) -> impl IntoResponse {
    let mut pa_bodies: Vec<String> = vec![];

    // --- BEGIN SineWave ------------------
    let pa = sinewave::availability_handler(axum::extract::State(db_pool))
        .await
        .into_response();

    if pa.status() != StatusCode::OK {
        _ = () // TODO: handle error
    }

    let limit = 2048usize; // TODO: consider if this should be increased or provided via an
                           // environment variable
    let body_bytes = match axum::body::to_bytes(pa.into_body(), limit).await {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(header::CONTENT_TYPE, "text/plain")],
                format!("axum::body::to_bytes() failed: {e} (max size: {limit} bytes)"),
            )
        }
    };

    let body_str = match String::from_utf8(body_bytes.to_vec()) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(header::CONTENT_TYPE, "text/plain")],
                format!("String::from_utf8() failed: {e}"),
            )
        }
    };

    pa_bodies.push(body_str);
    // --- END SineWave ------------------

    // TODO: add more product types

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        format!("[{}]", pa_bodies.join(",")),
    )
}

/// Returns the body of a request as a JSON value, or an error message.
async fn request_body(request: axum::http::Request<axum::body::Body>) -> Result<Value, String> {
    let limit = 2048usize; // TODO: consider if this should be increased or provided via an
                           // environment variable
    let body = request.into_body();
    let bytes = match axum::body::to_bytes(body, limit).await {
        Ok(v) => v,
        Err(e) => {
            return Err(format!(
                "axum::body::to_bytes() failed: {e} (max size: {limit} bytes)"
            ))
        }
    };

    let json_string = match String::from_utf8(bytes.to_vec()) {
        Ok(v) => v,
        Err(e) => return Err(format!("String::from_utf8() failed: {e}")),
    };

    match serde_json::from_str(json_string.as_str()) {
        Ok(v) => Ok(v),
        Err(e) => Err(format!("serde_json::from_str() failed: {e}")),
    }
}

#[cfg(test)]
mod tests {
    // TODO
}
