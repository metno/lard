use axum::{
    extract::{Query, State},
    http::{header, StatusCode},
    response::IntoResponse,
};

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::PgConnection;
use crate::PgConnectionPool;
use crate::{request_body, ObsChange};

/// Returns the unique name of the product type.
fn name() -> String {
    "SineWave".to_string()
}

/// Returns the description of the product type.
fn description() -> String {
    "A basic sine wave.".to_string()
}

/// Strongly typed representation of the product type input.
#[derive(Debug, Serialize, Deserialize)]
struct SineWaveInput {
    from_time: i64,         // from time (inclusive, UNIX timestamp)
    to_time: i64,           // to time (inclusive, UNIX timestamp)
    time_resolution: usize, // seconds between values
    min_value: f64,         // minimum value
    max_value: f64,         // maximum value
    frequency: f64,         // cycles per second
}

/// Strongly typed representation of the product type output.
#[derive(Debug, Serialize, Deserialize)]
struct SineWaveOutput {
    times: Vec<i64>, // UNIX timestamps
    values: Vec<f64>,
}

/// Returns the input schema for this product type. The body of a POST request to this product type
/// must be a valid instance of this schema.
fn input_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "properties": {
            "from_time": {
                "description": "earliest second",
                "type": "integer"
            },
            "to_time": {
                "description": "latest second",
                "type": "number"
            },
            "time_resolution": {
                "description": "seconds between values",
                "type": "integer",
                "minimum": 1
            },
            "min_value": {
                "description": "minimum value",
                "type": "number"
            },
            "max_value": {
                "description": "maximum value",
                "type": "number"
            },
            "frequency": {
                "description": "cycles per second",
                "type": "number",
                "minimum": 0
            }
        },
        "required": ["min_value"],
        "additionalProperties": false
    })
}

/// Returns the output schema for this product type. The response body will be an instance of this
/// schema.
fn output_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "properties": {
            "times": {
                "type": "array",
                "items": {"type": "integer"}
            },
            "values": {
                "type": "array",
                "items": {"type": "number"}
            }
        },
        "additionalProperties": false
    })
}

/// Returns the currently available input instances for this product type.
/// NOTE: n/a for this product type, but defined here for demonstration purposes.
fn input_instances(db_pool: PgConnectionPool) -> Result<Vec<String>, String> {
    _ = db_pool; // n/a since this product type doesn't access data on external storage
    Ok(vec![])
}

/// Notifies about observation changes that may be relevant to this product type.
/// NOTE: n/a for this product type, but defined here for demonstration purposes.
pub fn handle_obs_changes(db_conn: PgConnection, changes: &[ObsChange]) -> Result<(), String> {
    _ = db_conn; // n/a since this product type doesn't access data on external storage
    _ = changes; // n/a

    // avoid dead code warnings ... TODO: remove once these are used by some other product type
    _ = changes[0].tskey.station_id;
    _ = changes[0].tskey.param_id;
    _ = changes[0].tskey.type_id;
    _ = changes[0].tskey.sensor;
    _ = changes[0].tskey.level;
    _ = changes[0].from_time;
    _ = changes[0].to_time;

    Ok(())
}

/// Notifies about a timer event. NOTE: n/a for this product type, but defined here for
/// demonstration purposes.
pub fn handle_timer_event(db_conn: PgConnection, time: i64) -> Result<(), String> {
    _ = db_conn; // n/a since this product type doesn't access data on external storage
    _ = time; // n/a

    Ok(())
}

#[derive(Debug, Deserialize)]
pub struct SineWaveParams {
    dummy: Option<String>, // n/a since this product type gets its arguments from the request body,
                           // but shown for demonstration purposes
}

/// Retrieves/computes a product of this type. The product is defined by the request body which is
/// assumed to be a valid instance of the input schema.
///
/// On success the returned response body contains an instance of the output schema.
pub async fn product_handler(
    State(db_pool): State<PgConnectionPool>,
    Query(params): Query<SineWaveParams>,
    request: axum::http::Request<axum::body::Body>,
) -> impl IntoResponse {
    _ = db_pool; // n/a since this product type doesn't access data on external storage
    _ = params.dummy; // n/a

    // ensure this is a POST request
    if request.method() != "POST" {
        return (
            StatusCode::BAD_REQUEST,
            [(header::CONTENT_TYPE, "text/plain")],
            format!("expected HTTP POST; found {}", request.method()),
        );
    }

    // get request body
    let input0 = match request_body(request).await {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                [(header::CONTENT_TYPE, "text/plain")],
                format!("request_body() failed: {e}"),
            );
        }
    };

    // decode into strongly typed input
    let input: SineWaveInput = match serde_json::from_str(input0.as_str()) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                [(header::CONTENT_TYPE, "text/plain")],
                format!("failed to deserialize input: {e}"),
            );
        }
    };

    // --- BEGIN validate input ------------------

    if input.from_time >= input.to_time {
        return (
            StatusCode::BAD_REQUEST,
            [(header::CONTENT_TYPE, "text/plain")],
            format!(
                "from_time ({:?}) >= to_time {:?}",
                input.from_time, input.to_time
            ),
        );
    }

    if input.time_resolution < 1 {
        return (
            StatusCode::BAD_REQUEST,
            [(header::CONTENT_TYPE, "text/plain")],
            format!("time_resolution < 1: {:?}", input.time_resolution),
        );
    }

    if input.min_value > input.max_value {
        return (
            StatusCode::BAD_REQUEST,
            [(header::CONTENT_TYPE, "text/plain")],
            format!(
                "min_value ({:?}) > max_value {:?}",
                input.min_value, input.max_value
            ),
        );
    }

    if input.frequency <= 0.0 {
        return (
            StatusCode::BAD_REQUEST,
            [(header::CONTENT_TYPE, "text/plain")],
            format!("frequency <= 0: {:?}", input.frequency),
        );
    }

    // --- END validate input ------------------

    // --- BEGIN compute output -------------------

    let mut output = SineWaveOutput {
        times: vec![],
        values: vec![],
    };

    for t in (input.from_time..input.to_time).step_by(input.time_resolution) {
        output.times.push(t);
        let v0 =
            f64::sin(((t - input.from_time) as f64) * input.frequency * 2.0 * std::f64::consts::PI);
        let v = input.min_value + ((v0 + 1.0) / 2.0) * (input.max_value - input.min_value);
        output.values.push(v);
    }

    // --- END compute output -------------------

    // serialize output
    let ser_output = match serde_json::to_string(&output) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(header::CONTENT_TYPE, "text/plain")],
                format!("failed to serialize output: {e}"),
            );
        }
    };

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        ser_output,
    )
}

/// Gets the availability of this product type.
///
/// On success the returned response body contains a JSON object that contains name, description,
/// input schema, output schema, and available input instances for this product type.
///
/// An input schema instance will contain the available extremes for range fields. A typical
/// example of a range field is 'from_time' which will contain the oldest available time.
pub async fn availability_handler(State(db_pool): State<PgConnectionPool>) -> impl IntoResponse {
    _ = db_pool; // n/a since this product type doesn't access data on external storage

    let input_instances = match input_instances(db_pool) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(header::CONTENT_TYPE, "plain/text")],
                format!(
                    "failed to get available input instances for product type {}: {e}",
                    name(),
                ),
            )
        }
    };

    let pa = json!({
        "type": name(),
        "description": description(),
        "input_schema": input_schema(),
        "output_schema": output_schema(),
        "input_instances": input_instances,
    });

    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        pa.to_string(),
    )
}
