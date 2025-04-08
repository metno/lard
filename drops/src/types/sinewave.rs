use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::Availability;
use crate::ObsChange;
use crate::PgConnection;
use crate::PgConnectionPool;

/// Returns the unique name of the product type.
fn name() -> String {
    "SineWave".to_string()
}

/// Returns the description of the product type.
fn description() -> String {
    "A demo type that computes a sine wave.".to_string()
}

/// Strongly typed representation of the product type input (see input_schema() for details).
/// The body of a POST request for the product must deserializable to this representation.
#[derive(Debug, Serialize, Deserialize)]
pub struct SineWaveInput {
    from_time: i64,
    to_time: i64,
    time_resolution: usize,
    min_value: f64,
    max_value: f64,
    frequency: f64,
}

// Returns Some(error message) on invalid input, otherwise None.
fn validate_input(input: &SineWaveInput) -> Option<String> {
    if input.from_time >= input.to_time {
        return Some(format!(
            "from_time ({:?}) >= to_time {:?}",
            input.from_time, input.to_time
        ));
    }

    if input.min_value > input.max_value {
        return Some(format!(
            "min_value ({:?}) > max_value {:?}",
            input.min_value, input.max_value
        ));
    }

    None
}

/// Returns the input schema for this product type.
fn input_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "properties": {
            "from_time": {
                "description": "earliest second (UNIX timestamp)",
                "type": "integer"
            },
            "to_time": {
                "description": "latest second (UNIX timestamp)",
                "type": "integer"
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
        "required": [
            "from_time", "to_time", "time_resolution", "min_value", "max_value", "frequency"],
        "additionalProperties": false
    })
}

/// Strongly typed representation of the product type output.
#[derive(Debug, Serialize, Deserialize)]
pub struct SineWaveOutput {
    times: Vec<i64>, // UNIX timestamps
    values: Vec<f64>,
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
        "required": ["times", "values"],
        "additionalProperties": false
    })
}

// Computes the product from input into output. Returns Some(error message) on error, otherwise
// None.
fn compute_product(
    db_pool: PgConnectionPool,
    input: &SineWaveInput,
    output: &mut SineWaveOutput,
) -> Option<String> {
    _ = db_pool; // n/a since this product type doesn't access data on external storage

    for t in (input.from_time..input.to_time).step_by(input.time_resolution) {
        output.times.push(t);
        let v0 =
            f64::sin(((t - input.from_time) as f64) * input.frequency * 2.0 * std::f64::consts::PI);
        let v = input.min_value + ((v0 + 1.0) / 2.0) * (input.max_value - input.min_value);
        output.values.push(v);
    }

    None
}

/// Returns the currently available input instances for this product type.
/// NOTE: n/a for this product type, but defined here for demonstration.
fn input_instances(db_pool: PgConnectionPool) -> Result<Vec<String>, String> {
    _ = db_pool; // n/a
    Ok(vec![])
}

/// Notifies about observation changes that may be relevant to this product type.
/// NOTE: n/a for this product type, but defined here for demonstration.
pub fn handle_obs_changes(db_conn: PgConnection, changes: &[ObsChange]) -> Result<(), String> {
    _ = db_conn; // n/a
    _ = changes; // n/a

    Ok(())
}

/// Notifies about a timer event. NOTE: n/a for this product type, but defined here for
/// demonstration.
pub fn handle_timer_event(db_conn: PgConnection, time: i64) -> Result<(), String> {
    _ = db_conn; // n/a
    _ = time; // n/a

    Ok(())
}

/// Endpoint handler that retrieves/computes a product of this type. The product is defined by the
/// request body which is assumed to be a valid instance of the input schema.
///
/// On success the returned response body contains an instance of the output schema.
pub async fn product_handler(
    State(db_pool): State<PgConnectionPool>,
    Json(input): Json<SineWaveInput>,
) -> Result<Json<SineWaveOutput>, (StatusCode, String)> {
    if let Some(e) = validate_input(&input) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("validate_input() failed: {e}"),
        ));
    }

    let mut output = SineWaveOutput {
        times: vec![],
        values: vec![],
    };

    if let Some(e) = compute_product(db_pool, &input, &mut output) {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("compute_product() failed: {e}"),
        ));
    }

    Ok(Json(output))
}

/// Gets the availability of this product type.
///
/// On success the function returns standard availability information for this product type.
pub async fn availability(db_pool: PgConnectionPool) -> Result<Availability, (StatusCode, String)> {
    let input_instances = match input_instances(db_pool) {
        Ok(v) => v,
        Err(e) => {
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!(
                    "failed to get available input instances for product type {}: {e}",
                    name(),
                ),
            ))
        }
    };

    Ok(Availability {
        name: name(),
        description: description(),
        input_schema: input_schema(),
        output_schema: output_schema(),
        input_instances,
    })
}

/// Endpoint handler that wraps around 'availability()'.
///
/// On success the returned response body contains a JSON object with standard availability
/// information for this product type.
pub async fn availability_handler(
    State(db_pool): State<PgConnectionPool>,
) -> Result<Json<Availability>, (StatusCode, String)> {
    match availability(db_pool).await {
        Ok(v) => Ok(Json(v)),
        Err(e) => Err(e),
    }
}
