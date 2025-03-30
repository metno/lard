use axum::http::StatusCode;
use bb8_postgres::PostgresConnectionManager;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::Arc;
use tokio_postgres::NoTls;

use crate::ObsChange;

/// Just an empty struct since no state needs to be kept for this product type.
pub struct SineWave {
    // TODO: db_pool is n/a for this product type, but declared here for demonstration.
    // Remove field once actual use can be demonstrated in another product type.
    db_pool: bb8::Pool<PostgresConnectionManager<NoTls>>,
}

pub fn new(
    db_pool: bb8::Pool<PostgresConnectionManager<NoTls>>,
) -> Arc<dyn crate::operator::Operator + Send + Sync> {
    let sine_wave = SineWave { db_pool };
    Arc::new(sine_wave)
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

impl crate::operator::Operator for SineWave {
    fn name(&self) -> String {
        "SineWave".to_string()
    }

    fn description(&self) -> String {
        "A basic sine wave.".to_string()
    }

    fn input_schema(&self) -> serde_json::Value {
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

    fn output_schema(&self) -> serde_json::Value {
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

    fn input_instances(&self) -> Result<Vec<String>, (StatusCode, String)> {
        _ = self.db_pool; // n/a since this product type doesn't access data on external storage
        Ok(vec![])
    }

    fn handle_obs_changes(&self, changes: &[ObsChange]) -> Result<(), String> {
        _ = self.db_pool; // n/a since this product type doesn't access data on external storage
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

    fn handle_timer_event(&self, time: i64) -> Result<(), String> {
        _ = self.db_pool; // n/a since this product type doesn't access data on external storage
        _ = time; // n/a

        Ok(())
    }

    fn product(&self, input0: serde_json::Value) -> Result<String, (StatusCode, String)> {
        _ = self.db_pool; // n/a since this product type doesn't access data on external storage

        // deserialize input
        let input: SineWaveInput = match serde_json::from_value(input0) {
            Ok(v) => v,
            Err(e) => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!("failed to deserialize input: {e}"),
                ))
            }
        };

        // --- BEGIN validate input ------------------

        if input.from_time >= input.to_time {
            return Err((
                StatusCode::BAD_REQUEST,
                format!(
                    "from_time ({:?}) >= to_time {:?}",
                    input.from_time, input.to_time
                ),
            ));
        }

        if input.time_resolution < 1 {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("time_resolution < 1: {:?}", input.time_resolution),
            ));
        }

        if input.min_value > input.max_value {
            return Err((
                StatusCode::BAD_REQUEST,
                format!(
                    "min_value ({:?}) > max_value {:?}",
                    input.min_value, input.max_value
                ),
            ));
        }

        if input.frequency <= 0.0 {
            return Err((
                StatusCode::BAD_REQUEST,
                format!("frequency <= 0: {:?}", input.frequency),
            ));
        }

        // --- END validate input ------------------

        // --- BEGIN compute output -------------------

        let mut output = SineWaveOutput {
            times: vec![],
            values: vec![],
        };

        for t in (input.from_time..input.to_time).step_by(input.time_resolution) {
            output.times.push(t);
            let v0 = f64::sin(
                ((t - input.from_time) as f64) * input.frequency * 2.0 * std::f64::consts::PI,
            );
            let v = input.min_value + ((v0 + 1.0) / 2.0) * (input.max_value - input.min_value);
            output.values.push(v);
        }

        // --- END compute output -------------------

        // serialize output
        let ser_output = match serde_json::to_string(&output) {
            Ok(v) => v,
            Err(e) => {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("failed to serialize output: {e}"),
                ))
            }
        };

        Ok(ser_output)
    }
}
