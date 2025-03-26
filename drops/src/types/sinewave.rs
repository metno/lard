use axum::http::StatusCode;
use bb8_postgres::PostgresConnectionManager;
use serde_json::json;
use std::sync::Arc;
use tokio_postgres::NoTls;

// TODO ...

/// Documentation for SineWave ...
pub struct SineWave {
    /// documentation for x ...
    pub x: i32,
}

pub fn new() -> Arc<dyn crate::operator::Operator + Send + Sync> {
    let sine_wave = SineWave { x: -1 };
    Arc::new(sine_wave)
}

impl crate::operator::Operator for SineWave {
    fn name(&self) -> String {
        "SineWave".to_string()
    }

    fn description(&self) -> String {
        "A basic sine wave.".to_string()
    }

    fn input_schema(&self) -> String {
        json!({
            "type": "object",
            "properties": {
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
                },
                "from_time": {
                    "description": "earliest second",
                    "type": "integer"
                },
                "to_time": {
                    "description": "latest second",
                    "type": "number"
                }
            },
            "additionalProperties": false
        })
        .to_string()
    }

    fn output_schema(&self) -> String {
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
        .to_string()
    }

    fn input_instances(
        &self,
        pool: bb8::Pool<PostgresConnectionManager<NoTls>>,
    ) -> Result<Vec<String>, (StatusCode, String)> {
        _ = pool; // n/a since SineWave doesn't access data on external storage
        Ok(vec![])
    }
}
