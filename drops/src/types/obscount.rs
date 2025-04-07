use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::Availability;
use crate::ObsChange;
use crate::PgConnection;
use crate::PgConnectionPool;

/// Returns the unique name of the product type.
fn name() -> String {
    "ObsCount".to_string()
}

/// Returns the description of the product type.
fn description() -> String {
    "A demo type that gets the number of observations in a time range for either one or all \
    stations."
        .to_string()
}

/// Strongly typed representation of the product type input (see input_schema() for details).
/// The body of a POST request for the product must deserializable to this representation.
#[derive(Debug, Serialize, Deserialize)]
pub struct ObsCountInput {
    from_time: Option<i64>,
    to_time: Option<i64>,
    station_ids: Option<Vec<i64>>,
}

// Returns Some(error message) on invalid input, otherwise None.
fn validate_input(input: &ObsCountInput) -> Option<String> {
    if input.from_time >= input.to_time {
        return Some(format!(
            "from_time ({:?}) >= to_time {:?}",
            input.from_time, input.to_time
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
                "description": "earliest observation time (UNIX timestamp, default: -infinity)",
                "type": "integer"
            },
            "to_time": {
                "description": "latest observation time (UNIX timestamp, default: infinity)",
                "type": "integer"
            },
            "station_ids": {
                "description": "contributing station ID (default = all stations)",
                "type": "integer",
            },
        },
        "additionalProperties": false
    })
}

/// Strongly typed representation of the product type output.
#[derive(Debug, Serialize, Deserialize)]
pub struct ObsCountOutput {
    obs_count: i64,
}

/// Returns the output schema for this product type. The response body will be an instance of this
/// schema.
fn output_schema() -> serde_json::Value {
    json!({
        "type": "object",
        "properties": {
            "obs_count": {
                "type": "integer",
                "minimum": 0
            }
        },
        "additionalProperties": false
    })
}

// Computes the product from input into output. Returns Some(error message) on error, otherwise
// None.
fn compute_product(
    db_pool: PgConnectionPool,
    input: &ObsCountInput,
    output: &mut ObsCountOutput,
) -> Option<String> {
    // TODO
    _ = db_pool;
    _ = input;
    output.obs_count = 123; // for now

    None
}

/// Returns the currently available input instances for this product type.
fn input_instances(db_pool: PgConnectionPool) -> Result<Vec<String>, String> {
    // TODO

    /* generate an array of objects, one per unique station ID:
    [
        {
            "from_time": <earliest obs time for this station>,
            "to_time": <latest obs time for this station>,
            "station_id": <station ID>
        },
        {
            ...
        },
        ...
    ]
    */

    _ = db_pool;

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
    Json(input): Json<ObsCountInput>,
) -> Result<Json<ObsCountOutput>, (StatusCode, String)> {
    if let Some(e) = validate_input(&input) {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("validate_input() failed: {e}"),
        ));
    }

    let mut output = ObsCountOutput { obs_count: -1 };

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
