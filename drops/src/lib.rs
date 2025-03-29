use axum::http::StatusCode;
use bb8_postgres::PostgresConnectionManager;
use serde_json::json;
use std::{collections::HashMap, sync::Arc};
use tokio_postgres::NoTls;

pub mod operator;
pub mod types;

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

/// Represents the occurrence of a change to original or updated observations in a time range of a
/// time series. The specific change (i.e. which observations got inserted, updated, or deleted) is
/// for now assumed to be irrelevant.
pub struct ObsChange {
    tskey: TimeSeriesKey,
    from_time: i64, // UNIX timestamp, inclusive
    to_time: i64,   // UNIX timestamp, inclusive
}

/// Notifies about new, updated, or deleted observations in a set of time series.
pub fn obs_change_notify(
    pop_reg: HashMap<String, Arc<dyn operator::Operator + Send + Sync>>,
    pool: bb8::Pool<PostgresConnectionManager<NoTls>>,
    changes: &[ObsChange],
) -> Result<(), String> {
    // delegate to all product operators
    for (_, op) in pop_reg {
        match op.handle_obs_changes(&pool, changes) {
            Ok(_) => (),
            Err(e) => _ = e, // TODO: handle error
        }
    }

    Ok(()) // for now
}

/// Gets a product that matches prod_type and input. The input must be a valid instance of the
/// input schema of the product type.
///
/// On success the function returns an instance of the output schema of the product type as a
/// serialized JSON object.
///
/// On failure the function returns (HTTP status code, error message).
///
/// # Examples
///
/// ```ignore
/// match drops::product(
///     <postgres connection pool>,
///     <product operator registry>,
///     "SineWave".to_string(),
///     "<JSON input for SineWave>".to_string(),
/// ) {
///     Ok(product) => (), // do something with product
///     Error((status_code, err_msg)) => (),
/// }
/// ```
pub fn product(
    pool: bb8::Pool<PostgresConnectionManager<NoTls>>,
    pop_reg: HashMap<String, Arc<dyn operator::Operator + Send + Sync>>,
    prod_type: String,
    input: serde_json::Value,
) -> Result<String, (StatusCode, String)> {
    if let Some(op) = pop_reg.get(&prod_type) {
        // *** product type found ***

        // validate input against JSON schema
        match jsonschema::validate(&op.input_schema(), &input) {
            Ok(_) => (),
            Err(e) => {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!("input validation error: {e}"),
                ));
            }
        };

        // compute product
        return match op.product(pool, input) {
            Ok(product) => Ok(product),
            Err((status_code, err_msg)) => Err((status_code, err_msg)),
        };
    }

    // *** product type not found ***

    Err((
        StatusCode::BAD_REQUEST,
        format!(
            "product type: {prod_type} not among supported types: {}",
            util::keys_as_sorted_csv(pop_reg)
        ),
    ))
}

/// Gets availability of one product type, or all product types if none is specified.
///
/// If product_type is empty, information is returned for all available product types, otherwise it
/// will be returned for the given product type.
///
/// On success the function returns a serialized JSON object that contains the input schema,
/// output schema, and available input instances for the given product type(s).
///
/// On failure the function returns (HTTP status code, error message).
///
/// An input schema instance will contain the available extremes for range fields. A typical
/// example of a range field is 'from_time' which will contain the oldest available time.
///
/// # Examples
///
/// ```ignore
/// match drops::product_availability(
///     <postgres connection pool>,
///     <product operator registry>,
///     "SineWave".to_string(),
/// ) {
///     Ok(product_availability) => (), // do something with product_availability
///     Error((status_code, err_msg)) => (),
/// }
/// ```
pub fn product_availability(
    pool: bb8::Pool<PostgresConnectionManager<NoTls>>,
    pop_reg: HashMap<String, Arc<dyn operator::Operator + Send + Sync>>,
    prod_type: String,
) -> Result<String, (StatusCode, String)> {
    let mut ops: Vec<Arc<dyn operator::Operator + Send + Sync>> = Vec::new();

    if prod_type.is_empty() {
        // get availability for all product types
        for (_, op) in pop_reg.iter() {
            ops.push(op.clone());
        }
    } else if let Some(op) = pop_reg.get(&prod_type) {
        // get availability for this product type only
        ops.push(op.clone());
    } else {
        // *** product type not found ***
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "product type: {prod_type} not among supported types: {}",
                util::keys_as_sorted_csv(pop_reg)
            ),
        ));
    }

    // TODO: join and return availability info for all items in ops
    // _ = op.input_schema();
    // _ = op.output_schema();
    // _ = op.input_instances(pool);

    // for now
    _ = pool;
    let dummy_product_availability = json!({
        "foo": "bar"
    })
    .to_string();

    Ok(dummy_product_availability)
}

#[cfg(test)]
mod tests {

    // TODO: fix the below tests. What to use for connection string?

    //use super::*;

    // #[tokio::test]
    // async fn test_obs_notify() {
    //     // TODO: fix this test to do something useful
    //     let connect_string = "dummy connection string".to_string(); //<--- PROBLEM!
    //     let manager =
    //         PostgresConnectionManager::new_from_stringlike(connect_string, NoTls).unwrap();
    //     let pool = bb8::Pool::builder().build(manager).await.unwrap();
    //     let changes = vec![ObsChange {
    //         tskey: TimeSeriesKey::new(18700, 211, 506, Some(0), Some(0)),
    //         from_time: 1740812400,
    //         to_time: 1740813000,
    //     }];
    //     _ = obs_change_notify(operator::init_reg(), pool, changes);
    //     // TODO: handle error
    // }

    // #[tokio::test]
    // async fn test_product() {
    //     // TODO: fix this test to do something useful
    //     let connect_string = "dummy connection string".to_string(); <--- PROBLEM!
    //     let manager =
    //         PostgresConnectionManager::new_from_stringlike(connect_string, NoTls).unwrap();
    //     let pool = bb8::Pool::builder().build(manager).await.unwrap();
    //     let pop_reg = HashMap::new();
    //     let product = product(
    //         pool,
    //         pop_reg,
    //         "dummy product type".to_string(),
    //         "dummy input".to_string(),
    //     );
    //     _ = product; // TODO: actually verify that product has the expected value
    // }

    // //#[tokio::test]
    // async fn test_product_availability() {
    //     // TODO: fix this test to do something useful
    //     let connect_string = "dummy connection string".to_string(); <--- PROBLEM!
    //     let manager =
    //         PostgresConnectionManager::new_from_stringlike(connect_string, NoTls).unwrap();
    //     let pool = bb8::Pool::builder().build(manager).await.unwrap();
    //     let pop_reg = HashMap::new();
    //     let product_type = "dummy product type".to_string();
    //     let availability = product_availability(pool, pop_reg, product_type);
    //     _ = availability; // TODO: actually verify that availability has the expected value
    // }
}
