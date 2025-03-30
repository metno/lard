use axum::http::StatusCode;
use serde_json::json;
use std::{collections::HashMap, sync::Arc};

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
    pop_reg: HashMap<String, Arc<dyn operator::Operator + Send + Sync>>,
    changes: &[ObsChange],
) -> Result<(), String> {
    // delegate to all product operators
    for (_, op) in pop_reg {
        match op.handle_obs_changes(changes) {
            Ok(_) => (),
            Err(e) => _ = e, // TODO: handle error
        }
    }

    Ok(()) // for now
}

/// Notifies about a timer event.
pub fn timer_event_notify(
    pop_reg: HashMap<String, Arc<dyn operator::Operator + Send + Sync>>,
    time: i64,
) -> Result<(), String> {
    // delegate to all product operators
    for (_, op) in pop_reg {
        match op.handle_timer_event(time) {
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
///     <product operator registry>,
///     "SineWave".to_string(),
///     "<JSON input for SineWave>".to_string(),
/// ) {
///     Ok(product) => (), // do something with product
///     Error((status_code, err_msg)) => (), // flag error
/// }
/// ```
pub fn product(
    pop_reg: Arc<HashMap<String, Arc<dyn operator::Operator + Send + Sync>>>,
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
        return match op.product(input) {
            Ok(product) => Ok(product),
            Err((status_code, err_msg)) => Err((status_code, err_msg)),
        };
    }

    // *** product type not found ***

    Err((
        StatusCode::BAD_REQUEST,
        format!(
            "product type: {prod_type} not among supported types: {}",
            util::keys_as_sorted_csv((*pop_reg).clone())
        ),
    ))
}

/// Gets availability of one product type, or all product types if none is specified.
///
/// If product_type is empty, information is returned for all available product types, otherwise it
/// will be returned for the given product type.
///
/// On success the function returns a serialized JSON array that contains name, description, input
/// schema, output schema, and available input instances for the target product type(s).
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
///     <product operator registry>,
///     "SineWave".to_string(),
/// ) {
///     Ok(product_availability) => (), // do something with product_availability
///     Error((status_code, err_msg)) => (),
/// }
/// ```
pub fn product_availability(
    pop_reg: Arc<HashMap<String, Arc<dyn operator::Operator + Send + Sync>>>,
    prod_type: String,
) -> Result<String, (StatusCode, String)> {
    let mut ops: Vec<Arc<dyn operator::Operator + Send + Sync>> = Vec::new();

    if prod_type.is_empty() {
        // get availability of all product types
        for (_, op) in pop_reg.iter() {
            ops.push(op.clone());
        }
    } else if let Some(op) = pop_reg.get(&prod_type) {
        // get availability of this product type only
        ops.push(op.clone());
    } else {
        // *** product type not found ***
        return Err((
            StatusCode::BAD_REQUEST,
            format!(
                "product type: {prod_type} not among supported types: {}",
                util::keys_as_sorted_csv((*pop_reg).clone())
            ),
        ));
    }

    // get availability of all product types in ops

    let mut pas: Vec<String> = vec![];

    for op in ops {
        let input_instances = match op.input_instances() {
            Ok(v) => v,
            Err(e) => {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!(
                        "failed to get available input instances for product type {}: {e}",
                        op.name(),
                    ),
                ))
            }
        };
        let pa0 = json!({
            "type": op.name(),
            "description": op.description(),
            "input_schema": op.input_schema(),
            "output_schema": op.output_schema(),
            "input_instances": input_instances,
        });
        pas.push(pa0.to_string());
    }

    Ok(format!("[{}]", pas.join(",")))
}

#[cfg(test)]
mod tests {

    // TODO: fix the below tests. E.g. what to use for connection string?

    //use super::*;

    // #[tokio::test]
    // async fn test_obs_change_notify() {
    //     // TODO: fix this test to do something useful
    //     let connect_string = "dummy connection string".to_string(); //<--- ?
    //     let manager =
    //         PostgresConnectionManager::new_from_stringlike(connect_string, NoTls).unwrap();
    //     let db_pool = bb8::Pool::builder().build(manager).await.unwrap();
    //     let pop_reg = operator::init_reg(db_pool);
    //     let changes = vec![ObsChange {
    //         tskey: TimeSeriesKey::new(18700, 211, 506, Some(0), Some(0)),
    //         from_time: 1740812400,
    //         to_time: 1740813000,
    //     }];
    //     _ = obs_change_notify(pop_reg, changes);
    //     // TODO: handle error
    // }

    // #[tokio::test]
    // async fn test_timer_event_notify() {
    //     // TODO: fix this test to do something useful
    //     let connect_string = "dummy connection string".to_string(); //<--- ?
    //     let manager =
    //         PostgresConnectionManager::new_from_stringlike(connect_string, NoTls).unwrap();
    //     let db_pool = bb8::Pool::builder().build(manager).await.unwrap();
    //     let pop_reg = operator::init_reg(db_pool);
    //     let time = 1740812400;
    //     _ = timer_event_notify(pop_reg, time);
    //     // TODO: handle error
    // }

    // #[tokio::test]
    // async fn test_product() {
    //     // TODO: fix this test to do something useful
    //     let connect_string = "dummy connection string".to_string(); <--- ?
    //     let manager =
    //         PostgresConnectionManager::new_from_stringlike(connect_string, NoTls).unwrap();
    //     let db_pool = bb8::Pool::builder().build(manager).await.unwrap();
    //     let pop_reg = operator::init_reg(db_pool);
    //     let product_type = "dummy product type".to_string();   <--- ?
    //     let input = ...   <--- ?
    //     let product = product(
    //         pop_reg,
    //         product type,
    //         input,
    //     );
    //     _ = product; // TODO: actually verify that product has the expected value
    // }

    // //#[tokio::test]
    // async fn test_product_availability() {
    //     // TODO: fix this test to do something useful
    //     let connect_string = "dummy connection string".to_string(); <--- ?
    //     let manager =
    //         PostgresConnectionManager::new_from_stringlike(connect_string, NoTls).unwrap();
    //     let db_pool = bb8::Pool::builder().build(manager).await.unwrap();
    //     let pop_reg = operator::init_reg(db_pool);
    //     let product_type = "dummy product type".to_string();
    //     let availability = product_availability(pop_reg, product_type);
    //     _ = availability; // TODO: actually verify that availability has the expected value
    // }
}
