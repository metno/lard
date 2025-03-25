use bb8_postgres::PostgresConnectionManager;
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

/// Notifies about new, (quality) updated, or deleted observations in time series tskey in
/// time range [from_time, to_time]. from_time and to_time are UNIX timestamps relative to
/// 1970-01-01T00:00:00Z.
///
/// # Examples
///
/// ```
/// let tskey = drops::TimeSeriesKey::new(18700, 211, 506, Some(0), Some(0));
/// let status = drops::obs_notify(tskey, 1740812400, 1740813000);
/// // [2025-02-01T08:00:00Z, 2025-02-01T08:10:00Z]
/// ```
pub fn obs_notify(tskey: TimeSeriesKey, from_time: i64, to_time: i64) -> String {
    // TODO

    // for now:
    _ = tskey.station_id;
    _ = tskey.param_id;
    _ = tskey.type_id;
    _ = tskey.sensor;
    _ = tskey.level;
    _ = from_time;
    _ = to_time;
    let s = "no errors";
    let status = format!("obs_notify() status: {s}");
    status
}

/// Gets a product that matches product_type and input. The return value will be formatted
/// according to the product type's output_schema.
///
/// # Examples
///
/// ```ignore
/// let product = drops::get_product(
///     <postgres connection pool>,
///     <product operator registry>,
///     String::from("SineWave"),
///     String::from("<JSON input for SineWave>"),
/// );
/// ```
pub fn get_product(
    pool: bb8::Pool<PostgresConnectionManager<NoTls>>,
    pop_reg: HashMap<String, Arc<dyn operator::Operator + Send + Sync>>,
    prod_type: String,
    input: String,
) -> String {
    if let Some(op) = pop_reg.get(&prod_type) {
        println!("found operator for product type >{}<", prod_type);

        // TODO: ensure that input is a valid instance of op.input_schema()
        _ = op.input_schema();

        // TODO: compute product
        //_ = op.product(pool, input);
        // for now:
        _ = pool;
        _ = input;

        return String::from("200 Ok + response body"); // TODO
    }

    println!("did not find operator for {}", prod_type);

    // TODO
    String::from("400 Bad Request + reason = unsupported product type (supported types: ...)")
}

/// Gets the input schema, output schema, and available input schema instances for the given
/// product type(s).
///
/// If product_type is empty, information is returned for all available product types, otherwise it
/// will be returned for the given product type.
///
/// An input schema instance will contain the available extremes for range fields (e.g. from_time
/// and to_time will contain the oldest and newest available time respectively.
///
/// # Examples
///
/// ```ignore
/// let availability = drops::get_product_availability(
///     <postgres connection pool>,
///     <product operator registry>,
///     String::from("SineWave"),
/// );
/// ```
pub fn get_product_availability(
    pool: bb8::Pool<PostgresConnectionManager<NoTls>>,
    pop_reg: HashMap<String, Arc<dyn operator::Operator + Send + Sync>>,
    prod_type: String,
) -> String {
    // TODO

    // for now:
    _ = pool;
    _ = pop_reg;
    _ = prod_type;

    String::from("400 Bad Request + reason = unsupported product type (supported types: ...)")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_obs_notify() {
        let tskey = TimeSeriesKey::new(18700, 211, 506, Some(0), Some(0));
        let status = obs_notify(tskey, 1740812400, 1740813000);
        _ = status; // TODO: actually verify that status has the expected value
    }

    // TODO: fix the below tests. What to use for connection string?

    // #[tokio::test]
    // async fn test_get_product() {
    //     // TODO: fix this test to do something useful
    //     let connect_string = String::from("dummy connection string"); <--- PROBLEM!
    //     let manager =
    //         PostgresConnectionManager::new_from_stringlike(connect_string, NoTls).unwrap();
    //     let pool = bb8::Pool::builder().build(manager).await.unwrap();
    //     let pop_reg = HashMap::new();
    //     let product = get_product(
    //         pool,
    //         pop_reg,
    //         String::from("dummy product type"),
    //         String::from("dummy input"),
    //     );
    //     _ = product; // TODO: actually verify that product has the expected value
    // }

    // //#[tokio::test]
    // async fn test_get_product_availability() {
    //     // TODO: fix this test to do something useful
    //     let connect_string = String::from("dummy connection string"); <--- PROBLEM!
    //     let manager =
    //         PostgresConnectionManager::new_from_stringlike(connect_string, NoTls).unwrap();
    //     let pool = bb8::Pool::builder().build(manager).await.unwrap();
    //     let pop_reg = HashMap::new();
    //     let product_type = String::from("dummy product type");
    //     let availability = get_product_availability(pool, pop_reg, product_type);
    //     _ = availability; // TODO: actually verify that availability has the expected value
    // }
}
