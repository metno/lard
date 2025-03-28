use axum::http::StatusCode;
use bb8_postgres::PostgresConnectionManager;
use std::{collections::HashMap, sync::Arc};
use tokio_postgres::NoTls;

/// Defines functions that all product type operators need to implement.
pub trait Operator {
    /// Returns the name of the product type. This serves as a unique identifier.
    fn name(&self) -> String;

    /// Returns the description of the product type.
    fn description(&self) -> String;

    /// Returns the JSON schema that the 'input' query parameter of the /product endpoint must
    /// validate against.
    fn input_schema(&self) -> String;

    /// Returns the JSON schema of a successful response body from the /product endpoint.
    fn output_schema(&self) -> String;

    /// Retrieves the available instances of the input schema. Each instance represents a
    /// combination of non-range fields (e.g. station number or parameter name) and the available
    /// extremes for range fields for each such combination (typically the lowest and highest
    /// available value of 'from_time' and 'to_time' respectively).
    /// NOTE: while a non-range field can have numeric type (like integer for station numbers),
    /// they're classified as non-range since it usually makes no sense for the user to specify
    /// them as [from, to] ranges, but typically as explicit lists.
    ///
    /// The 'pool' argument is a connection pool for the primary Postgres database.
    ///
    /// On success the function returns a vector of available input schema instances.
    ///
    /// On failure the function returns (HTTP status code, error message).
    fn input_instances(
        &self,
        pool: bb8::Pool<PostgresConnectionManager<NoTls>>,
    ) -> Result<Vec<String>, (StatusCode, String)>;

    /// Retrieves/computes a product of this type. The product is defined by 'input' which is
    /// assumed to be a valid instance of the input schema.
    ///
    /// The 'pool' argument is a connection pool for the primary Postgres database.
    ///
    /// On success the function returns an instance of the output schema.
    ///
    /// On failure the function returns (HTTP status code, error message).
    fn product(
        &self,
        pool: bb8::Pool<PostgresConnectionManager<NoTls>>,
        input: String,
    ) -> Result<String, (StatusCode, String)>;

    // TODO: also add:
    // - handle_obs_change_events
    // - handle_timer_event
}

/// Initializes the registry.
///
/// # Examples
///
/// ```
/// let pop_reg = drops::operator::init_reg();
/// ```
pub fn init_reg() -> HashMap<String, Arc<dyn Operator + Send + Sync>> {
    let mut reg = HashMap::new();

    // populate reg with operators
    // TODO: populate with only those operators that are specified as args to init_reg

    // sine wave (for testing - this product type doesn't access any external storage)
    reg.insert("SineWave".to_string(), crate::types::sinewave::new());

    // basic stats on the fly (i.e. never precompute anything) ... TODO
    // reg.insert("BasicStatsOTF".to_string(), crate::types::basicstatsotf::new());

    // operators for more product types ... TODO

    reg
}
