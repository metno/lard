use std::{collections::HashMap, sync::Arc};

/// Defines functions that all product type operators need to implement.
pub trait Operator {
    /// Returns for this product type the JSON schema that the 'input' query parameter of the
    /// /product endpoint must validate against.
    fn input_schema(&self) -> String;

    // TODO: also add:
    // - output_schema
    // - name
    // - description
    // - input_schema_instances
    // - product
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
    reg.insert(String::from("SineWave"), crate::types::sinewave::new());

    // basic stats on the fly (i.e. never precompute anything) ... TODO
    // reg.insert(String::from("BasicStatsOTF"), crate::types::basicstatsotf::new());

    // operators for more product types ... TODO

    reg
}
