use std::{collections::HashMap, sync::Arc};

/// Defines functions that all product type operators need to implement.
pub trait Operator {
    fn input_schema(&self) -> String;
    // TODO: also add:
    // - output_schema
    // - name
    // - description
    // - input_instances
    // - product
    // - handle_obs_change_events
    // - handle_timer_event
}

/// Initializes the registry.
pub fn init_reg() -> HashMap<String, Arc<dyn Operator + Send + Sync>> {
    let mut reg = HashMap::new();

    // populate reg with operators
    // TODO: populate with only those operators that are specified as args to init_reg

    // test 1 - sine wave
    let sine_wave = crate::types::sinewave::new();
    match reg.insert(String::from("SineWave"), sine_wave) {
        Some(_) => {
            println!("key SineWave already exists - old value kept")
        }
        None => {
            println!("first value for SineWave inserted")
        }
    }

    // test 2 - basic stats on the fly (i.e. never precomputing anything)
    // TODO

    reg
}
