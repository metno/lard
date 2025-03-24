use std::sync::Arc;

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
    fn input_schema(&self) -> String {
        _ = self.x;
        String::from("dummy input schema for SineWave")
    }
}
