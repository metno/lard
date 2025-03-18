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
/// let status = drops::notify(tskey, 1740812400, 1740813000);
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
    println!("notify() called");
    let s = "no errors";
    let status = format!("notify() status: {s}");
    status
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_notify() {
        let tskey = TimeSeriesKey::new(18700, 211, 506, Some(0), Some(0));
        let status = obs_notify(tskey, 1740812400, 1740813000);
        _ = status; // TODO: actually verify that status has the expected value
    }
}
