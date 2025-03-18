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

/// Gets a product that matches product_type and input_schema_instance. The return value will be
/// formatted according to the product type's output_schema.
///
/// # Examples
///
/// ```
/// let product = drops::get_product(
///     String::from("dummy product type"),
///     String::from("dummy input_schema_instance"),
/// );
/// ```
pub fn get_product(product_type: String, input_schema_instance: String) -> String {
    // TODO

    // for now:
    _ = product_type;
    _ = input_schema_instance;

    String::from("dummy")
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
/// ```
/// let availability = drops::get_product_availability(String::from("dummy product type"));
/// ```
pub fn get_product_availability(product_type: String) -> String {
    // TODO

    // for now:
    _ = product_type;

    String::from("dummy")
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

    #[test]
    fn test_get_product() {
        let product = get_product(
            String::from("dummy product type"),
            String::from("dummy input_schema_instance"),
        );
        _ = product; // TODO: actually verify that product has the expected value
    }

    #[test]
    fn test_get_product_availability() {
        let product_type = String::from("dummy product type");
        let availability = get_product_availability(product_type);
        _ = availability; // TODO: actually verify that availability has the expected value
    }
}
