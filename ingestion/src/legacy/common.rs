use chrono::{DateTime, Utc};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct KvalobsId {
    pub station: i32,
    pub paramid: i32,
    pub typeid: i32,
    pub sensor: i32,
    pub level: i32,
}

#[derive(Debug, Clone)]
pub struct RawDatum<T> {
    pub kvid: KvalobsId,
    pub obstime: DateTime<Utc>,
    pub value: T,
}

#[derive(Debug)]
pub struct Datum<T> {
    pub tsid: i64,
    pub obstime: DateTime<Utc>,
    pub value: T,
}

// Query to get a tsid from the relevant source-specific label
pub const QUERY_GET_MET_STR: &str = r#"
    SELECT timeseries FROM labels.kvalobs
        WHERE station_id = $1
        AND param_id = $2
        AND type_id = $3
        AND (($4::int IS NULL AND lvl IS NULL) OR (lvl = $4))
        AND (($5::int IS NULL AND sensor IS NULL) OR (sensor = $5))
    "#;
