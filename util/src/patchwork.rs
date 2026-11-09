//! Definitions related to `lard_egress::patchwork` that are needed outside
//! egress

use serde::{Deserialize, Serialize};

use crate::ParamId;

/// [`crate::MetTimeseriesKey`] without typeid, as patchwork's purpose can be
/// seen as removing the typeid (it merges all timeseries that differ only by
/// typeid, picking the "best" for each timerange based on priorities defined
/// in metadata).
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Label {
    #[serde(rename = "stationid")]
    pub station_id: i32,
    #[serde(rename = "paramid")]
    pub param_id: ParamId,
    pub level: Option<i32>,
    pub sensor: Option<i32>,
}

impl Label {
    pub fn new(
        station_id: i32,
        param_id: ParamId,
        level: Option<i32>,
        sensor: Option<i32>,
    ) -> Self {
        Self {
            station_id,
            param_id,
            level,
            sensor,
        }
    }
}
