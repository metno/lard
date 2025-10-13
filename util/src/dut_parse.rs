use serde::{Deserialize, Serialize};

use crate::deserialize::idf_date;
use crate::idf_parse::IdfValue;

/// Season magic numbers used at MET
#[derive(Debug, Serialize, Deserialize)]
pub enum Season {
    Spring = 21,
    Summer = 22,
    Autumn = 23,
    Winter = 24,
}

/// Metadata and parameters used for fitting IDF values
// NOTE: the same as IdfMetadata except that here `station_id` becomes `municipality_id`
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DutMetadata {
    /// Norwegian municipality identifier
    #[serde(alias = "stnr")]
    pub municipality_id: i32,
    /// Number of years considered in the calculation
    /// In Norway, the most severe rainfall events usually fall in the May-September period,
    /// so if the data coverage in this period is below 80% the year is skipped
    #[serde(alias = "SEASONS")]
    pub number_of_seasons: i32,
    /// First date considered in the precipitation timeseries
    #[serde(alias = "FDATO", deserialize_with = "idf_date")]
    pub from_time: chrono::NaiveDate,
    /// Last date considered in the precipitation timeseries
    #[serde(alias = "TDATO", deserialize_with = "idf_date")]
    pub to_time: chrono::NaiveDate,
    /// Robustness of the estimated IDF values, computed by running multiple IDF estimations and
    /// comparing the convergence of their results. Currently only three values are possible:
    /// 1 (robust), 2 (uncertain), 3 (very uncertain)
    #[serde(alias = "CLASS")]
    pub quality_class: i32,
    /// RNG seed used in the calculation
    #[serde(alias = "SEED")]
    pub seed_parameter: i32,
    /// When the calculation was carried out
    #[serde(alias = "UPDATE", deserialize_with = "idf_date")]
    pub updated_at: chrono::NaiveDate,
}

#[cfg(feature = "integration_tests")]
impl DutMetadata {
    pub fn new(
        municipality_id: i32,
        number_of_seasons: i32,
        from_time: chrono::NaiveDate,
        to_time: chrono::NaiveDate,
        quality_class: i32,
        seed_parameter: i32,
        updated_at: chrono::NaiveDate,
    ) -> Self {
        Self {
            municipality_id,
            number_of_seasons,
            from_time,
            to_time,
            quality_class,
            seed_parameter,
            updated_at,
        }
    }
}

// Similar to IdfRecord, but it includes different sets of idf values per season
#[derive(Debug, Serialize, Deserialize)]
struct DutRecord {
    #[serde(flatten)]
    metadata: DutMetadata,
    #[serde(flatten)]
    value: IdfValue,

    // Which season this value is
    #[serde(alias = "time_of_year")]
    season: Season,
    // Unused
    #[serde(alias = "REF_period")]
    reference_period: String,
}
