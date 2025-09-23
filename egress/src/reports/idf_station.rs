use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};

use crate::{
    error::{self, Error},
    S3Bucket,
};

/// Unit of the intensity values in the response
#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum IdfUnit {
    /// Millimeters
    #[default]
    Mm,
    /// Litres per second per hectare
    Lsha,
}

/// Precipitation intensity values fitted from a GEV distribution on annual precipitation timeseries.
/// More information can be found [here](https://doi.org/10.1016/j.jhydrol.2021.127000).
/// The code responsible for generating these values can be found [here](https://github.com/ClimDesign/fixIDF).
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IdfValue {
    /// Duration of the precipitation event [min]
    pub duration: u32,
    /// Expected time between events of computed intensity [years]
    pub frequency: i32,
    /// Computed rainfall intensity value [mm]
    pub intensity: f64,
    /// 0.025 quantile of computed rainfall intensity [mm]
    pub lower_interval: f64,
    /// 0.975 quantile of computed rainfall intensity [mm]
    pub upper_interval: f64,
}

#[cfg(feature = "integration_tests")]
impl IdfValue {
    pub fn new(
        duration: u32,
        frequency: i32,
        intensity: f64,
        lower_interval: f64,
        upper_interval: f64,
    ) -> Self {
        Self {
            duration,
            frequency,
            intensity,
            lower_interval,
            upper_interval,
        }
    }
}

/// Metadata and parameters used for fitting IDF values
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IdfMetadata {
    /// MET station identifier
    pub station_id: i32,
    /// Number of years considered in the calculation
    /// In Norway, the most severe rainfall events usually fall in the May-September period,
    /// so if the data coverage in this period is below 80% the year is skipped
    pub number_of_seasons: i32,
    /// First date considered in the precipitation timeseries
    pub from_time: chrono::NaiveDate,
    /// Last date considered in the precipitation timeseries
    pub to_time: chrono::NaiveDate,
    /// Robustness of the estimated IDF values, computed by running multiple IDF estimations and
    /// comparing the convergence of their results. Currently only three values are possible:
    /// 1 (robust), 2 (uncertain), 3 (very uncertain)
    pub quality_class: i32,
    /// RNG seed used in the calculation
    pub seed_parameter: i32,
    /// When the calculation was carried out
    pub updated_at: chrono::NaiveDate,
}

#[cfg(feature = "integration_tests")]
impl IdfMetadata {
    pub fn new(
        station_id: i32,
        number_of_seasons: i32,
        first_year_of_period: chrono::NaiveDate,
        last_year_of_period: chrono::NaiveDate,
        quality_class: i32,
        seed_parameter: i32,
        updated_at: chrono::NaiveDate,
    ) -> Self {
        Self {
            station_id,
            number_of_seasons,
            from_time: first_year_of_period,
            to_time: last_year_of_period,
            quality_class,
            seed_parameter,
            updated_at,
        }
    }
}

/// Query parameters struct for the station/:station_id endpoint
#[derive(Serialize, Deserialize)]
pub struct IdfStationParams {
    #[serde(default)]
    unit: IdfUnit,
}

/// Response struct returned by the station/:station_id endpoint
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IdfStationResp {
    // TODO: is this correct???
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub values: Vec<IdfValue>,
    pub unit: IdfUnit,
    #[serde(flatten)]
    pub metadata: IdfMetadata,
}

/// Response struct returned by the availability endpoint
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct IdfStationAvailability {
    pub stations: Vec<IdfMetadata>,
}

/// Converts value [mm] and duration [minutes] to intensiry in [liter per second per hectare]
pub fn mm_to_lsha(val: f64, duration: u32) -> f64 {
    1e4 / 60.0 * val / duration as f64
}

// TODO: need blocking thread?
fn parse_values_csv(bytes: &[u8], unit: IdfUnit) -> Result<(IdfMetadata, Vec<IdfValue>), Error> {
    // flexible allows us to store metadata in the header
    let mut reader = csv::ReaderBuilder::new().flexible(true).from_reader(bytes);

    // TODO: duplicated metadata record in station csv header row, are there better options?
    let metadata: IdfMetadata = {
        let header = reader.headers()?;
        // NOTE: requires column order to be same as struct field order
        header.deserialize(None)?
    };

    let values: Vec<IdfValue> = match unit {
        IdfUnit::Mm => reader
            // NOTE: requires column order to be same as struct field order
            .into_records()
            .map(|res| {
                let value = res?.deserialize(None)?;
                Ok(value)
            })
            .collect::<Result<Vec<IdfValue>, Error>>(),

        IdfUnit::Lsha => reader
            .into_records()
            .map(|res| {
                let value: IdfValue = res?.deserialize(None)?;

                Ok(IdfValue {
                    duration: value.duration,
                    frequency: value.frequency,
                    intensity: mm_to_lsha(value.intensity, value.duration),
                    lower_interval: mm_to_lsha(value.lower_interval, value.duration),
                    upper_interval: mm_to_lsha(value.upper_interval, value.duration),
                })
            })
            .collect(),
    }?;

    Ok((metadata, values))
}

pub async fn idf_station_handler(
    Path(station_id): Path<i32>,
    State(s3_bucket): State<S3Bucket>,
    Query(params): Query<IdfStationParams>,
) -> Result<Json<IdfStationResp>, (StatusCode, String)> {
    let station_file = s3_bucket
        // TODO: possible vulnerability?
        .get_object(format!("/lard_reports/idf/{station_id}.csv"))
        .await
        .map_err(error::internal_error)?;

    let bytes = station_file
        .as_str()
        .map_err(error::internal_error)?
        .as_bytes();

    let (metadata, values) = parse_values_csv(bytes, params.unit).map_err(error::internal_error)?;

    Ok(Json(IdfStationResp {
        // station_id,
        metadata,
        unit: params.unit,
        values,
    }))
}

// TODO: need blocking thread?
fn parse_metadata_csv(bytes: &[u8]) -> Result<Vec<IdfMetadata>, csv::Error> {
    // NOTE: requires column order to be same as struct field order
    csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(bytes)
        .into_deserialize()
        .collect::<Result<Vec<IdfMetadata>, csv::Error>>()
}

pub async fn idf_station_availability_handler(
    State(s3_bucket): State<S3Bucket>,
) -> Result<Json<IdfStationAvailability>, (StatusCode, String)> {
    let metadata = s3_bucket
        .get_object("/lard_reports/idf/metadata.csv".to_string())
        .await
        .map_err(error::internal_error)?;

    let bytes = metadata.as_str().map_err(error::internal_error)?.as_bytes();

    let stations = parse_metadata_csv(bytes).map_err(error::internal_error)?;

    Ok(Json(IdfStationAvailability { stations }))
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use std::fmt::Write;

    use super::*;

    #[test]
    fn test_value_csv_parser() {
        let expected_metadata = IdfMetadata {
            station_id: 12345,
            number_of_seasons: 39,
            from_time: NaiveDate::from_ymd_opt(1968, 1, 1).unwrap(),
            to_time: NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
            quality_class: 3,
            seed_parameter: 0,
            updated_at: NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
        };

        let expected_values = [
            IdfValue {
                duration: 1,
                frequency: 2,
                intensity: 1.2,
                lower_interval: 1.5,
                upper_interval: 1.7,
            },
            IdfValue {
                duration: 1,
                frequency: 5,
                intensity: 1.2,
                lower_interval: 1.5,
                upper_interval: 1.7,
            },
            IdfValue {
                duration: 5,
                frequency: 2,
                intensity: 1.2,
                lower_interval: 1.5,
                upper_interval: 1.7,
            },
            IdfValue {
                duration: 5,
                frequency: 5,
                intensity: 1.2,
                lower_interval: 1.5,
                upper_interval: 1.7,
            },
        ];

        let csv = {
            let mut csv = format!(
                "{},{},{},{},{},{},{}\n",
                expected_metadata.station_id,
                expected_metadata.number_of_seasons,
                expected_metadata.from_time,
                expected_metadata.to_time,
                expected_metadata.quality_class,
                expected_metadata.seed_parameter,
                expected_metadata.updated_at,
            );

            for val in &expected_values {
                writeln!(
                    &mut csv,
                    "{},{},{},{},{}",
                    val.duration,
                    val.frequency,
                    val.intensity,
                    val.lower_interval,
                    val.upper_interval,
                )
                .unwrap();
            }

            csv
        };

        let (metadata, values) = parse_values_csv(csv.as_bytes(), IdfUnit::Mm).unwrap();

        assert_eq!(metadata, expected_metadata);

        for i in 0..values.len() {
            assert_eq!(values[i], expected_values[i]);
        }
    }
    #[test]
    fn test_metadata_csv_parser() {
        let expected_stations = [
            IdfMetadata {
                station_id: 12345,
                number_of_seasons: 39,
                from_time: NaiveDate::from_ymd_opt(1968, 1, 1).unwrap(),
                to_time: NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
                quality_class: 3,
                seed_parameter: 0,
                updated_at: NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            },
            IdfMetadata {
                station_id: 67890,
                number_of_seasons: 50,
                from_time: NaiveDate::from_ymd_opt(1999, 1, 1).unwrap(),
                to_time: NaiveDate::from_ymd_opt(2009, 1, 1).unwrap(),
                quality_class: 0,
                seed_parameter: 0,
                updated_at: NaiveDate::from_ymd_opt(2010, 1, 1).unwrap(),
            },
        ];

        let csv = {
            let mut csv = String::new();
            for meta in &expected_stations {
                writeln!(
                    &mut csv,
                    "{},{},{},{},{},{},{}\n",
                    meta.station_id,
                    meta.number_of_seasons,
                    meta.from_time,
                    meta.to_time,
                    meta.quality_class,
                    meta.seed_parameter,
                    meta.updated_at
                )
                .unwrap()
            }
            csv
        };

        let stations = parse_metadata_csv(csv.as_bytes()).unwrap();

        for i in 0..stations.len() {
            assert_eq!(stations[i], expected_stations[i]);
        }
    }
}
