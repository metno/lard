use serde::{Deserialize, Serialize};

use crate::Error;
use util::idf_parse::{IdfMetadata, IdfValue};

/// Unit of the intensity values in the response
#[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Unit {
    /// Millimeters
    #[default]
    Mm,
    /// Litres per second per hectare
    Lsha,
}

/// Converts value [mm] and duration [minutes] to intensiry in [liter per second per hectare]
pub fn mm_to_lsha(val: f64, duration: u32) -> f64 {
    1e4 / 60.0 * val / duration as f64
}

// TODO: need blocking thread?
pub fn parse_values_csv(bytes: &[u8], unit: Unit) -> Result<(IdfMetadata, Vec<IdfValue>), Error> {
    // flexible allows us to store metadata in the header
    let mut reader = csv::ReaderBuilder::new().flexible(true).from_reader(bytes);

    // TODO: duplicated metadata record in station csv header row, are there better options?
    let metadata: IdfMetadata = {
        let header = reader.headers()?;
        // NOTE: requires column order to be same as struct field order
        header.deserialize(None)?
    };

    let values: Vec<IdfValue> = match unit {
        Unit::Mm => reader
            // NOTE: requires column order to be same as struct field order
            .into_records()
            .map(|res| {
                let value = res?.deserialize(None)?;
                Ok(value)
            })
            .collect::<Result<Vec<IdfValue>, Error>>(),

        Unit::Lsha => reader
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

// TODO: need blocking thread?
pub fn parse_metadata_csv(bytes: &[u8]) -> Result<Vec<IdfMetadata>, csv::Error> {
    // NOTE: requires column order to be same as struct field order
    csv::ReaderBuilder::new()
        .has_headers(false)
        .from_reader(bytes)
        .into_deserialize()
        .collect::<Result<Vec<IdfMetadata>, csv::Error>>()
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

        let (metadata, values) = parse_values_csv(csv.as_bytes(), Unit::Mm).unwrap();

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
