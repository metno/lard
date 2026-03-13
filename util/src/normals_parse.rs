use csv::{Reader, ReaderBuilder, WriterBuilder};
use serde::{Deserialize, Serialize};
use std::sync::LazyLock;
use std::{collections::HashMap, fs::File, io::Read};

use crate::idf_parse::Error;

pub const NORMALS_S3_BASEPATH: &str = "/lard_reports/normals/";
pub const NORMALS_S3_PATH: &str = "/lard_reports/normals/latest/";

/// Documentation comments for normals record struct:
#[derive(Debug, Serialize, Deserialize)]
pub struct NormalsRecord {
    /// STNR: National station number
    #[serde(alias = "STNR")]
    pub station_id: i32,
    /// MONTH: Month of year a normal value is for
    #[serde(alias = "MONTH")]
    pub month: i32,
    /// DAY: Day of month a normal value is for (in T_NORMAL_DIURNAL only)
    #[serde(alias = "DAY")]
    pub day: Option<i32>,
    /// ELEM_CODE: Element identifier, has a unique element_id equivalent
    #[serde(alias = "ELEM_CODE")]
    pub elem_code: String,
    /// NORMAL: The data value itself
    #[serde(alias = "NORMAL")]
    pub normal_value: Option<f64>,
    /// FYEAR: Start of the 30-year period, typically 1931, 1961, 1991
    #[serde(alias = "FYEAR")]
    pub from_year: i32,
    /// TYEAR: End of the 30-year period, typically 1960, 1990, 2020
    #[serde(alias = "TYEAR")]
    pub to_year: i32,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct NormalMetadata {
    pub element_id: String,
    // a comma separated list of available stations for the element
    // can't use vec<i32> since we want to write to csv
    // (and it can't handle that they are different lengths)
    pub available_stations: String,
}

#[cfg(feature = "integration_tests")]
impl NormalMetadata {
    pub fn new(element_id: String, available_stations: String) -> Self {
        Self {
            element_id,
            available_stations,
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Normal {
    pub month: i32,
    pub day: Option<i32>,
    pub normal_value: Option<f64>,
    pub from_year: i32,
    pub to_year: i32,
}

#[cfg(feature = "integration_tests")]
impl Normal {
    pub fn new(
        month: i32,
        day: Option<i32>,
        normal_value: f64,
        from_year: i32,
        to_year: i32,
    ) -> Self {
        Self {
            month,
            day,
            normal_value: Some(normal_value),
            from_year,
            to_year,
        }
    }
}

/// NormalsMapMonth maps ElemCode from KDVH to ElementID/NormalID in ODA
// https://gitlab.met.no/oda/oda/-/blob/main/tools/kdvh-importer/normals.go?ref_type=heads#L52
// note: DDR_GE1 was changed to DRR_GE1 since that is how it appears in the csv file
// appear to be missing conversion for GD17 (without _I)
static MONTHLY_NORMALS_ELEM_MAP: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        HashMap::from([
            // monthly normals
            (
                "DRR_GE1",
                "number_of_days_gte(sum(precipitation_amount P1D) %s 1.0)",
            ),
            (
                "GD17_I",
                "integral_of_deficit_interpolated(mean(air_temperature P1D) %s 17.0)",
            ),
            ("OT", "sum(duration_of_sunshine %s)"),
            ("POM", "mean(surface_air_pressure %s)"),
            ("PRM", "mean(air_pressure_at_sea_level %s)"),
            ("RR", "sum(precipitation_amount %s)"),
            (
                "RRGRP0",
                "frequency_group_thresholds(precipitation_amount %s threshold0)",
            ),
            (
                "RRGRP1",
                "frequency_group_thresholds(precipitation_amount %s threshold1)",
            ),
            (
                "RRGRP2",
                "frequency_group_thresholds(precipitation_amount %s threshold2)",
            ),
            (
                "RRGRP3",
                "frequency_group_thresholds(precipitation_amount %s threshold3)",
            ),
            (
                "RRGRP4",
                "frequency_group_thresholds(precipitation_amount %s threshold4)",
            ),
            (
                "RRGRP5",
                "frequency_group_thresholds(precipitation_amount %s threshold5)",
            ),
            (
                "RRGRP6",
                "frequency_group_thresholds(precipitation_amount %s threshold6)",
            ),
            ("TAM", "mean(air_temperature %s)"),
            (
                "TAM_DAY_STDEV",
                "standard_deviation(mean(air_temperature P1D) %s)",
            ),
            ("TANM", "mean(min(air_temperature P1D) %s)"),
            ("TAXM", "mean(max(air_temperature P1D) %s)"),
            ("UM", "mean(relative_humidity %s)"),
        ])
    });

static DIURNAL_NORMALS_ELEM_MAP: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        HashMap::from([
            // diurnal normals
            ("TAM", "mean(air_temperature P1D)"),
            ("RR_ACC", "sum_until_day_of_year(precipitation_amount P1D)"),
        ])
    });

pub fn parse_normals_csv_file(
    filename: &str,
    normal_type: &str,
) -> Result<HashMap<i32, HashMap<String, Vec<Normal>>>, Error> {
    let file = File::open(filename)?;
    let mut rdr = ReaderBuilder::new().delimiter(b',').from_reader(file);

    parse_normals_csv_content(&mut rdr, normal_type)
}

/// Documentation comments for use of month:
/// 13: yearly values
/// 21: spring (Mar-May)
/// 22: summer (Jun-Aug)
/// 23: autumn (Sep-Nov)
/// 24: winter (Dec–Feb)
/// 25: cold half (TODO: not sure about exact months/dates)
/// 26: warm half (TODO: not sure about exact months/dates)
pub fn parse_normals_csv_content<R: Read>(
    rdr: &mut Reader<R>,
    normal_type: &str,
) -> Result<HashMap<i32, HashMap<String, Vec<Normal>>>, Error> {
    // Iterate over records and print them
    let mut map_values: HashMap<i32, HashMap<String, Vec<Normal>>> = HashMap::new();
    for result in rdr.deserialize() {
        let record: NormalsRecord = result?;

        let elem_id = match normal_type {
            "monthly" => MONTHLY_NORMALS_ELEM_MAP.get(record.elem_code.as_str()),
            "diurnal" => DIURNAL_NORMALS_ELEM_MAP.get(record.elem_code.as_str()),
            _ => {
                // commenting out error to be able to parse files with unknown elem codes
                // currently for example GD17 which we don't have mapping for
                /*
                return Err(Error::ParseError(format!(
                    "Unknown ElemCode in normals file: {}",
                    record.elem_code
                )));
                */
                eprintln!("Unknown normal type: {}", normal_type);
                continue;
            }
        };
        if elem_id.is_none() {
            eprintln!("Unknown ElemCode in normals file: {}", record.elem_code);
            continue;
        }
        let time_resolution = match record.month {
            1..13 => "P1M",
            13 => "P1Y",
            21..25 => "P3M",
            25 | 26 => "P6M",
            _ => {
                eprintln!("Unknown month value in normals file: {}", record.month);
                continue;
            }
        };
        // change the %s to a period based on month
        let elem_id = elem_id.unwrap().replace("%s", time_resolution);

        let normal = Normal {
            month: record.month,
            day: record.day,
            normal_value: record.normal_value,
            from_year: record.from_year,
            to_year: record.to_year,
        };
        // insert the data
        map_values
            .entry(record.station_id)
            .or_default()
            .entry(elem_id)
            .or_default()
            .push(normal);
    }
    println!("Parsed {} normals records", map_values.len());

    Ok(map_values)
}

pub fn create_normals_csv_content(
    data: HashMap<i32, HashMap<String, Vec<Normal>>>,
    normal_type: &str,
) -> Result<Vec<(String, String)>, Error> {
    let mut list_of_name_content: Vec<(String, String)> = vec![];
    // setup writer for metadata
    let mut wtr_metadata = WriterBuilder::new().has_headers(false).from_writer(vec![]);
    let mut elem_stations_map: HashMap<String, Vec<i32>> = HashMap::new();

    for (station_id, element_normal) in data {
        for elem in element_normal.keys() {
            // keep the information for the metadata file
            elem_stations_map
                .entry(elem.to_string())
                .or_default()
                .push(station_id);
        }

        let filename = format!("{}_{}.csv", normal_type, station_id);
        // writer for data
        let mut wtr = WriterBuilder::new()
            .flexible(true)
            .has_headers(false)
            .from_writer(vec![]);

        // write to data file
        for value in element_normal {
            wtr.serialize(value)?;
        }
        let data = String::from_utf8(
            wtr.into_inner()
                .map_err(|e| Error::CsvWriterError(e.to_string()))?,
        )?;
        list_of_name_content.push((filename, data));
    }
    // write metadata file
    for (elem, stations) in elem_stations_map {
        let metadata = NormalMetadata {
            element_id: elem,
            available_stations: stations
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<String>>()
                .join(","),
        };
        wtr_metadata.serialize(&metadata)?;
    }
    let metadata = String::from_utf8(
        wtr_metadata
            .into_inner()
            .map_err(|e| Error::CsvWriterError(e.to_string()))?,
    )?;
    let metadata_filename = format!("{}_metadata.csv", normal_type);
    list_of_name_content.push((metadata_filename, metadata));

    Ok(list_of_name_content)
}
