use csv::{Reader, ReaderBuilder, WriterBuilder};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs::File, io::Read};

use crate::idf_parse::Error;
use crate::stinfofacade::elem;

pub const NORMALS_S3_BASEPATH: &str = "/lard_reports/normals/";
pub const NORMALS_S3_PATH: &str = "/lard_reports/normals/latest/";

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
    pub param_id: i32,
    pub station: i32,
}

#[cfg(feature = "integration_tests")]
impl NormalMetadata {
    pub fn new(element_id: String, param_id: i32, station: i32) -> Self {
        Self {
            element_id,
            param_id,
            station,
        }
    }
}

// define the size of the RRGRP normal arrays, which is 7 since there are 7 thresholds
const RRGRP_ARRAY_SIZE: usize = 7;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Normal {
    pub element_id: String,
    pub param_id: i32,
    pub period: String,
    pub month: i32,
    pub day: Option<i32>,
    pub normal_value: Option<f64>,
    pub normal_array: Option<[Option<f64>; RRGRP_ARRAY_SIZE]>,
}

#[cfg(feature = "integration_tests")]
impl Normal {
    pub fn new(
        element_id: String,
        param_id: i32,
        period: String,
        month: i32,
        day: Option<i32>,
        normal_value: Option<f64>,
        normal_array: Option<[Option<f64>; RRGRP_ARRAY_SIZE]>,
    ) -> Self {
        Self {
            element_id,
            param_id,
            period,
            month,
            day,
            normal_value,
            normal_array,
        }
    }
}

pub fn parse_normals_csv_file(
    filename: &str,
    elem_tables: elem::Tables,
) -> Result<HashMap<i32, Vec<Normal>>, Error> {
    let file = File::open(filename)?;
    let mut rdr = ReaderBuilder::new().delimiter(b',').from_reader(file);

    parse_normals_csv_content(&mut rdr, elem_tables)
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
    tables: elem::Tables,
) -> Result<HashMap<i32, Vec<Normal>>, Error> {
    // Iterate over records and print them
    let mut map_values: HashMap<i32, Vec<Normal>> = HashMap::new();
    // need the conversion tables
    for result in rdr.deserialize() {
        let record: NormalsRecord = result?;

        let mut elem_code = record.elem_code.as_str();
        if elem_code.starts_with("RRGRP") {
            // get rid of the numbers at the end of RRGRP, since collapsing to an array of 7
            // values instead of separate normals for each threshold
            elem_code = "RRGRP";
        }

        let mut elem_id: Option<&str> = None;
        // try to find time resolution
        let time_resolution = match (record.month, record.day) {
            (_, Some(_)) => "P1D",
            (1..13, _) => "P1M",
            (13, _) => "P1Y",
            (21..25, _) => "P3M",
            (25 | 26, _) => "P6M",
            _ => {
                eprintln!("Unknown month value in normals file: {}", record.month);
                continue;
            }
        };
        let from_to_date = format!("{}_{}", record.from_year, record.to_year);

        // try to get the element id from the elemcode
        let element_id_ref = tables.code_to_elem_table.get(elem_code);
        if let Some(elem_ids) = element_id_ref {
            for x in elem_ids {
                if x.contains(time_resolution) && x.contains(&from_to_date) {
                    // if there is an element id with %s, use that one since it has the period and frequency information we need for normals
                    elem_id = Some(x.as_str());
                    break;
                }
            }
        } else {
            eprintln!("No element id found for elem code: {}", elem_code);
            continue;
        }
        // if we have the element id, we can get the param id and then create the normal record
        if let Some(elem_id) = elem_id {
            // then get the param id from the element id
            let param_id = tables.elem_to_param_table.get(elem_id).cloned();
            // only actually use this in if there is a paramid
            if let Some(param_id) = param_id {
                if record.elem_code.starts_with("RRGRP") {
                    // check if the normal is already in the map
                    let normals = map_values.entry(record.station_id).or_default();
                    let mut found = false;
                    for normal in normals {
                        // if it is, add the normal value to the normal array
                        if normal.element_id == elem_id && normal.month == record.month {
                            // get the digit at the end of the elem code to know which index of the normal array to put the value in
                            let index = record
                                .elem_code
                                .chars()
                                .last()
                                .unwrap()
                                .to_digit(10)
                                .unwrap() as usize;
                            if let Some(normal_array) = normal.normal_array.as_mut() {
                                // modify the normal array in place since we have a mutable reference to it
                                normal_array[index] = record.normal_value;
                            }
                            found = true;
                            break;
                        }
                    }
                    // if it is not, create a new normal with the normal array and add it to the map
                    if !found {
                        let mut normal_array: [Option<f64>; RRGRP_ARRAY_SIZE] =
                            [None; RRGRP_ARRAY_SIZE];
                        let index = record
                            .elem_code
                            .chars()
                            .last()
                            .unwrap()
                            .to_digit(10)
                            .unwrap() as usize;
                        normal_array[index] = record.normal_value;
                        let normal = Normal {
                            element_id: elem_id.to_string(),
                            param_id,
                            period: from_to_date,
                            month: record.month,
                            day: record.day,
                            normal_value: None,
                            normal_array: Some(normal_array),
                        };
                        map_values
                            .entry(record.station_id)
                            .or_default()
                            .push(normal);
                    }
                } else {
                    let normal = Normal {
                        element_id: elem_id.to_string(),
                        param_id,
                        period: from_to_date,
                        month: record.month,
                        day: record.day,
                        normal_value: record.normal_value,
                        normal_array: None,
                    };
                    // insert the data
                    map_values
                        .entry(record.station_id)
                        .or_default()
                        .push(normal);
                }
            }
        }
    }
    println!("Parsed {} normals records", map_values.len());

    Ok(map_values)
}

pub fn create_normals_csv_content(
    data: HashMap<i32, Vec<Normal>>,
    normal_type: &str,
) -> Result<Vec<(String, String)>, Error> {
    let mut list_of_name_content: Vec<(String, String)> = vec![];
    // setup writer for metadata
    let mut wtr_metadata = WriterBuilder::new().has_headers(false).from_writer(vec![]);

    for (station_id, normal) in data {
        // keep the information for the metadata file
        for value in &normal {
            // keep metadata
            wtr_metadata.serialize(NormalMetadata {
                element_id: value.element_id.clone(),
                param_id: value.param_id,
                station: station_id,
            })?;
        }

        let filename = format!("{}_{}.csv", normal_type, station_id);
        // writer for data
        let mut wtr = WriterBuilder::new()
            .flexible(true)
            .has_headers(false)
            .from_writer(vec![]);

        // write to data file
        for value in &normal {
            wtr.serialize(value)?;
        }
        let data = String::from_utf8(
            wtr.into_inner()
                .map_err(|e| Error::CsvWriterError(e.to_string()))?,
        )?;
        list_of_name_content.push((filename, data));
    }
    // write metadata to file
    let metadata = String::from_utf8(
        wtr_metadata
            .into_inner()
            .map_err(|e| Error::CsvWriterError(e.to_string()))?,
    )?;
    let metadata_filename = format!("{}_metadata.csv", normal_type);
    list_of_name_content.push((metadata_filename, metadata));

    Ok(list_of_name_content)
}
