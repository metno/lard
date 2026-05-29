use std::{collections::HashMap, fs::File, io::Read, str::FromStr};

use csv::{Reader, ReaderBuilder, WriterBuilder};
use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::{idf_parse::Error, stinfofacade::elem};

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

/// In between type of Record and normal, so we can separate parsing the records
/// from clustering RRGRP
#[derive(Debug)]
pub struct NormalFlat {
    pub station_id: i32,
    pub element_id: String,
    pub param_id: i32,
    pub period: String,
    pub month: i32,
    pub day: Option<i32>,
    pub rrgrp_index: Option<usize>,
    pub normal_value: Option<f64>,
}

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

/// Documentation comments for use of month:
/// 13: yearly values
/// 21: spring (Mar-May)
/// 22: summer (Jun-Aug)
/// 23: autumn (Sep-Nov)
/// 24: winter (Dec–Feb)
/// 25: cold half (TODO: not sure about exact months/dates)
/// 26: warm half (TODO: not sure about exact months/dates)
fn parse_normals_record(record: NormalsRecord, tables: &elem::Tables) -> Option<NormalFlat> {
    let (elem_code, rrgrp_index) = if let Some(suffix) = record.elem_code.strip_prefix("RRGRP") {
        (
            "RRGRP",
            Some(usize::from_str(suffix).expect("param starting with RRGRP must end in digit")),
        )
    } else {
        (record.elem_code.as_str(), None)
    };

    // try to find time resolution
    let time_resolution = match (record.month, record.day) {
        (_, Some(_)) => "P1D",
        (1..13, _) => "P1M",
        (13, _) => "P1Y",
        (21..25, _) => "P3M",
        (25 | 26, _) => "P6M",
        _ => {
            // TODO: Should this just panic instead?
            eprintln!("Unknown month value in normals file: {}", record.month);
            return None;
        }
    };
    let from_to_date = format!("{}_{}", record.from_year, record.to_year);

    // try to get the element id from the elemcode
    let elem_id: Option<String> = tables
        .code_to_elem_table
        .get(elem_code)
        // if there is an element id with %s, use that one since it has the
        // period and frequency information we need for normals
        .and_then(|ids| {
            ids.iter()
                .find(|id| id.contains(time_resolution) && id.contains(&from_to_date))
        })
        .cloned();

    let param_id: Option<i32> = elem_id
        .as_ref()
        .and_then(|elem_id| tables.elem_to_param_table.get(elem_id.as_str()).copied());

    // TODO: log the `None` case?
    param_id.map(|param_id| NormalFlat {
        station_id: record.station_id,
        element_id: elem_id.unwrap(),
        param_id,
        period: from_to_date,
        month: record.month,
        day: record.day,
        rrgrp_index,
        normal_value: record.normal_value,
    })
}

pub fn parse_normals_csv_content<R: Read>(
    rdr: &mut Reader<R>,
    tables: elem::Tables,
) -> Result<HashMap<i32, Vec<Normal>>, Error> {
    let map_values: HashMap<i32, Vec<Normal>> = rdr
        .deserialize()
        .flat_map(|record| parse_normals_record(record.expect("malformed csv record"), &tables))
        // first group by station id, as that will be the map key
        // TODO: assumes records come ordered by station, if not we need to sort
        .chunk_by(|normal| normal.station_id)
        .into_iter()
        .map(|(station, records)| {
            // key here is month
            // TODO: are we sure they only collide on month?
            let mut rrgrp_normals: HashMap<i32, Normal> = HashMap::new();
            let mut normals = Vec::new();
            for record in records {
                if let Some(i) = record.rrgrp_index {
                    let normal = rrgrp_normals.entry(record.month).or_insert(Normal {
                        element_id: record.element_id,
                        param_id: record.param_id,
                        period: record.period,
                        month: record.month,
                        day: record.day,
                        normal_value: None,
                        normal_array: Some([None; RRGRP_ARRAY_SIZE]),
                    });
                // the RRGRP normals need to be merged into one normal, so we use a map
                    if let Some(arr) = normal.normal_array.as_mut() {
                        arr[i] = record.normal_value
                    }
                } else {
                    normals.push(Normal {
                        element_id: record.element_id,
                        param_id: record.param_id,
                        period: record.period,
                        month: record.month,
                        day: record.day,
                        normal_value: record.normal_value,
                        normal_array: None,
                    })
                }
            }
            // put the now merged RRGRP normals into the main vec
            normals.extend(rrgrp_normals.into_values());

            (station, normals)
        })
        .collect();

    println!("Parsed {} normals records", map_values.len());

    Ok(map_values)
}

pub fn parse_normals_csv_file(
    filename: &str,
    elem_tables: elem::Tables,
) -> Result<HashMap<i32, Vec<Normal>>, Error> {
    let file = File::open(filename)?;
    let mut rdr = ReaderBuilder::new().delimiter(b',').from_reader(file);

    parse_normals_csv_content(&mut rdr, elem_tables)
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
