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
    pub param_id: i32,
    pub element_id: String,
    pub station: i32,
    pub from_year: i32,
    pub to_year: i32,
}

#[cfg(feature = "integration_tests")]
impl NormalMetadata {
    pub fn new(
        element_id: String,
        param_id: i32,
        station: i32,
        from_year: i32,
        to_year: i32,
    ) -> Self {
        Self {
            element_id,
            param_id,
            station,
            from_year,
            to_year,
        }
    }
}

// define the size of the RRGRP normal arrays, which is 7 since there are 7 thresholds
const RRGRP_ARRAY_SIZE: usize = 7;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Normal {
    pub param_id: i32,
    pub element_id: String,
    pub from_year: i32,
    pub to_year: i32,
    pub normal_type: String,
    pub month: Option<i32>,
    pub day: Option<i32>,
    pub normal_value: Option<f64>,
    pub normal_array: Option<[Option<f64>; RRGRP_ARRAY_SIZE]>,
}

#[cfg(feature = "integration_tests")]
#[allow(clippy::too_many_arguments)]
impl Normal {
    pub fn new(
        param_id: i32,
        element_id: String,
        from_year: i32,
        to_year: i32,
        normal_type: String,
        month: Option<i32>,
        day: Option<i32>,
        normal_value: Option<f64>,
        normal_array: Option<[Option<f64>; RRGRP_ARRAY_SIZE]>,
    ) -> Self {
        Self {
            param_id,
            element_id,
            from_year,
            to_year,
            normal_type,
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
fn parse_normals_record(
    record: NormalsRecord,
    tables: &elem::Tables,
) -> Option<(i32, Normal, Option<usize>)> {
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
        (1..=12, _) => "P1M",
        (13, _) => "P1Y",
        (21..=24, _) => "P3M",
        (25 | 26, _) => "P6M",
        _ => {
            // TODO: Should this just panic instead?
            eprintln!("Unknown month value in normals file: {}", record.month);
            return None;
        }
    };
    let from_to_date = format!("{}_{}", record.from_year, record.to_year);

    // try to get the element id from the elemcode
    let elem_id = if let Some(ids) = tables.code_to_elem_table.get(elem_code) {
        // if there is an element id with %s, use that one since it has the
        // period and frequency information we need for normals
        ids.iter()
            .find(|id| id.contains(time_resolution) && id.contains(&from_to_date))
            .or_else(|| ids.first())
            .expect("any existing table entry must contain at least one id")
    } else {
        eprintln!("No elem_id found for elem code: {elem_code}");
        return None;
    };

    let param_id = if let Some(param_id) = tables.elem_to_param_table.get(elem_id.as_str()) {
        *param_id
    } else {
        eprintln!("No param_id found for elem id: {elem_id}");
        return None;
    };

    // handle only showing month if its =< 12, otherwise give a metadata string
    let (normal_type, month) = match (record.month, record.day) {
        (_, Some(_)) => ("diurnal", None),
        (1..=12, _) => ("monthly", Some(record.month)),
        (13, _) => ("yearly", None),
        (21, _) => ("spring", None),
        (22, _) => ("summer", None),
        (23, _) => ("autumn", None),
        (24, _) => ("winter", None),
        (25, _) => ("cold half", None),
        (26, _) => ("warm half", None),
        _ => {
            eprintln!("Unknown month value in normals file: {}", record.month);
            return None;
        }
    };

    Some((
        record.station_id,
        Normal {
            element_id: elem_id.to_string(),
            param_id,
            from_year: record.from_year,
            to_year: record.to_year,
            normal_type: normal_type.to_string(),
            month,
            day: record.day,
            normal_value: record.normal_value,
            normal_array: None,
        },
        rrgrp_index,
    ))
}

pub fn parse_normals_csv_content<R: Read>(
    rdr: &mut Reader<R>,
    tables: elem::Tables,
) -> Result<HashMap<i32, Vec<Normal>>, Error> {
    let mut sorting_vector: Vec<(i32, Normal, Option<usize>)> = rdr
        .deserialize()
        .flat_map(|record| parse_normals_record(record.expect("malformed csv record"), &tables))
        .collect();
    // sort by the station id
    sorting_vector.sort_by_key(|(s, _, _)| *s);

    // first group by station id, as that will be the map key
    let map_values: HashMap<i32, Vec<Normal>> = sorting_vector
        .into_iter()
        .chunk_by(|normal| normal.0)
        .into_iter()
        .map(|(station, chunk)| {
            // key here is month and period (from year, to year)
            // because you can have different rrgrp for each of these combinations
            let mut rrgrp_normals: HashMap<(i32, i32, i32), Normal> = HashMap::new();
            let mut normals = Vec::new();
            for (_station, mut normal, rrgrp_index) in chunk {
                // the RRGRP normals need to be merged into one normal, so we use a map
                // to track them as we merge
                if let Some(i) = rrgrp_index {
                    let value = normal.normal_value;
                    normal.normal_value = None;
                    normal.normal_array = Some([None; RRGRP_ARRAY_SIZE]);
                    let month = normal.month.expect("rrgrp normals should have month");
                    let normal = rrgrp_normals
                        .entry((month, normal.from_year, normal.to_year))
                        .or_insert(normal);
                    if let Some(arr) = normal.normal_array.as_mut() {
                        arr[i] = value
                    }
                } else {
                    normals.push(normal)
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
                from_year: value.from_year,
                to_year: value.to_year,
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
