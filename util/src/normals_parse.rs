use std::{collections::HashMap, fs::File, io::Read, str::FromStr};

use csv::{Reader, ReaderBuilder};
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
    pub normal_type: NormalType,
    pub value: Value,
}

#[cfg(feature = "integration_tests")]
#[allow(clippy::too_many_arguments)]
impl Normal {
    pub fn new(
        param_id: i32,
        element_id: String,
        from_year: i32,
        to_year: i32,
        normal_type: NormalType,
        normal_value: Option<f64>,
        normal_array: Option<[Option<f64>; RRGRP_ARRAY_SIZE]>,
    ) -> Self {
        Self {
            param_id,
            element_id,
            from_year,
            to_year,
            normal_type,
            value: match (normal_value, normal_array) {
                (Some(v), None) => Value::Single(v),
                (None, Some(arr)) => Value::Array(arr),
                _ => panic!("Invalid combination of normal_value and normal_array"),
            },
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Value {
    Single(f64),
    Array([Option<f64>; RRGRP_ARRAY_SIZE]),
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Season {
    Spring,
    Summer,
    Autumn,
    Winter,
    Unknown,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum Half {
    Cold,
    Warm,
    Unknown,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
#[serde(tag = "type", content = "value")]
pub enum NormalType {
    Diurnal(i32), // Day of month
    Monthly(i32), // Month of year
    Seasonal(Season),
    Biannually(Half),
    Annually,
}

impl NormalType {
    fn from_record(record: &NormalsRecord) -> Result<Self, Error> {
        let normal_type = match (record.month, record.day) {
            (_, Some(day)) => NormalType::Diurnal(day),
            (1..=12, _) => NormalType::Monthly(record.month),
            (13, _) => NormalType::Annually,
            (21, _) => NormalType::Seasonal(Season::Spring),
            (22, _) => NormalType::Seasonal(Season::Summer),
            (23, _) => NormalType::Seasonal(Season::Autumn),
            (24, _) => NormalType::Seasonal(Season::Winter),
            (25, _) => NormalType::Biannually(Half::Cold),
            (26, _) => NormalType::Biannually(Half::Warm),
            _ => {
                return Err(Error::ParseError(format!(
                    "Unknown month value in normals file: {}",
                    record.month
                )));
            }
        };

        Ok(normal_type)
    }
    fn time_resolution(&self) -> &str {
        match self {
            NormalType::Diurnal(_) => "P1D",
            NormalType::Monthly(_) => "P1M",
            NormalType::Seasonal(_) => "P3M",
            NormalType::Biannually(_) => "P6M",
            NormalType::Annually => "P1Y",
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

    // get the normal type from the record
    let normal_type = NormalType::from_record(&record).expect("failed to parse normal type");

    // find time resolution
    let time_resolution = normal_type.time_resolution();

    // try to get the element id from the elemcode
    let from_to_date = format!("{}_{}", record.from_year, record.to_year);
    let Some(ids) = tables.code_to_elem_table.get(elem_code) else {
        eprintln!("No elem_id found for elem code: {elem_code}");
        return None;
    };
    // if there is an element id with %s, use that one since it has the
    // period and frequency information we need for normals
    let elem_id = ids
        .iter()
        .find(|id| id.contains(time_resolution) && id.contains(&from_to_date))
        .or_else(|| ids.first())
        .expect("any existing table entry must contain at least one id");

    let Some(param_id) = tables.elem_to_param_table.get(elem_id.as_str()) else {
        eprintln!("No param_id found for elem id: {elem_id}");
        return None;
    };

    Some((
        record.station_id,
        Normal {
            element_id: elem_id.to_string(),
            param_id: *param_id,
            from_year: record.from_year,
            to_year: record.to_year,
            normal_type,
            value: match record.normal_value {
                Some(v) => Value::Single(v),
                None => Value::Array([None; RRGRP_ARRAY_SIZE]),
            },
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
                    let value = match normal.value {
                        Value::Single(v) => Some(v),
                        Value::Array(_) => None,
                    };
                    normal.value = Value::Array([None; RRGRP_ARRAY_SIZE]);
                    let month = match normal.normal_type {
                        NormalType::Monthly(m) => m,
                        _ => panic!("rrgrp normals should have month"),
                    };
                    let normal = rrgrp_normals
                        .entry((month, normal.from_year, normal.to_year))
                        .or_insert(normal);
                    if let Value::Array(arr) = &mut normal.value {
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

pub fn create_normals_json_content(
    data: HashMap<i32, Vec<Normal>>,
    normal_type: &str,
) -> Result<Vec<(String, String)>, Error> {
    let mut list_of_name_content: Vec<(String, String)> = vec![];
    let mut metadata: Vec<NormalMetadata> = Vec::new();

    for (station_id, normal) in data {
        // keep the information for the metadata file
        for value in &normal {
            metadata.push(NormalMetadata {
                element_id: value.element_id.clone(),
                param_id: value.param_id,
                station: station_id,
                from_year: value.from_year,
                to_year: value.to_year,
            });
        }

        let filename = format!("{}_{}.json", normal_type, station_id);
        let data = serde_json::to_string(&normal)
            .map_err(|e| Error::ParseError(format!("failed to serialize normals json: {e}")))?;
        list_of_name_content.push((filename, data));
    }

    let metadata = serde_json::to_string(&metadata)
        .map_err(|e| Error::ParseError(format!("failed to serialize metadata json: {e}")))?;
    let metadata_filename = format!("{}_metadata.json", normal_type);
    list_of_name_content.push((metadata_filename, metadata));

    Ok(list_of_name_content)
}
