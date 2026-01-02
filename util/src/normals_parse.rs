use csv::{ReaderBuilder, WriterBuilder};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::sync::LazyLock;

use crate::idf_parse::Error;

pub const NORMALS_S3_BASEPATH: &str = "/lard_reports/normals/";
pub const NORMALS_S3_PATH: &str = "/lard_reports/normals/latest/";

#[derive(Debug, Serialize, Deserialize)]
pub struct NormalsRecord {
    #[serde(alias = "STNR")]
    pub station_id: i32,
    #[serde(alias = "MONTH")]
    pub month: i32,
    #[serde(alias = "DAY")]
    pub day: Option<i32>,
    #[serde(alias = "ELEM_CODE")]
    pub elem_code: String,
    #[serde(alias = "NORMAL")]
    pub normal_value: Option<f64>,
    #[serde(alias = "FYEAR")]
    pub from_year: i32,
    #[serde(alias = "TYEAR")]
    pub to_year: i32,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct NormalMetadata {
    pub station_id: i32,
    // a comma separated list of available elements for the station
    // can't use vec<String> since we want to write to csv
    // (and it can't handle that they are different lengths)
    pub available_elements: String,
}

#[cfg(feature = "integration_tests")]
impl NormalMetadata {
    pub fn new(station_id: i32, available_elements: String) -> Self {
        Self {
            station_id,
            available_elements,
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Normal {
    pub month: i32,
    pub elem_id: String,
    pub normal_value: Option<f64>,
    pub from_year: i32,
    pub to_year: i32,
}

#[cfg(feature = "integration_tests")]
impl Normal {
    pub fn new(
        month: i32,
        elem_id: String,
        normal_value: f64,
        from_year: i32,
        to_year: i32,
    ) -> Self {
        Self {
            month,
            elem_id,
            normal_value: Some(normal_value),
            from_year,
            to_year,
        }
    }
}

// NormalsMapMonth maps ElemCode from KDVH to ElementID/NormalID in ODA
// note: DDR_GE1 was changed to DRR_GE1 since that is how it appears in the csv file
// appear to be missing conversion for GD17 (without _I)
static NORMALS_ELEM_MAP: LazyLock<HashMap<&'static str, &'static str>> = LazyLock::new(|| {
    let mut map = HashMap::new();
    // monthly normals
    map.insert(
        "DRR_GE1",
        "number_of_days_gte(sum(precipitation_amount P1D) %s 1.0)",
    );
    map.insert(
        "GD17_I",
        "integral_of_deficit_interpolated(mean(air_temperature P1D) %s 17.0)",
    );
    map.insert("OT", "sum(duration_of_sunshine %s)");
    map.insert("POM", "mean(surface_air_pressure %s)");
    map.insert("PRM", "mean(air_pressure_at_sea_level %s)");
    map.insert("RR", "sum(precipitation_amount %s)");
    map.insert(
        "RRGRP0",
        "frequency_group_thresholds(precipitation_amount %s threshold0)",
    );
    map.insert(
        "RRGRP1",
        "frequency_group_thresholds(precipitation_amount %s threshold1)",
    );
    map.insert(
        "RRGRP2",
        "frequency_group_thresholds(precipitation_amount %s threshold2)",
    );
    map.insert(
        "RRGRP3",
        "frequency_group_thresholds(precipitation_amount %s threshold3)",
    );
    map.insert(
        "RRGRP4",
        "frequency_group_thresholds(precipitation_amount %s threshold4)",
    );
    map.insert(
        "RRGRP5",
        "frequency_group_thresholds(precipitation_amount %s threshold5)",
    );
    map.insert(
        "RRGRP6",
        "frequency_group_thresholds(precipitation_amount %s threshold6)",
    );
    map.insert("TAM", "mean(air_temperature %s)");
    map.insert(
        "TAM_DAY_STDEV",
        "standard_deviation(mean(air_temperature P1D) %s)",
    );
    map.insert("TANM", "mean(min(air_temperature P1D) %s)");
    map.insert("TAXM", "mean(max(air_temperature P1D) %s)");
    map.insert("UM", "mean(relative_humidity %s)");
    // diurnal normals
    map.insert("TAM", "mean(air_temperature P1D)");
    map.insert("RR_ACC", "sum_until_day_of_year(precipitation_amount P1D)");
    map
});

pub fn parse_normals_csv_file(filename: &str) -> Result<HashMap<i32, Vec<Normal>>, Error> {
    let file = File::open(filename)?;
    let mut rdr = ReaderBuilder::new().delimiter(b',').from_reader(file);

    // Iterate over records and print them
    let mut map_values: HashMap<i32, Vec<Normal>> = HashMap::new();
    for result in rdr.deserialize() {
        let record: NormalsRecord = result?;

        let mut elem_id = match NORMALS_ELEM_MAP.get(record.elem_code.as_str()) {
            Some(id) => id.to_string(),
            None => {
                // commenting out error to be able to parse files with unknown elem codes
                // currently for example GD17 which we don't have mapping for
                /*
                return Err(Error::ParseError(format!(
                    "Unknown ElemCode in normals file: {}",
                    record.elem_code
                )));
                */
                eprintln!("Unknown ElemCode in normals file: {}", record.elem_code);
                continue;
            }
        };
        // then change the %s to a period based on month
        if record.month < 13 {
            if let Some(index) = elem_id.find("%s") {
                elem_id.replace_range(index..index + 2, "P1M");
            }
        } else if record.month == 13 {
            if let Some(index) = elem_id.find("%s") {
                elem_id.replace_range(index..index + 2, "P1Y");
            }
        } else if record.month == 21
            || record.month == 22
            || record.month == 23
            || record.month == 24
        {
            if let Some(index) = elem_id.find("%s") {
                elem_id.replace_range(index..index + 2, "P3M");
            }
        } else if record.month == 25 || record.month == 26 {
            if let Some(index) = elem_id.find("%s") {
                elem_id.replace_range(index..index + 2, "P6M");
            }
        } else {
            // should return ParseError?
            eprintln!("Unknown month value in normals file: {}", record.month);
        }

        let normal = Normal {
            month: record.month,
            elem_id,
            normal_value: record.normal_value,
            from_year: record.from_year,
            to_year: record.to_year,
        };
        // insert the data
        map_values
            .entry(record.station_id)
            .or_default()
            .push(normal);
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
        // write the metatada to metadata file
        // flatten all elements for the station
        let mut available_elements = normal
            .iter()
            .map(|n| n.elem_id.clone())
            .collect::<Vec<String>>();
        // remove duplicates
        available_elements.dedup();
        let metadata = NormalMetadata {
            station_id,
            available_elements: available_elements.join(","),
        };
        wtr_metadata.serialize(&metadata)?;

        let filename = format!("{}_{}.csv", normal_type, station_id);
        // writer for data
        let mut wtr = WriterBuilder::new()
            .flexible(true)
            .has_headers(false)
            .from_writer(vec![]);

        // write to data file
        for value in normal {
            wtr.serialize(value)?;
        }
        let data = String::from_utf8(
            wtr.into_inner()
                .map_err(|e| Error::CsvWriterError(e.to_string()))?,
        )?;
        list_of_name_content.push((filename, data));
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
