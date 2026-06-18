use csv::{Reader, ReaderBuilder, WriterBuilder};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs::File, io::Read};
use tracing::warn;

use crate::deserialize::record_date;
use crate::{idf_parse::Error, stinfofacade::elem};

// We have both the basepath for putting dated folders with the parsed
// files into, as well as the path to latest which is used by the
// reports endpoint as the location to find the files.
pub const RECORDS_S3_BASEPATH: &str = "/lard_reports/records/";
pub const RECORDS_S3_PATH: &str = "/lard_reports/records/latest/";

// This is the form that we currently get the records in,
// as in how they are exported from KDVH
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct KdvhRecord {
    #[serde(alias = "STNR")]
    pub stnr: i32,
    #[serde(alias = "DATO_D", deserialize_with = "record_date")]
    pub date: chrono::NaiveDate,
    #[serde(alias = "ELEM_CODE")]
    pub elem_code: String,
    #[serde(alias = "RECORD")]
    pub value: f64,
}

// This is the form we distribute them in, once we convert the elem_code to a param_id
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Record {
    pub station_nr: i32,
    pub param_id: i32,
    pub date: chrono::NaiveDate,
    pub value: f64,
}

pub fn parse_records_csv_file(filename: &str) -> Result<Vec<KdvhRecord>, Error> {
    let file = File::open(filename)?;
    let mut rdr = ReaderBuilder::new().delimiter(b',').from_reader(file);

    parse_records_csv_content(&mut rdr)
}

pub fn parse_records_csv_content<R: Read>(rdr: &mut Reader<R>) -> Result<Vec<KdvhRecord>, Error> {
    let mut records: Vec<KdvhRecord> = Vec::new();
    for result in rdr.deserialize() {
        let record: KdvhRecord = result?;
        // insert the data
        records.push(record);
    }
    Ok(records)
}

pub fn create_records_csv_content(
    data: &[KdvhRecord],
    tables: &elem::Tables,
) -> Result<Vec<(String, String)>, Error> {
    let mut list_of_name_content: Vec<(String, String)> = vec![];

    // get the unique elem codes in the records
    let mut elem_codes = data
        .iter()
        .map(|record| record.elem_code.clone())
        .collect::<Vec<String>>();
    elem_codes.sort(); // do we need to sort?
    elem_codes.dedup();

    // first find the conversion from elem code to param id for each of the elem codes in the records
    let mut elem_code_to_param_id: HashMap<String, i32> = HashMap::new();

    for ec in &elem_codes {
        let elem_id = tables.code_to_elem_table.get(&ec.to_string());

        if let Some(element) = elem_id {
            if element.len() > 1 {
                warn!(
                    "Multiple elements found for elem code {}: {:?}",
                    ec, element
                );

                let mut p1d_matches = element.iter().filter(|x| x.contains("P1D"));
                match (p1d_matches.next(), p1d_matches.next()) {
                    (Some(p1d_element), None) => {
                        if let Some(param_id) = tables.elem_to_param_table.get(p1d_element).copied()
                        {
                            elem_code_to_param_id.insert(ec.to_string(), param_id);
                        } else {
                            warn!(
                                "Could not find param id for elem code {} with mapped elem {}",
                                ec, p1d_element
                            );
                        }
                    }
                    (Some(_), Some(_)) => {
                        warn!(
                            "Multiple P1D elements found for elem code {}, skipping ambiguous mapping",
                            ec
                        );
                    }
                    _ => {
                        warn!(
                            "No P1D element found for elem code {}, skipping mapping",
                            ec
                        );
                    }
                }
            } else if let Some(first_elem) = element.first() {
                if let Some(param_id) = tables.elem_to_param_table.get(first_elem).copied() {
                    elem_code_to_param_id.insert(ec.to_string(), param_id);
                } else {
                    warn!(
                        "Could not find param id for elem code {} with mapped elem {}",
                        ec, first_elem
                    );
                }
            }
        } else {
            warn!("No element mapping found for elem code {}, skipping", ec);
        }
    }
    // then keep only the records that have an elem code that maps to a param id,
    // and create the content for each of those files
    // setup writer for metadata
    let mut wtr_metadata = WriterBuilder::new().has_headers(false).from_writer(vec![]);
    let mut mappings = elem_code_to_param_id.iter().collect::<Vec<_>>();
    mappings.sort_by_key(|(_elem_code, param_id)| *param_id);

    for (elem_code, param_id) in mappings {
        let mut wtr = WriterBuilder::new()
            .flexible(true)
            .has_headers(false)
            .from_writer(vec![]);
        for kdvhrecord in data {
            if kdvhrecord.elem_code == *elem_code {
                let record = Record {
                    station_nr: kdvhrecord.stnr,
                    param_id: *param_id,
                    date: kdvhrecord.date,
                    value: kdvhrecord.value,
                };
                wtr.serialize(record)?;
            }
        }
        // create the content for the file
        let content = String::from_utf8(
            wtr.into_inner()
                .map_err(|e| Error::CsvWriterError(e.to_string()))?,
        )?;
        // create the name for the file
        let name = format!("records_{}.csv", param_id);
        list_of_name_content.push((name, content));
        // add param to metadata file
        wtr_metadata.serialize(param_id)?;
    }
    // write metadata to file
    let metadata = String::from_utf8(
        wtr_metadata
            .into_inner()
            .map_err(|e| Error::CsvWriterError(e.to_string()))?,
    )?;
    list_of_name_content.push(("metadata.csv".to_string(), metadata));
    Ok(list_of_name_content)
}
