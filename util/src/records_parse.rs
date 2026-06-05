use csv::{Reader, ReaderBuilder, WriterBuilder};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs::File, io::Read};

use crate::deserialize::record_date;
use crate::{idf_parse::Error, stinfofacade::elem};

// We have both the basepath for putting dated folders with the parsed
// files into, as well as the path to latest which is used by the
// reports endpoint as the location to find the files.
pub const RECORDS_S3_BASEPATH: &str = "/lard_reports/records/";
pub const RECORDS_S3_PATH: &str = "/lard_reports/records/latest/";

//
#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct Record {
    #[serde(alias = "STNR")]
    pub stnr: i32,
    #[serde(alias = "DATO_D", deserialize_with = "record_date")]
    pub date: chrono::NaiveDate,
    #[serde(alias = "ELEM_CODE")]
    pub elem_code: String,
    #[serde(alias = "RECORD")]
    pub value: f64,
}

pub fn parse_records_csv_file(filename: &str) -> Result<Vec<Record>, Error> {
    let file = File::open(filename)?;
    let mut rdr = ReaderBuilder::new().delimiter(b',').from_reader(file);

    parse_records_csv_content(&mut rdr)
}

pub fn parse_records_csv_content<R: Read>(rdr: &mut Reader<R>) -> Result<Vec<Record>, Error> {
    let mut records: Vec<Record> = Vec::new();
    for result in rdr.deserialize() {
        let record: Record = result?;
        // insert the data
        records.push(record);
    }
    println!("Parsed {} records", records.len());
    Ok(records)
}

pub fn create_records_csv_content(
    data: &[Record],
    tables: &elem::Tables,
) -> Result<Vec<(String, String)>, Error> {
    let mut list_of_name_content: Vec<(String, String)> = vec![];

    let mut elem_codes = data
        .iter()
        .map(|record| record.elem_code.clone())
        .collect::<Vec<String>>();
    elem_codes.sort();
    elem_codes.dedup();
    //println!("Unique elem codes in the records: {:?}", elem_codes);

    // first find the conversion from elem code to param id for each of the elem codes in the records
    let mut elem_code_to_param_id: HashMap<String, Option<i32>> = HashMap::new();

    for ec in &elem_codes {
        let elem_id = tables.code_to_elem_table.get(&ec.to_string());

        if let Some(element) = elem_id {
            if element.len() > 1 {
                println!(
                    "Multiple elements found for elem code {}: {:?}",
                    ec, element
                );
                // Find the first element containing "P1D" and insert its param_id
                if let Some(p1d_element) = element.iter().find(|x| x.contains("P1D")) {
                    let param_id = tables.elem_to_param_table.get(p1d_element).cloned();
                    elem_code_to_param_id.insert(ec.to_string(), param_id);
                }
            } else if let Some(first_elem) = element.first() {
                let param_id = tables.elem_to_param_table.get(first_elem).cloned();
                elem_code_to_param_id.insert(ec.to_string(), param_id);
            }
            // else have not found it... but we do nothing in that case
        }
    }
    // then keep only the records that have an elem code that maps to a param id,
    // and create the content for each of those files
    // setup writer for metadata
    let mut wtr_metadata = WriterBuilder::new().has_headers(false).from_writer(vec![]);
    for x in elem_code_to_param_id.iter() {
        println!("Elem code {} maps to param id {:?}", x.0, x.1);
        let mut wtr = WriterBuilder::new()
            .flexible(true)
            .has_headers(false)
            .from_writer(vec![]);
        for record in data {
            if record.elem_code == *x.0 {
                wtr.serialize(record)?;
            }
        }
        // create the content for the file
        let content = String::from_utf8(
            wtr.into_inner()
                .map_err(|e| Error::CsvWriterError(e.to_string()))?,
        )?;
        // create the name for the file
        let name = format!("records_{}.csv", x.1.unwrap_or(-1));
        list_of_name_content.push((name, content));
        // add param to metadata file
        wtr_metadata.serialize(x.1.unwrap_or(-1))?;
    }
    let metadata = String::from_utf8(
        wtr_metadata
            .into_inner()
            .map_err(|e| Error::CsvWriterError(e.to_string()))?,
    )?;
    list_of_name_content.push(("metadata.csv".to_string(), metadata));
    Ok(list_of_name_content)
}
