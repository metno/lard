use csv::{ReaderBuilder, WriterBuilder};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::File;
use thiserror::Error;

use lard_egress::reports::{IdfMetadata, IdfValue};

#[derive(Error, Debug)]
pub enum Error {
    #[error("CLI error: {0}")]
    CliError(String),
    #[error("Writer error {0}")]
    CsvWriterError(String),
    #[error("CSV parsing error: {0}")]
    CsvError(#[from] csv::Error),
    #[error("UTF error: {0}")]
    UtfError(#[from] std::string::FromUtf8Error),
    #[error("IO error: {0}")]
    IOError(#[from] std::io::Error),
    #[error("S3 error: {0}")]
    S3Error(#[from] s3::error::S3Error),
    #[error("env error: {0}")]
    EnvError(#[from] std::env::VarError),
}

#[derive(Debug, Deserialize)]
struct Record {
    #[serde(flatten)]
    metadata: IdfMetadata,
    #[serde(flatten)]
    value: IdfValue,
}

pub type IdfTuple = (IdfMetadata, Vec<IdfValue>);

pub fn parse_csv_file(filename: &str) -> Result<HashMap<i32, IdfTuple>, Error> {
    let file = File::open(filename)?;
    let mut rdr = ReaderBuilder::new().delimiter(b';').from_reader(file);

    // Iterate over records and print them
    let mut map_station_values: HashMap<i32, IdfTuple> = HashMap::new();
    for result in rdr.deserialize() {
        let record: Record = result?;
        //println!("{:?}", record);
        // This can become simply `record.metadata`
        let metadata: IdfMetadata = IdfMetadata {
            station_id: record.metadata.station_id,
            number_of_seasons: record.metadata.number_of_seasons,
            from_time: record.metadata.from_time,
            to_time: record.metadata.to_time,
            quality_class: record.metadata.quality_class,
            seed_parameter: record.metadata.seed_parameter,
            updated_at: record.metadata.updated_at,
        };

        // This can become simply `record.value`
        let value: IdfValue = IdfValue {
            duration: record.value.duration,
            frequency: record.value.frequency,
            intensity: record.value.intensity,
            lower_interval: record.value.lower_interval,
            upper_interval: record.value.upper_interval,
        };

        // insert the data
        map_station_values
            .entry(record.metadata.station_id)
            .or_insert((metadata, vec![]))
            .1
            .push(value);
    }
    Ok(map_station_values)
}

pub fn create_csv_content(
    data: HashMap<i32, (IdfMetadata, Vec<IdfValue>)>,
) -> Result<Vec<(String, String)>, Error> {
    let mut list_of_name_content: Vec<(String, String)> = vec![];
    // setup writer for metadata
    let mut wtr_metadata = WriterBuilder::new().has_headers(false).from_writer(vec![]);

    for (station, station_data) in data {
        // write the metatada to metadata file
        wtr_metadata.serialize(&station_data.0)?;

        let filename = format!("{station}.csv");
        // writer for data
        let mut wtr = WriterBuilder::new()
            .flexible(true)
            .has_headers(false)
            .from_writer(vec![]);
        // need metadata header
        wtr.serialize(station_data.0)?;
        // write to data file
        for value in station_data.1 {
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
    list_of_name_content.push(("metadata.csv".to_string(), metadata));

    Ok(list_of_name_content)
}
