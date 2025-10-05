use chrono::NaiveDate;
use csv::{ReaderBuilder, WriterBuilder};
use serde::Deserialize;
use std::collections::HashMap;
use std::error::Error;
use std::fs::File;
use std::fs::OpenOptions;

use lard_egress::reports::{IdfMetadata, IdfValue};

#[derive(Debug, Deserialize)]
struct Record {
    #[serde(rename = "stnr")]
    station_id: i32,
    #[serde(rename = "retlev_2.5")]
    lower_interval: f64,
    #[serde(rename = "retlev")]
    intensity: f64,
    #[serde(rename = "retlev_97.5")]
    upper_interval: f64,
    duration: u32,
    #[serde(rename = "retperiod")]
    frequency: i32,
    #[serde(rename = "FDATO")]
    from_time: String,
    #[serde(rename = "TDATO")]
    to_time: String,
    #[serde(rename = "SEASONS")]
    number_of_seasons: i32,
    #[serde(rename = "CLASS")]
    quality_class: i32,
    #[serde(rename = "UPDATE")]
    updated_at: String,
    #[serde(rename = "SEED")]
    seed_parameter: i32,
}

pub type IdfTuple = (IdfMetadata, Vec<IdfValue>);

fn convert_string_to_naivedate(date_string: &str) -> Result<NaiveDate, Box<dyn Error>> {
    let format = "%d.%m.%Y"; // DD.MM.YYYY

    match NaiveDate::parse_from_str(date_string, format) {
        Ok(naive_date) => Ok(naive_date),
        Err(e) => Err(Box::new(e)),
    }
}

pub fn parse_csv_file(filename: &str) -> Result<HashMap<i32, IdfTuple>, Box<dyn Error>> {
    let file = File::open(filename)?;
    let mut rdr = ReaderBuilder::new().delimiter(b';').from_reader(file);

    // Iterate over records and print them
    let mut last_station = 0;
    let mut map_station_values: HashMap<i32, IdfTuple> = HashMap::new();
    for result in rdr.deserialize() {
        let record: Record = result?;
        //println!("{:?}", record);
        // This can become simply `record.metadata`
        let metadata: IdfMetadata = IdfMetadata {
            station_id: record.station_id,
            number_of_seasons: record.number_of_seasons,
            from_time: convert_string_to_naivedate(&record.from_time)?,
            to_time: convert_string_to_naivedate(&record.to_time)?,
            quality_class: record.quality_class,
            seed_parameter: record.seed_parameter,
            updated_at: convert_string_to_naivedate(&record.updated_at)?,
        };

        // This can become simply `record.value`
        let value: IdfValue = IdfValue {
            duration: record.duration,
            frequency: record.frequency,
            intensity: record.intensity,
            lower_interval: record.lower_interval,
            upper_interval: record.upper_interval,
        };

        // insert the data
        map_station_values
            .entry(record.station_id)
            .or_insert((metadata, vec![]))
            .1
            .push(value);
    }
    Ok(map_station_values)
}

pub fn write_to_csv_files(
    output_path: &str,
    data: HashMap<i32, (IdfMetadata, Vec<IdfValue>)>,
) -> Result<Vec<String>, Box<dyn Error>> {
    let mut list_of_files: Vec<String> = vec![];
    // setup writer for metadata
    let metadata_filename = format!("{output_path}metadata.csv");
    let metadata_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&metadata_filename)?;
    // writer for metadata
    let mut wtr_metadata = WriterBuilder::new()
        .has_headers(false)
        .from_writer(metadata_file);

    for (station, station_data) in data {
        // write the metatada to metadata file
        wtr_metadata.serialize(&station_data.0)?;
        let name = format!("{station}.csv");
        list_of_files.push(name.clone());
        let filename = format!("{output_path}{name}");
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&filename)?;
        // writer for data
        let mut wtr = WriterBuilder::new()
            .flexible(true)
            .has_headers(false)
            .from_writer(file);
        // need metadata header
        wtr.serialize(station_data.0)?;
        // write to data file
        for value in station_data.1 {
            wtr.serialize(value)?;
        }
        wtr.flush()?;
    }
    wtr_metadata.flush()?;

    Ok(list_of_files)
}
