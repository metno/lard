use chrono::NaiveDate;
use csv::{ReaderBuilder, WriterBuilder};
use serde::Deserialize;
use std::error::Error;
use std::fs::File;
use std::fs::OpenOptions;
use std::path::Path;

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

fn convert_string_to_naivedate(date_string: &str) -> Result<NaiveDate, Box<dyn Error>> {
    let format = "%d.%m.%Y"; // DD.MM.YYYY

    match NaiveDate::parse_from_str(date_string, format) {
        Ok(naive_date) => Ok(naive_date),
        Err(e) => Err(Box::new(e)),
    }
}

pub fn parse_csv_file<P: AsRef<Path>>(
    filename: P,
    output_path: &str,
) -> Result<(), Box<dyn Error>> {
    let file = File::open(filename)?;
    let mut rdr = ReaderBuilder::new().delimiter(b';').from_reader(file);

    // Iterate over records and print them
    let mut last_station = 0;
    for result in rdr.deserialize() {
        let record: Record = result?;
        //println!("{:?}", record);
        let filename = record.station_id.to_string() + ".csv";
        let mut path = output_path.to_string();
        path.push_str(&filename);
        if last_station != record.station_id {
            //println!("station: {:?}", record.station_id);
            last_station = record.station_id;
            // start new file
            let metadata: IdfMetadata = IdfMetadata {
                station_id: record.station_id,
                number_of_seasons: record.number_of_seasons,
                from_time: convert_string_to_naivedate(&record.from_time)?,
                to_time: convert_string_to_naivedate(&record.to_time)?,
                quality_class: record.quality_class,
                seed_parameter: record.seed_parameter,
                updated_at: convert_string_to_naivedate(&record.updated_at)?,
            };
            // write the metadata header to file
            write_header_of_csv_file(&path, metadata)?;
        }
        // write data
        let value: IdfValue = IdfValue {
            duration: record.duration,
            frequency: record.frequency,
            intensity: record.intensity,
            lower_interval: record.lower_interval,
            upper_interval: record.upper_interval,
        };
        write_to_csv_file(path, value)?;
    }
    Ok(())
}
fn write_header_of_csv_file<P: AsRef<Path>>(
    filename: P,
    metadata: IdfMetadata,
) -> Result<(), Box<dyn Error>> {
    let mut wtr = WriterBuilder::new()
        .has_headers(false)
        .from_path(filename)?;

    wtr.serialize(metadata)?;

    wtr.flush()?;
    Ok(())
}

fn write_to_csv_file<P: AsRef<Path>>(filename: P, value: IdfValue) -> Result<(), Box<dyn Error>> {
    // append to the file, its should be created when make the header
    let file = OpenOptions::new().append(true).open(filename)?;

    let mut wtr = WriterBuilder::new().has_headers(false).from_writer(file);

    wtr.serialize(value)?;

    wtr.flush()?;
    Ok(())
}
