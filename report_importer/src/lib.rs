use csv::{ReaderBuilder, WriterBuilder};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;

use csv::Error;
use lard_egress::reports::{IdfMetadata, IdfValue};

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

pub fn write_to_csv_files(
    output_path: &str,
    data: HashMap<i32, (IdfMetadata, Vec<IdfValue>)>,
) -> Result<Vec<String>, Error> {
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
        list_of_files.push(name);
        wtr.flush()?;
    }
    wtr_metadata.flush()?;

    Ok(list_of_files)
}
