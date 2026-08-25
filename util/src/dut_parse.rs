use csv::{Reader, ReaderBuilder, WriterBuilder};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fs::File, io::Read};

use crate::deserialize::{dut_season, idf_date};
use crate::idf_parse::{Error, IdfValue};

// We have both the basepath for putting dated folders with the parsed
// files into, as well as the path to latest which is used by the
// reports endpoint as the location to find the files.
pub const DUT_S3_BASEPATH: &str = "/lard_reports/dut/";
pub const DUT_S3_PATH: &str = "/lard_reports/dut/latest/";

/// Season magic numbers used at MET
#[derive(Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Season {
    Spring = 21,
    Summer = 22,
    Autumn = 23,
    Winter = 24,
}

/// Metadata and parameters used for fitting IDF values
// NOTE: the same as IdfMetadata except that here `station_id` becomes `municipality_id`
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DutMetadata {
    /// Norwegian municipality identifier
    #[serde(alias = "stnr")]
    pub municipality_id: i32,
    /// Number of years considered in the calculation
    /// In Norway, the most severe rainfall events usually fall in the May-September period,
    /// so if the data coverage in this period is below 80% the year is skipped
    #[serde(alias = "SEASONS")]
    pub number_of_seasons: i32,
    /// First date considered in the precipitation timeseries
    #[serde(alias = "FDATO", deserialize_with = "idf_date")]
    pub from_time: chrono::NaiveDate,
    /// Last date considered in the precipitation timeseries
    #[serde(alias = "TDATO", deserialize_with = "idf_date")]
    pub to_time: chrono::NaiveDate,
    /// RNG seed used in the calculation
    #[serde(alias = "SEED")]
    pub seed_parameter: i32,
    /// When the calculation was carried out
    #[serde(alias = "UPDATE", deserialize_with = "idf_date")]
    pub updated_at: chrono::NaiveDate,
}

impl DutMetadata {
    pub fn new(
        municipality_id: i32,
        number_of_seasons: i32,
        from_time: chrono::NaiveDate,
        to_time: chrono::NaiveDate,
        seed_parameter: i32,
        updated_at: chrono::NaiveDate,
    ) -> Self {
        Self {
            municipality_id,
            number_of_seasons,
            from_time,
            to_time,
            seed_parameter,
            updated_at,
        }
    }
}

// Similar to IdfRecord, but it includes different sets of idf values per season
#[derive(Debug, Serialize, Deserialize)]
pub struct DutRecord {
    #[serde(flatten)]
    pub metadata: DutMetadata,
    #[serde(flatten)]
    pub value: IdfValue,
    // Which season this value is
    #[serde(alias = "time_of_year", deserialize_with = "dut_season")]
    pub season: Season,
    // Unused
    #[serde(alias = "REF_period")]
    reference_period: String,
}

pub type DutTuple = (DutMetadata, Vec<(Season, IdfValue)>);

pub fn parse_dut_csv_content<R: Read>(
    rdr: &mut Reader<R>,
) -> Result<HashMap<i32, DutTuple>, Error> {
    // Iterate over records and print them
    let mut map_values: HashMap<i32, DutTuple> = HashMap::new();
    for result in rdr.deserialize() {
        let record: DutRecord = result?;
        // insert the data
        map_values
            .entry(record.metadata.municipality_id)
            .or_insert((record.metadata, vec![]))
            .1
            .push((record.season, record.value));
    }
    Ok(map_values)
}

pub fn parse_dut_csv_file(filename: &str) -> Result<HashMap<i32, DutTuple>, Error> {
    let file = File::open(filename)?;
    let mut rdr = ReaderBuilder::new().delimiter(b',').from_reader(file);

    parse_dut_csv_content(&mut rdr)
}

pub fn create_dut_csv_content(
    data: HashMap<i32, DutTuple>,
) -> Result<Vec<(String, String)>, Error> {
    let mut list_of_name_content: Vec<(String, String)> = vec![];
    // setup writer for metadata
    let mut wtr_metadata = WriterBuilder::new().has_headers(false).from_writer(vec![]);

    for (municipality, dut_data) in data {
        // write the metatada to metadata file
        wtr_metadata.serialize(&dut_data.0)?;

        let filename = format!("{municipality}.csv");
        // writer for data
        let mut wtr = WriterBuilder::new()
            .flexible(true)
            .has_headers(false)
            .from_writer(vec![]);
        // need metadata header
        wtr.serialize(dut_data.0)?;
        // write to data file
        for value in dut_data.1 {
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
