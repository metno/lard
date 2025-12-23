use csv::{ReaderBuilder, WriterBuilder};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use thiserror::Error;

use crate::deserialize::idf_date;

// We have both the basepath for putting dated folders with the parsed
// files into, as well as the path to latest which is used by the
// reports endpoint as the location to find the files.
pub const IDF_S3_BASEPATH: &str = "/lard_reports/idf/";
pub const IDF_S3_PATH: &str = "/lard_reports/idf/latest/";

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
    #[error("parse error: {0}")]
    ParseError(String),
}

/// Precipitation intensity values fitted from a GEV distribution on annual precipitation timeseries.
/// More information can be found [here](https://doi.org/10.1016/j.jhydrol.2021.127000).
/// The code responsible for generating these values can be found [here](https://github.com/ClimDesign/fixIDF).
// TODO: make more general, this struct can be used for different types of observations
#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IdfValue {
    /// Duration of the precipitation event [min]
    pub duration: u32,
    /// Expected time between events of computed intensity [years]
    #[serde(alias = "retperiod")]
    pub frequency: i32,
    /// Computed rainfall intensity value [mm]
    #[serde(alias = "retlev")]
    pub intensity: f64,
    /// 0.025 quantile of computed rainfall intensity [mm]
    #[serde(alias = "retlev_2.5")]
    pub lower_interval: f64,
    /// 0.975 quantile of computed rainfall intensity [mm]
    #[serde(alias = "retlev_97.5")]
    pub upper_interval: f64,
}

#[cfg(feature = "integration_tests")]
impl IdfValue {
    pub fn new(
        duration: u32,
        frequency: i32,
        intensity: f64,
        lower_interval: f64,
        upper_interval: f64,
    ) -> Self {
        Self {
            duration,
            frequency,
            intensity,
            lower_interval,
            upper_interval,
        }
    }
}

/// Metadata and parameters used for fitting IDF values
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IdfMetadata {
    /// MET station identifier
    #[serde(alias = "stnr")]
    pub station_id: i32,
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
    /// Robustness of the estimated IDF values, computed by running multiple IDF estimations and
    /// comparing the convergence of their results. Currently only three values are possible:
    /// 1 (robust), 2 (uncertain), 3 (very uncertain)
    #[serde(alias = "CLASS")]
    pub quality_class: i32,
    /// RNG seed used in the calculation
    #[serde(alias = "SEED")]
    pub seed_parameter: i32,
    /// When the calculation was carried out
    #[serde(alias = "UPDATE", deserialize_with = "idf_date")]
    pub updated_at: chrono::NaiveDate,
}

#[cfg(feature = "integration_tests")]
impl IdfMetadata {
    pub fn new(
        station_id: i32,
        number_of_seasons: i32,
        from_time: chrono::NaiveDate,
        to_time: chrono::NaiveDate,
        quality_class: i32,
        seed_parameter: i32,
        updated_at: chrono::NaiveDate,
    ) -> Self {
        Self {
            station_id,
            number_of_seasons,
            from_time,
            to_time,
            quality_class,
            seed_parameter,
            updated_at,
        }
    }
}

#[derive(Debug, Deserialize)]
struct IdfRecord {
    #[serde(flatten)]
    metadata: IdfMetadata,
    #[serde(flatten)]
    value: IdfValue,
}

pub type IdfTuple = (IdfMetadata, Vec<IdfValue>);

pub fn parse_idf_csv_file(filename: &str) -> Result<HashMap<i32, IdfTuple>, Error> {
    let file = File::open(filename)?;
    let mut rdr = ReaderBuilder::new().delimiter(b';').from_reader(file);

    // Iterate over records and print them
    let mut map_station_values: HashMap<i32, IdfTuple> = HashMap::new();
    for result in rdr.deserialize() {
        let record: IdfRecord = result?;
        // insert the data
        map_station_values
            .entry(record.metadata.station_id)
            .or_insert((record.metadata, vec![]))
            .1
            .push(record.value);
    }
    Ok(map_station_values)
}

pub fn create_idf_csv_content(
    data: HashMap<i32, IdfTuple>,
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
