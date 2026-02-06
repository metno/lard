/// Util for writing stinfosys caches to disk as CSVs, so we can start our
/// services with these persisted caches when stinfosys is down
use std::path::Path;

use csv::{Reader, Writer};
use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;

pub mod permissions;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Csv(#[from] csv::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub fn write_to_csv(records: Vec<impl Serialize>, path: impl AsRef<Path>) -> Result<(), Error> {
    let mut writer = Writer::from_path(path)?;
    for record in records {
        writer.serialize(record)?;
    }
    writer.flush()?;
    Ok(())
}

//pub fn read_from_csv<'a, T: Deserialize<'a>>(path: impl AsRef<Path>) -> Result<Vec<T>, Error> {
pub fn read_from_csv<T: DeserializeOwned>(path: impl AsRef<Path>) -> Result<Vec<T>, Error> {
    let mut reader = Reader::from_path(path)?;
    let records = reader
        .deserialize()
        .collect::<Result<Vec<T>, csv::Error>>()?;
    Ok(records)
}
