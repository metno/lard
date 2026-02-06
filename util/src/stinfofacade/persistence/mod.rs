/// Util for writing stinfosys caches to disk as CSVs, so we can start our
/// services with these persisted caches when stinfosys is down
use std::path::Path;

use csv::Writer;
use serde::Serialize;
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
