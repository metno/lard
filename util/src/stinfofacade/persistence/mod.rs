/// Util for writing stinfosys caches to disk as CSVs, so we can start our
/// services with these persisted caches when stinfosys is down
use std::path::Path;

use csv::{Reader, Writer};
use serde::{de::DeserializeOwned, Serialize};

use crate::stinfofacade::Error;

pub mod permissions;

pub async fn write_to_csv(
    records: Vec<impl Serialize>,
    path: impl AsRef<Path>,
) -> Result<(), Error> {
    let mut writer = Writer::from_writer(Vec::new());
    for record in records {
        writer.serialize(record)?;
    }
    writer.flush()?;

    tokio::fs::write(path, writer.into_inner()?).await?;
    Ok(())
}

pub async fn read_from_csv<T: DeserializeOwned>(path: impl AsRef<Path>) -> Result<Vec<T>, Error> {
    let bytes = tokio::fs::read(path).await?;
    let mut reader = Reader::from_reader(bytes.as_slice());

    let records = reader
        .deserialize()
        .collect::<Result<Vec<T>, csv::Error>>()?;
    Ok(records)
}
