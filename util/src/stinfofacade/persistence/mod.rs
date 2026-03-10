//! Util for writing stinfosys caches to disk as CSVs, so we can start our
//! services with these persisted caches when stinfosys is down.
//!
//! Each module includes "Record" types that represent the csv record structure
//! we want to serialize to, and some helper functions to go between the
//! record representation and the formats we use in practice, and to write
//! the records to disk.
//!
//! The CSVs are written to `<working dir>/persistence`, which will be created
//! if it does not exist.

use std::path::Path;

use csv::{Reader, Writer};
use serde::{de::DeserializeOwned, Serialize};
use tracing::error;

use crate::stinfofacade::Error;

pub mod level;
pub mod message_priority;
pub mod param;
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

    if let Some(parent) = path.as_ref().parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .inspect_err(|e| error!("failed to create parent dir for csv: {}", e))?;
    }

    tokio::fs::write(path, writer.into_inner()?)
        .await
        .inspect_err(|e| error!("failed to write csv: {}", e))?;
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
