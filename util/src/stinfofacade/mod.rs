use std::sync::{LazyLock, PoisonError};

use thiserror::Error;
use tracing::warn;

use crate::getenv;

pub static STINFO_CONN_STRING: LazyLock<Option<String>> = LazyLock::new(|| {
    let stinfo_conn_string = getenv("STINFO_CONN_STRING").ok();
    if stinfo_conn_string.is_none() {
        warn!("Running with no stinfosys conn string");
    }
    stinfo_conn_string
});

#[derive(Error, Debug)]
pub enum Error {
    #[error("no conn string was provided")]
    NoConnString,
    #[error("operation was cancelled")]
    Cancelled,
    #[error("postgres returned an error: {0}")]
    Database(#[from] tokio_postgres::Error),
    #[error("database pool could not return a connection: {0}")]
    Pool(#[from] bb8::RunError<tokio_postgres::Error>),
    #[error("RwLock was poisoned")]
    Lock,
    #[error("issues with level conversion: {0}")]
    Level(String),
    #[error("Csv ser/de error: {0}")]
    Csv(#[from] csv::Error),
    #[error("Csv writer failed to yield inner writer")]
    CsvIntoInner,
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

// we need this instead of a `#[from]` because of PoisonError's generic type.
// the error doesn't contain anything useful, except the corrupted data you can
// attempt to salvage
impl<T> From<PoisonError<T>> for Error {
    fn from(_: PoisonError<T>) -> Self {
        Self::Lock
    }
}

impl From<csv::IntoInnerError<csv::Writer<Vec<u8>>>> for Error {
    fn from(_: csv::IntoInnerError<csv::Writer<Vec<u8>>>) -> Self {
        Self::CsvIntoInner
    }
}

pub mod from_to_time;
pub mod level;
pub mod message_priority;
pub mod param;
pub mod permissions;
pub mod persistence;
