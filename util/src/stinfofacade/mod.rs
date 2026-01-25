use std::sync::PoisonError;

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("postgres returned an error: {0}")]
    Database(#[from] tokio_postgres::Error),
    #[error("RwLock was poisoned")]
    Lock,
    #[error("issues with level conversion: {0}")]
    Level(String),
}

// we need this instead of a `#[from]` because of PoisonError's generic type.
// the error doesn't contain anything useful, except the corrupted data you can
// attempt to salvage
impl<T> From<PoisonError<T>> for Error {
    fn from(_: PoisonError<T>) -> Self {
        Self::Lock
    }
}

pub mod level;
pub mod permissions;
