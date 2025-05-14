use axum::http::StatusCode;
use thiserror::Error;

/// Utility function for mapping any error into a `500 Internal Server Error`
/// response.
pub fn internal_error<E: std::error::Error>(err: E) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("postgres returned an error: {0}")]
    Database(#[from] tokio_postgres::Error),
    #[error("database pool could not return a connection: {0}")]
    Pool(#[from] bb8::RunError<tokio_postgres::Error>),
    #[error("parse int error: {0}")]
    Parse(#[from] std::num::ParseIntError),
    #[error("parse float error: {0}")]
    ParseFloat(#[from] std::num::ParseFloatError),
    #[error(transparent)]
    Csv(#[from] csv::Error),
    #[error(transparent)]
    Env(#[from] std::env::VarError),
    #[error(transparent)]
    S3(#[from] s3::error::S3Error),
}
