use axum::http::StatusCode;
use hmac::digest::InvalidLength;
use thiserror::Error;
use tokio::task::JoinError;

/// Utility function for mapping any error into a `500 Internal Server Error`
/// response.
pub fn internal_error<E: std::error::Error>(err: E) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}

pub fn unauthorized<E: std::error::Error>(err: E) -> (StatusCode, String) {
    (StatusCode::UNAUTHORIZED, err.to_string())
}

pub fn bad_request<E: std::error::Error>(err: E) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, err.to_string())
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("postgres returned an error: {0}")]
    Database(#[from] tokio_postgres::Error),
    #[error("database pool could not return a connection: {0}")]
    Pool(#[from] bb8::RunError<tokio_postgres::Error>),
    #[error("join error: {0}")]
    Join(#[from] JoinError),
    #[error("reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("jwt error: {0}")]
    JWT(#[from] jwt::Error),
    #[error("invalid length error: {0}")]
    Invalidlength(#[from] InvalidLength),
    #[error("parse int error: {0}")]
    Parse(#[from] std::num::ParseIntError),
    #[error("parse float error: {0}")]
    ParseFloat(#[from] std::num::ParseFloatError),
    #[error("csv parsing error: {0}")]
    Csv(#[from] csv::Error),
    #[error("env var error: {0}")]
    Env(#[from] std::env::VarError),
    #[error("S3 error: {0}")]
    S3(#[from] s3::error::S3Error),
    #[error("RwLock was poisoned: {0}")]
    Lock(String),
    #[error("filter error: {0}")]
    Filter(String),
    #[error("filter error: {0}")]
    User(StatusCode, String),
}
