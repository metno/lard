use std::sync::PoisonError;

use axum::http::StatusCode;
use thiserror::Error;
use tokio::task::JoinError;

use ::util::{EnvError, stinfofacade};

/// Utility function for mapping any error into a `500 Internal Server Error` response.
pub fn internal_error<E: std::error::Error>(err: E) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
}

/// Utility function for mapping any error into a `401 Unauthorized` response.
pub fn unauthorized<E: std::error::Error>(err: E) -> (StatusCode, String) {
    (StatusCode::UNAUTHORIZED, err.to_string())
}

/// Utility function for mapping any error into a `404 Not Found Error` response.
pub fn not_found_error<E: std::error::Error>(err: E) -> (StatusCode, String) {
    (StatusCode::NOT_FOUND, err.to_string())
}

/// Utility function for mapping any error into a `400 Bad Request Error` response.
pub fn bad_request<E: std::error::Error>(err: E) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, err.to_string())
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("no conn string was provided")]
    NoConnString,
    #[error("postgres returned an error: {0}")]
    Database(#[from] tokio_postgres::Error),
    #[error("database pool could not return a connection: {0}")]
    Pool(#[from] bb8::RunError<tokio_postgres::Error>),
    #[error("join error: {0}")]
    Join(#[from] JoinError),
    #[error("reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("jwt error: {0}")]
    Jwt(#[from] jsonwebtoken::errors::Error),
    #[error("auth error: {0}")]
    Auth(String),
    #[error("parse int error: {0}")]
    Parse(#[from] std::num::ParseIntError),
    #[error("parse float error: {0}")]
    ParseFloat(#[from] std::num::ParseFloatError),
    #[error("csv parsing error: {0}")]
    Csv(#[from] csv::Error),
    #[error(transparent)]
    Env(#[from] EnvError),
    #[error("S3 error: {0}")]
    S3(#[from] s3::error::S3Error),
    #[error("RwLock was poisoned")]
    Lock,
    #[error("Utf8 error: {0}")]
    Utf8(#[from] std::str::Utf8Error),
    #[error("metadata cache error: {0}")]
    Stinfo(#[from] stinfofacade::Error),
}

impl<T> From<PoisonError<T>> for Error {
    fn from(_: PoisonError<T>) -> Self {
        Self::Lock
    }
}
