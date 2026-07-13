use thiserror::Error;

/// Auth using explicitly user-passed tokens, mainly for API use
pub mod bearer;
/// Auth using full OIDC flow, mainly for GUI use
pub mod oidc;

#[derive(Error, Debug)]
pub enum Error {
    #[error("reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("jwt error: {0}")]
    Jwt(#[from] jsonwebtoken::errors::Error),
    #[error("auth error: {0}")]
    Auth(String),
}
