use axum::{
    extract::{FromRequestParts, OptionalFromRequestParts, OriginalUri, Request},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};
use http::request::Parts;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tower_sessions::Session;

use crate::http_error::internal;

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
    #[error("session cookie error: {0}")]
    Session(#[from] tower_sessions::session::Error),
    // TODO: this should probably be broken up
    #[error("auth error: {0}")]
    Auth(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Auth {
    // also possible to extract email address if we need
    user: String,
    cms_base: bool,
    permit_roles: Vec<i32>,
    station_roles: Vec<i32>,
}
impl Auth {
    pub const SESSION_KEY: &'static str = "auth";
}

impl<T> OptionalFromRequestParts<T> for Auth
where
    T: Send + Sync,
{
    // TODO: figure out what error type to use here
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(
        req: &mut Parts,
        state: &T,
    ) -> Result<Option<Self>, Self::Rejection> {
        let session = Session::from_request_parts(req, state).await?;
        // TODO: combine with bearer auth?
        // TODO: is this unwrap OK? it's from the tower sessions examples
        Ok(session.get(Self::SESSION_KEY).await.unwrap())
    }
}

/// Middleware to ensure users have the "cms_base" permission to access any routes under it
///
/// If users are not already logged in (or passing a bearer token), we redirect them to log in
/// with oidc
pub async fn enforce_cms(
    session: Session,
    OriginalUri(next_url): OriginalUri,
    req: Request,
    next: Next,
) -> Result<Response, (StatusCode, String)> {
    // TODO: this probably needs to use the OptionalFromRequestParts extractor instead of just
    // fetching from the cookie once that includes checking for a bearer token?
    let auth: Option<Auth> = session.get(Auth::SESSION_KEY).await.map_err(internal)?;
    if let Some(auth) = auth {
        if auth.cms_base {
            Ok(next.run(req).await)
        } else {
            todo!() // Error page?
        }
    } else {
        let redirect = oidc::init_auth_challenge(&session, next_url.to_string())
            .await
            .map_err(internal)?;

        // send user to the oidc provider to get an auth code.
        // the provider will then redirect them back to [`oidc::redirect_handler`] to
        // continue the flow
        Ok(redirect.into_response())
    }
}
