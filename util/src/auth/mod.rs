use axum::{
    extract::{FromRequestParts, OptionalFromRequestParts, OriginalUri, Request},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};
use http::request::Parts;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tower_sessions::{MemoryStore, Session, SessionManagerLayer};

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

// NOTE: if adding fields, make sure their Default impl does what you expect
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Auth {
    // also possible to extract email address if we need
    pub user: Option<String>,
    /// Whether the user has access to the CMS. The "base" in the name was
    /// written with the idea we might want to have more granular CMS
    /// permissions in the future
    pub cms_base: bool,
    /// Permits (restriction levels defined in stinfosys, see
    /// [crate::stinfofacade::permissions]) a user had access to
    pub permit_roles: Vec<i32>,
    /// Stations a user has access to. The user will be able to access all data
    /// from a station they have the role for, regardless of its permitid.
    pub station_roles: Vec<i32>,
}
impl Auth {
    pub const SESSION_KEY: &'static str = "auth";

    fn merge(mut self, mut other: Self) -> Self {
        // I'm assuming bearer won't contain a user so I don't have to combine them somehow
        self.user = self.user.or(other.user);
        self.cms_base = self.cms_base || other.cms_base;
        self.permit_roles.append(&mut other.permit_roles);
        self.station_roles.append(&mut other.station_roles);
        self
    }
}

// Note: In cases where we enforce some restriction using middleware AND use the extractor again
// in the handler, this code will run twice. It's possible to avoid that by using an outer
// middleware that puts this into an extension, and then getting the auth object from the extension
// instead of this extractor in the enforcement middleware and the handler. However, I decided it's
// not worth it because that would cause this code to run even when neither middleware nor the
// handler needs it, and it would complicate the API a bit.
impl<T: Send + Sync> OptionalFromRequestParts<T> for Auth {
    // TODO: figure out what error type to use here
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(
        req: &mut Parts,
        state: &T,
    ) -> Result<Option<Self>, Self::Rejection> {
        let session = Session::from_request_parts(req, state).await?;
        // TODO: is this unwrap OK? it's from the tower sessions examples
        let oidc_auth: Option<Auth> = session.get(Self::SESSION_KEY).await.unwrap();

        let auth_header = req.headers.get(http::header::AUTHORIZATION);
        let bearer_auth: Option<Auth> = auth_header.and_then(bearer::parse_auth_header);

        // TODO: can simplify with Option::reduce once it stabilises
        // combine permissions from both sources
        Ok(match (oidc_auth, bearer_auth) {
            (Some(oidc_auth), Some(bearer_auth)) => Some(oidc_auth.merge(bearer_auth)),
            (oidc_auth, bearer_auth) => oidc_auth.or(bearer_auth),
        })
    }
}
// If a use case doesn't specifically need to know whether there was any auth provided at all,
// this provides a more convenient extractor, returning a default Auth with no permissions
// instead of None
impl<T: Send + Sync> FromRequestParts<T> for Auth {
    // TODO: figure out what error type to use here
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(req: &mut Parts, state: &T) -> Result<Self, Self::Rejection> {
        Ok(Option::<Auth>::from_request_parts(req, state)
            .await?
            .unwrap_or_default())
    }
}

/// Middleware to ensure users have the "cms_base" permission to access any routes under it
///
/// If users are not already logged in (or passing a bearer token), we redirect them to log in
/// with oidc
pub async fn enforce_cms(
    session: Session,
    OriginalUri(next_url): OriginalUri,
    auth: Option<Auth>,
    req: Request,
    next: Next,
) -> Result<Response, (StatusCode, String)> {
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

pub fn init_session_layer() -> SessionManagerLayer<MemoryStore> {
    // TODO: we probably want to more robust backing store!
    // do we need it to be persistent across restarts?
    let session_store = MemoryStore::default();
    // TODO: we probably want expiry on this
    SessionManagerLayer::new(session_store)
}
