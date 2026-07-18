use axum::{
    extract::{FromRequestParts, OptionalFromRequestParts, Request},
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Redirect, Response},
};
use http::request::Parts;
use openidconnect::{CsrfToken, Nonce, PkceCodeChallenge, Scope, core::CoreAuthenticationFlow};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tower_sessions::Session;

/// Auth using explicitly user-passed tokens, mainly for API use
pub mod bearer;

/// Auth using full OIDC flow, mainly for GUI use
pub mod oidc;
use oidc::OidcState;

#[derive(Error, Debug)]
pub enum Error {
    #[error("reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("jwt error: {0}")]
    Jwt(#[from] jsonwebtoken::errors::Error),
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

pub async fn enforce_cms(
    session: Session,
    // TODO: should probably just take Session and derive this from it
    //auth: Option<Auth>,
    req: Request,
    next: Next,
    // TODO: should be type Redirect?
) -> Result<Response, (StatusCode, String)> {
    // TODO: is this unwrap OK? it's from the tower sessions examples
    let auth: Option<Auth> = session.get(Auth::SESSION_KEY).await.unwrap();
    if let Some(auth) = auth {
        if auth.cms_base {
            Ok(next.run(req).await)
        } else {
            todo!() // Error page?
        }
    } else {
        // TODO: maybe this whole branch should live on a dedicated endpoint?

        // TODO: get redirect query?
        let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();
        // TODO: docs say make sure set_auth_uri has been called on client?
        let (auth_url, csrf_token, nonce) = oidc::CLIENT
            .get()
            .expect("must initialize CLIENT before tryigng to do auth")
            .authorize_url(
                CoreAuthenticationFlow::AuthorizationCode,
                CsrfToken::new_random,
                Nonce::new_random,
            )
            // this scope is required to get the groups claim
            // TODO: any scopes we need?
            .add_scope(Scope::new("groups".to_string()))
            .set_pkce_challenge(pkce_challenge)
            .url();

        // TODO: remove unwrap
        session
            .insert(
                OidcState::SESSION_KEY,
                OidcState {
                    csrf_token,
                    nonce,
                    pkce_verifier,
                },
            )
            .await
            .unwrap();

        // send user to the oidc issuer to get an auth code
        Ok(Redirect::to(auth_url.as_str()).into_response())
    }
}
