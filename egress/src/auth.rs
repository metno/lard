// auth middleware for decoding oauth2 jwks tokens
use hmac::{Hmac, Mac};
use jwt::VerifyWithKey;
use reqwest;
use sha2::Sha256;
use std::collections::BTreeMap;

use axum::{extract::Request, http::StatusCode, middleware::Next, response::Response};

pub type JWKScerts = Hmac<Sha256>;

use crate::error::Error;

// probably best to cache the cert (in main) to speed things up
// and not rely on a consistent login.met.no connection
pub async fn cache_jwks_certs() -> Result<JWKScerts, Error> {
    let jwks_url = std::env::var("JWKS_URL")?;
    let certs = reqwest::get(jwks_url).await?.text().await?;
    let byte_cert: &[u8] = certs.as_bytes();
    let key: Hmac<Sha256> = Hmac::new_from_slice(byte_cert)?;
    Ok(key)
}

// verify a token with the certs
pub async fn verify_token(
    token_str: &str,
    certs: JWKScerts,
) -> Result<BTreeMap<String, String>, Error> {
    let claims: BTreeMap<String, String> = token_str.verify_with_key(&certs)?;
    Ok(claims)
}

async fn parse_auth_header(header: &str) -> Option<String> {
    // Assuming "Bearer <token>" format
    if header.starts_with("Bearer ") {
        let token = header.strip_prefix("Bearer ").unwrap().to_string();
        return Some(token);
    }
    None
}

pub async fn auth_middleware(mut req: Request, next: Next) -> Result<Response, StatusCode> {
    let auth_header = req
        .headers()
        .get(http::header::AUTHORIZATION)
        .and_then(|header| header.to_str().ok());

    let auth_header = if let Some(auth_header) = auth_header {
        auth_header
    } else {
        // for now we still want things to work when people don't send an auth header
        return Ok(next.run(req).await);
    };

    if let Some(token) = parse_auth_header(auth_header).await {
        //println!("token in middleware: {token:?}");
        // insert the token into a request extension so the handler can extract it
        req.extensions_mut().insert(token);
        Ok(next.run(req).await)
    } else {
        // didn't have the expected bearer token format
        Err(StatusCode::UNAUTHORIZED)
    }
}
