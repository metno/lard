//! auth middleware for decoding oauth2 jwks tokens
use std::sync::LazyLock;

use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::Response,
};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use regex::Regex;
use reqwest;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("jwt error: {0}")]
    Jwt(#[from] jsonwebtoken::errors::Error),
    #[error("auth error: {0}")]
    Auth(String),
}

pub type JWKScerts = DecodingKey;

// structs for getting keycloak certs
#[derive(Deserialize, Debug)]
struct Keycloak {
    alg: String,
    x: Option<String>,
    y: Option<String>,
}
#[derive(Deserialize, Debug)]
struct Keys {
    keys: Vec<Keycloak>,
}
// Claims structs...
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Claims {
    pub resource_access: Resource,
    pub exp: usize, // need when creating a token for testing
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Resource {
    #[serde(rename = "ODA")] // currently the name of the resource
    pub resource: Roles,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Roles {
    pub roles: Vec<String>,
}

// probably best to cache the cert to speed things up
// and not rely on a consistent login.met.no connection
pub async fn cache_jwks_certs(url: String) -> Result<JWKScerts, Error> {
    let certs: Keys = reqwest::get(url).await?.json().await?;
    if !certs.keys.is_empty() {
        for key in certs.keys {
            // Use default of ES384
            if key.alg == "ES384"
                && let Some(x) = key.x
                && let Some(y) = key.y
            {
                let decoding_key = jsonwebtoken::DecodingKey::from_ec_components(&x, &y)?;
                return Ok(decoding_key);
            }
        }
    }
    Err(Error::Auth("unable to get certs from keycloak".to_string()))
}

// find the numbers after the string permitid
static RE_PERMITID: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^read-permitid-(\d+)$").unwrap());

fn parse_permitid(roles: &[String]) -> Vec<i32> {
    roles
        .iter()
        .filter_map(|role| RE_PERMITID.captures(role))
        .filter_map(|capture| capture.get(1))
        .filter_map(|end_num| end_num.as_str().parse::<i32>().ok())
        .collect()
}

// find the numbers after the string stationid
static RE_STATIONID: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^read-stationid-(\d+)$").unwrap());

fn parse_stations(roles: &[String]) -> Vec<i32> {
    // Note: this is a temporary solution to parse stationids from the token roles,
    // it does not scale well since there is a limit to token size and is not managed in the metadata db.
    // We should ideally have a better auth structure in the future
    // see: https://github.com/metno/lard/issues/222
    roles
        .iter()
        .filter_map(|role| RE_STATIONID.captures(role))
        .filter_map(|capture| capture.get(1))
        .filter_map(|end_num| end_num.as_str().parse::<i32>().ok())
        .collect()
}

// verify a token with the certs
pub fn verify_token(token_str: &str, certs: JWKScerts) -> Result<(Vec<i32>, Vec<i32>), Error> {
    let mut validation = Validation::new(Algorithm::ES384);
    validation.set_audience(&["ODA"]);
    let token_message = decode::<Claims>(token_str, &certs, &validation)?;

    Ok((
        parse_permitid(&token_message.claims.resource_access.resource.roles),
        parse_stations(&token_message.claims.resource_access.resource.roles),
    ))
}

fn parse_auth_header(header: &str) -> Option<&str> {
    // Assuming "Bearer <token>" format
    header.strip_prefix("Bearer ")
}

pub async fn auth_middleware(
    State(certs): State<JWKScerts>,
    mut req: Request,
    next: Next,
) -> Result<Response, (StatusCode, String)> {
    let roles = req
        .headers()
        .get(http::header::AUTHORIZATION)
        .and_then(|header| header.to_str().ok())
        .and_then(parse_auth_header)
        .and_then(|token| verify_token(token, certs).ok());
    // the .ok() on verify token means that if there is an error it will be consumed
    // then we get a None, which means open access
    req.extensions_mut().insert(roles);

    Ok(next.run(req).await)
}

#[cfg(test)]
mod tests {
    use crate::auth::{parse_auth_header, parse_permitid, parse_stations};

    #[test]
    fn test_parse_permitid() {
        let cases = [
            (
                vec!["read-permitid-9".to_string(), "read-permitid-5".to_string()],
                vec![9, 5], // should find the integers
            ),
            (
                vec![
                    "something-9".to_string(),
                    "something-5".to_string(),
                    "read-permitid-5-a".to_string(),
                    "no-read-permitid-5".to_string(),
                ],
                vec![], // should not find the integers
            ),
        ];

        for (roles, expected_output) in cases {
            let output = parse_permitid(&roles);
            assert_eq!(output, expected_output);
        }
    }

    #[test]
    fn test_parse_stations() {
        let cases = [
            (
                vec![
                    "read-stationid-12345".to_string(),
                    "read-stationid-54321".to_string(),
                ],
                vec![12345, 54321], // should find the integers
            ),
            (
                vec![
                    "something-99999".to_string(),
                    "something-55555".to_string(),
                    "cannot-read-stationid-54321".to_string(),
                    "read-stationid-54321abc".to_string(),
                ],
                vec![], // should not find the integers
            ),
        ];

        for (roles, expected_output) in cases {
            let output = parse_stations(&roles);
            assert_eq!(output, expected_output);
        }
    }

    #[test]
    fn test_parse_auth_header() {
        let cases = [
            (
                // valid bearer token
                "Bearer abcdefghijklmnopqrstuvwxyz",
                Some("abcdefghijklmnopqrstuvwxyz"),
            ),
            (
                // check its ok with basic (no bearer)
                "Basic abcdefghijklmnopqrstuvwxyz",
                None,
            ),
        ];

        for (token, expected_output) in cases {
            let output = parse_auth_header(token);
            assert_eq!(output, expected_output);
        }
    }
}
