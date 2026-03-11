//! auth middleware for decoding oauth2 jwks tokens
use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::Response,
};
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use regex::Regex;
use reqwest;
use serde::{Deserialize, Serialize};

pub type JWKScerts = DecodingKey;

use crate::error::{self, Error};
use ::util::getenv;

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
pub async fn cache_jwks_certs() -> Result<JWKScerts, Error> {
    let jwks_url = getenv("JWKS_URL")?;
    let certs: Keys = reqwest::get(jwks_url).await?.json().await?;
    if !certs.keys.is_empty() {
        for key in certs.keys {
            // Use default of ES384
            if key.alg == "ES384" {
                if let Some(x) = key.x {
                    if let Some(y) = key.y {
                        let decoding_key = jsonwebtoken::DecodingKey::from_ec_components(&x, &y)?;
                        return Ok(decoding_key);
                    }
                }
            }
        }
    }
    Err(Error::Auth("unable to get certs from keycloak".to_string()))
}

fn parse_permitid(roles: Vec<String>) -> Vec<i32> {
    // find the numbers after the string permitid
    let re = Regex::new(r"read-permitid-(\d+)").unwrap();

    roles
        .iter()
        .filter_map(|role| re.captures(role))
        .filter_map(|capture| capture.get(1))
        .filter_map(|end_num| end_num.as_str().parse::<i32>().ok())
        .collect()
}

fn parse_stations(roles: Vec<String>) -> Vec<i32> {
    // Note: this is a temporary solution to parse stationids from the token roles,
    // it does not scale well since there is a limit to token size and is not managed in the metadata db.
    // We should ideally have a better auth structure in the future
    // see: https://github.com/metno/lard/issues/222

    // find the numbers after the string stationid
    let re = Regex::new(r"read-stationid-(\d+)").unwrap();

    roles
        .iter()
        .filter_map(|role| re.captures(role))
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
        parse_permitid(token_message.claims.resource_access.resource.roles.clone()),
        parse_stations(token_message.claims.resource_access.resource.roles),
    ))
}

fn parse_auth_header(header: &str) -> Option<String> {
    // Assuming "Bearer <token>" format
    header
        .starts_with("Bearer ")
        .then(|| header.strip_prefix("Bearer ").unwrap().to_string())
}

pub async fn auth_middleware(
    State(certs): State<JWKScerts>,
    mut req: Request,
    next: Next,
) -> Result<Response, (StatusCode, String)> {
    match req
        .headers()
        .get(http::header::AUTHORIZATION)
        .and_then(|header| header.to_str().ok())
        .and_then(parse_auth_header)
    {
        Some(token) => {
            // if errors, then default to open access (aka empty roles)
            let roles = verify_token(&token, certs)
                .map_err(error::unauthorized)
                .unwrap_or_default();
            req.extensions_mut().insert(Some(roles));
        }
        None => {
            // no scopes, this user has only open data access
            req.extensions_mut()
                .insert(<Option<(Vec<i32>, Vec<i32>)>>::None);
        }
    }

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
                vec!["something-9".to_string(), "something-5".to_string()],
                vec![], // should not find the integers
            ),
        ];

        for (roles, expected_output) in cases {
            let output = parse_permitid(roles);
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
                vec!["something-99999".to_string(), "something-55555".to_string()],
                vec![], // should not find the integers
            ),
        ];

        for (roles, expected_output) in cases {
            let output = parse_stations(roles);
            assert_eq!(output, expected_output);
        }
    }

    #[test]
    fn test_parse_auth_header() {
        let cases = [
            (
                // valid bearer token
                "Bearer abcdefghijklmnopqrstuvwxyz",
                Some("abcdefghijklmnopqrstuvwxyz".to_string()),
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
