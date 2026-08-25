//! auth middleware for decoding oauth2 jwks tokens
use std::sync::{LazyLock, OnceLock};

use jsonwebtoken::{Algorithm, DecodingKey, TokenData, Validation, decode};
use regex::Regex;
use reqwest;
use serde::{Deserialize, Serialize};

use crate::auth::{Auth, Error};

pub static JWKS_CERTS: OnceLock<DecodingKey> = OnceLock::new();

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

/// Raw auth roles from keycloak tokens
pub type Roles = Vec<String>;

// Claims structs...
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Resource {
    pub roles: Roles,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Access {
    #[serde(rename = "ODA")] // currently the name of the resource
    pub resource: Resource,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Claims {
    pub resource_access: Access,
    pub exp: usize, // need when creating a token for testing
}

// find the numbers after the string permitid
static RE_PERMITID: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^read-permitid-(\d+)$").unwrap());

// find the numbers after the string stationid
static RE_STATIONID: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^read-stationid-(\d+)$").unwrap());

// probably best to cache the cert to speed things up
// and not rely on a consistent login.met.no connection
pub async fn cache_jwks_certs(url: String) -> Result<DecodingKey, Error> {
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

// verify a token with the certs
pub fn verify_token(token_str: &str) -> Result<TokenData<Claims>, Error> {
    let mut validation = Validation::new(Algorithm::ES384);
    validation.set_audience(&["ODA"]);
    // this also checks that the token isn't expired. `Validation` has a field
    // `validate_exp` which defaults to true.
    Ok(decode::<Claims>(
        token_str,
        JWKS_CERTS
            .get()
            .expect("must init jwks certs OnceLock before trying to verify a bearer token"),
        &validation,
    )?)
}

fn parse_permit_roles(roles: &Roles) -> Vec<i32> {
    roles
        .iter()
        .filter_map(|role| RE_PERMITID.captures(role))
        .filter_map(|capture| capture.get(1))
        .filter_map(|end_num| end_num.as_str().parse::<i32>().ok())
        .collect()
}

fn parse_station_roles(roles: &Roles) -> Vec<i32> {
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

pub type AuthHeader<'a> = &'a axum::http::header::HeaderValue;

// TODO: Should this return Result to tell us what went wrong?
pub fn parse_auth_header(header: AuthHeader) -> Option<Auth> {
    header
        .to_str()
        .ok()
        // Assuming "Bearer <token>" format
        .and_then(|s| s.strip_prefix("Bearer "))
        .and_then(|token| verify_token(token).ok())
        .map(|token_message| token_message.claims.resource_access.resource.roles)
        .map(|roles| {
            let permit_roles = parse_permit_roles(&roles);
            let station_roles = parse_station_roles(&roles);
            Auth {
                user: None,
                cms_base: false,
                permit_roles,
                station_roles,
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;

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
            let output = parse_permit_roles(&roles);
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
            let output = parse_station_roles(&roles);
            assert_eq!(output, expected_output);
        }
    }
}
