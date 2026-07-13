//! auth middleware for decoding oauth2 jwks tokens
use std::sync::LazyLock;

use axum::{
    RequestPartsExt,
    extract::{Extension, FromRequestParts, Request, State},
    http::{StatusCode, request::Parts},
    middleware::Next,
    response::Response,
};
use jsonwebtoken::{Algorithm, DecodingKey, TokenData, Validation, decode};
use regex::Regex;
use reqwest;
use serde::{Deserialize, Serialize};

use crate::{auth::Error, http_error::internal};

pub type JwksCerts = DecodingKey;

/// Permits (restriction levels defined in stinfosys, see
/// [crate::stinfofacade::permissions]) a user had access to
#[derive(Clone, Debug, PartialEq)]
pub struct PermitRoles(pub Vec<i32>);

/// Stations a user has access to. The user will be able to access all data
/// from a station they have the role for, regardless of its permitid.
#[derive(Clone, Debug, PartialEq)]
pub struct StationRoles(pub Vec<i32>);

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
pub async fn cache_jwks_certs(url: String) -> Result<JwksCerts, Error> {
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

fn parse_auth_header(header: &str) -> Option<&str> {
    // Assuming "Bearer <token>" format
    header.strip_prefix("Bearer ")
}

// verify a token with the certs
pub fn verify_token(token_str: &str, certs: JwksCerts) -> Result<TokenData<Claims>, Error> {
    let mut validation = Validation::new(Algorithm::ES384);
    validation.set_audience(&["ODA"]);
    // this also checks that the token isn't expired. `Validation` has a field
    // `validate_exp` which defaults to true.
    Ok(decode::<Claims>(token_str, &certs, &validation)?)
}

pub async fn auth_middleware(
    State(certs): State<JwksCerts>,
    mut req: Request,
    next: Next,
) -> Result<Response, (StatusCode, String)> {
    // the `.ok()`s mean that if there is an error it will be consumed.
    // Then we get a None, which means no special authorisation
    let roles = req
        .headers()
        .get(http::header::AUTHORIZATION)
        .and_then(|header| header.to_str().ok())
        .and_then(parse_auth_header)
        .and_then(|token| verify_token(token, certs).ok())
        .map(|token_message| token_message.claims.resource_access.resource.roles);
    req.extensions_mut().insert(roles);

    Ok(next.run(req).await)
}

fn parse_permit_roles(roles: Roles) -> PermitRoles {
    PermitRoles(
        roles
            .iter()
            .filter_map(|role| RE_PERMITID.captures(role))
            .filter_map(|capture| capture.get(1))
            .filter_map(|end_num| end_num.as_str().parse::<i32>().ok())
            .collect(),
    )
}

impl<S> FromRequestParts<S> for PermitRoles
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, String);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let Extension(auth_roles) = parts
            .extract::<Extension<Option<Roles>>>()
            .await
            .map_err(internal)?;

        Ok(parse_permit_roles(auth_roles.unwrap_or_default()))
    }
}

fn parse_station_roles(roles: Roles) -> StationRoles {
    // Note: this is a temporary solution to parse stationids from the token roles,
    // it does not scale well since there is a limit to token size and is not managed in the metadata db.
    // We should ideally have a better auth structure in the future
    // see: https://github.com/metno/lard/issues/222
    StationRoles(
        roles
            .iter()
            .filter_map(|role| RE_STATIONID.captures(role))
            .filter_map(|capture| capture.get(1))
            .filter_map(|end_num| end_num.as_str().parse::<i32>().ok())
            .collect(),
    )
}

impl<S> FromRequestParts<S> for StationRoles
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, String);

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        let Extension(auth_roles) = parts
            .extract::<Extension<Option<Roles>>>()
            .await
            .map_err(internal)?;

        Ok(parse_station_roles(auth_roles.unwrap_or_default()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_permitid() {
        let cases = [
            (
                vec!["read-permitid-9".to_string(), "read-permitid-5".to_string()],
                PermitRoles(vec![9, 5]), // should find the integers
            ),
            (
                vec![
                    "something-9".to_string(),
                    "something-5".to_string(),
                    "read-permitid-5-a".to_string(),
                    "no-read-permitid-5".to_string(),
                ],
                PermitRoles(vec![]), // should not find the integers
            ),
        ];

        for (roles, expected_output) in cases {
            let output = parse_permit_roles(roles);
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
                StationRoles(vec![12345, 54321]), // should find the integers
            ),
            (
                vec![
                    "something-99999".to_string(),
                    "something-55555".to_string(),
                    "cannot-read-stationid-54321".to_string(),
                    "read-stationid-54321abc".to_string(),
                ],
                StationRoles(vec![]), // should not find the integers
            ),
        ];

        for (roles, expected_output) in cases {
            let output = parse_station_roles(roles);
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
