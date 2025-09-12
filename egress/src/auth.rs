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

use crate::error::Error;

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
#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    resource_access: Resource,
    exp: usize, // need when creating a token for testing
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Resource {
    #[serde(rename = "ODA")] // currently the name of the resource
    resource: Roles,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Roles {
    roles: Vec<String>,
}

// probably best to cache the cert to speed things up
// and not rely on a consistent login.met.no connection
pub async fn cache_jwks_certs() -> Result<JWKScerts, Error> {
    let jwks_url = std::env::var("JWKS_URL")?;
    let certs = reqwest::get(jwks_url).await?.text().await?;
    let parsed_json: Keys = serde_json::from_str(&certs)?;
    if !parsed_json.keys.is_empty() {
        for key in parsed_json.keys {
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
    let re = Regex::new(r".*?permitid-(\d+)").unwrap();

    roles
        .iter()
        .filter_map(|role| re.captures(role))
        .filter_map(|capture| capture.get(0))
        .filter_map(|end_num| end_num.as_str().parse::<i32>().ok())
        .collect()
}

// verify a token with the certs
pub async fn verify_token(token_str: &str, certs: JWKScerts) -> Result<Vec<i32>, Error> {
    let mut validation = Validation::new(Algorithm::ES384);
    validation.set_audience(&["ODA"]);
    let token_message = decode::<Claims>(token_str, &certs, &validation)?;

    Ok(parse_permitid(
        token_message.claims.resource_access.resource.roles,
    ))
}

async fn parse_auth_header(header: &str) -> Option<String> {
    // Assuming "Bearer <token>" format
    header
        .starts_with("Bearer ")
        .then(|| header.strip_prefix("Bearer ").unwrap().to_string())
}

pub async fn auth_middleware(
    State(certs): State<JWKScerts>,
    mut req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let auth_header = match req
        .headers()
        .get(http::header::AUTHORIZATION)
        .and_then(|header| header.to_str().ok())
    {
        Some(auth_header) => auth_header,
        None => {
            req.extensions_mut().insert(<Option<Vec<i32>>>::None);
            // for now we still want things to work when people don't send an auth header
            return Ok(next.run(req).await);
        }
    };

    let token = parse_auth_header(auth_header)
        .await
        // didn't have the expected bearer token format
        // 400 includes "malformed request syntax, invalid request message framing"
        .ok_or(StatusCode::BAD_REQUEST)?;

    let roles = verify_token(&token, certs)
        .await
        // token could not be verified
        .map_err(|_| StatusCode::UNAUTHORIZED)?;

    req.extensions_mut().insert(Some(roles));
    Ok(next.run(req).await)
}
