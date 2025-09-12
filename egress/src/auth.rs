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
    let mut permit_list: Vec<i32> = Vec::new();
    // find the numbers after the string permitid
    let re = Regex::new(r".*?permitid-(\d+)").unwrap();
    for x in roles {
        if let Some(captures) = re.captures(&x) {
            if let Some(end_num) = captures.get(1) {
                let permit = end_num.as_str().parse::<i32>();
                if let Ok(p) = permit {
                    permit_list.push(p);
                }
            }
        }
    }
    permit_list
}

// verify a token with the certs
pub async fn verify_token(token_str: &str, certs: JWKScerts) -> Result<Vec<i32>, Error> {
    let mut validation = Validation::new(Algorithm::ES384);
    validation.set_audience(&["ODA"]);
    let token_message = decode::<Claims>(token_str, &certs, &validation);
    if let Ok(tm) = token_message {
        Ok(parse_permitid(tm.claims.resource_access.resource.roles))
    } else {
        let token_message_err = token_message.unwrap_err();
        Err(Error::Auth(token_message_err.to_string()))
    }
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

    if let Some(token) = parse_auth_header(auth_header).await {
        let roles = verify_token(&token, certs).await;
        // insert the roles into a request extension so the handler can extract it
        if let Ok(r) = roles {
            req.extensions_mut().insert(Some(r));
            Ok(next.run(req).await)
        } else {
            // token could not be verified
            Err(StatusCode::UNAUTHORIZED)
        }
    } else {
        // didn't have the expected bearer token format
        // 400 includes "malformed request syntax, invalid request message framing"
        Err(StatusCode::BAD_REQUEST)
    }
}
