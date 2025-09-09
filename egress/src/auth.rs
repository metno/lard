// auth middleware for decoding oauth2 jwks tokens
use axum::{extract::Request, http::StatusCode, middleware::Next, response::Response};
use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use regex::Regex;
use reqwest;
use serde::{Deserialize, Serialize};

pub type JWKScerts = DecodingKey;

use crate::error::Error;

// structs for getting keycloak certs
#[derive(Deserialize, Debug)]
struct Keycloak {
    _alg: String,
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
    resource_access: Oda,
    exp: usize, // need when creating a token for testing
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Oda {
    #[serde(rename = "ODA")]
    oda: Roles,
}
#[derive(Debug, Serialize, Deserialize)]
pub struct Roles {
    roles: Vec<String>,
}

// probably best to cache the cert (in main) to speed things up
// and not rely on a consistent login.met.no connection
pub async fn cache_jwks_certs() -> Result<JWKScerts, Error> {
    let jwks_url = std::env::var("JWKS_URL")?;
    let certs = reqwest::get(jwks_url).await?.text().await?;
    let parsed_json: Keys = serde_json::from_str(&certs)?;
    if !parsed_json.keys.is_empty() {
        // The first one is the one that is used ES384
        if let Some(x) = &parsed_json.keys[0].x {
            if let Some(y) = &parsed_json.keys[0].y {
                let key = jsonwebtoken::DecodingKey::from_ec_components(x, y)?;
                return Ok(key);
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
        Ok(parse_permitid(tm.claims.resource_access.oda.roles))
    } else {
        println!("could not verify token: {token_message:?}");
        Err(Error::Auth("problem parsing the token".to_string()))
    }
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
