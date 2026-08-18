use std::sync::LazyLock;

use axum::{
    Router,
    extract::{Query, Request},
    response::{Json, Redirect},
    routing::{get, post},
};
use chrono::{Duration, Utc};
use openidconnect::{
    AccessToken, Audience, AuthUrl, EmptyAdditionalProviderMetadata, EmptyExtraTokenFields,
    IssuerUrl, JsonWebKeySetUrl, PrivateSigningKey, ResponseTypes, Scope, StandardClaims,
    SubjectIdentifier, TokenUrl,
    core::{
        CoreJsonWebKeySet, CoreJwsSigningAlgorithm, CoreProviderMetadata, CoreResponseType,
        CoreRsaPrivateSigningKey, CoreSubjectIdentifierType, CoreTokenType,
    },
};
use rsa::{
    RsaPrivateKey,
    pkcs1::{EncodeRsaPrivateKey, LineEnding},
};
use serde::Deserialize;

use crate::auth::oidc::{
    MetClaims, MetIdToken, MetIdTokenClaims, MetIdTokenFields, MetTokenResponse,
};

// TODO: this is a hack! do it properly
use std::sync::OnceLock;
pub static NONCE: OnceLock<openidconnect::Nonce> = OnceLock::new();

static PRIVATE_KEY: LazyLock<CoreRsaPrivateSigningKey> = LazyLock::new(|| {
    let mut rng = rand::thread_rng();
    // Don't try this in prod kids lol but a small key is fast to generate
    let private_key = RsaPrivateKey::new(&mut rng, 512).expect("failed to gen private key");
    let pem = private_key
        // NOTE: will break on non-unix OS
        .to_pkcs1_pem(LineEnding::LF)
        .expect("failed to encode private key");
    CoreRsaPrivateSigningKey::from_pem(&pem, None)
        .expect("failed to convert rsa key to openidconnect crate type")
});

fn generate_provider_metadata() -> CoreProviderMetadata {
    CoreProviderMetadata::new(
        // TODO: check port for conflict
        IssuerUrl::new("http://localhost:3008".to_string()).unwrap(),
        AuthUrl::new("http://localhost:3008/authorize".to_string()).unwrap(),
        JsonWebKeySetUrl::new("http://localhost:3008/jwk".to_string()).unwrap(),
        vec![ResponseTypes::new(vec![CoreResponseType::Code])],
        vec![CoreSubjectIdentifierType::Pairwise],
        vec![CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256],
        EmptyAdditionalProviderMetadata {},
    )
    .set_token_endpoint(Some(
        TokenUrl::new("http://localhost:3008/token".to_string()).unwrap(),
    ))
    .set_scopes_supported(Some(vec![Scope::new("groups".to_string())]))
}

async fn handle_provider_metadata() -> Json<CoreProviderMetadata> {
    // it would be more efficient to generate once and reuse, but for mocking purposes we're only
    // calling this once or twice
    Json(generate_provider_metadata())
}

async fn handle_jwk() -> Json<CoreJsonWebKeySet> {
    Json(CoreJsonWebKeySet::new(vec![
        PRIVATE_KEY.as_verification_key(),
    ]))
}

#[derive(Deserialize)]
struct AuthorizeQuery {
    #[allow(dead_code)]
    response_type: String,
    #[allow(dead_code)]
    client_id: String,
    state: String,
    #[allow(dead_code)]
    code_challenge: String,
    #[allow(dead_code)]
    code_challenge_method: String,
    redirect_uri: String,
    #[allow(dead_code)]
    scope: String,
    nonce: String,
}

async fn handle_authorize(Query(query): Query<AuthorizeQuery>) -> Redirect {
    // TODO: would there be any value in trying to test the crypto stuff instead of just returning?
    let mut redirect = query.redirect_uri.clone();
    if redirect.contains('?') {
        redirect.push('&')
    } else {
        redirect.push('?')
    }
    redirect.push_str("code=123456&state=");
    redirect.push_str(&query.state);

    NONCE.set(openidconnect::Nonce::new(query.nonce)).unwrap();

    Redirect::to(&redirect)
}

async fn handle_token(_req: Request) -> Json<MetTokenResponse> {
    let nonce = NONCE.get().unwrap();
    let id_token_claims = MetIdTokenClaims::new(
        IssuerUrl::new("http://localhost:3008".to_string()).unwrap(),
        // TODO: check this is right
        vec![Audience::new("lard_integration_testing".to_string())],
        // expiration time
        Utc::now() + Duration::seconds(300),
        // issue time
        Utc::now(),
        // TODO: do we need to include anything else? email?
        StandardClaims::new(SubjectIdentifier::new("id".to_string())),
        MetClaims {
            // TODO: do we want to test without this?
            groups: vec!["/AD/lard-cms-base".to_string()],
        },
    )
    .set_nonce(Some(nonce.clone()));

    let access_token = AccessToken::new("does there need to be something here?".to_string());

    let id_token = MetIdToken::new(
        id_token_claims,
        &*PRIVATE_KEY,
        CoreJwsSigningAlgorithm::RsaSsaPkcs1V15Sha256,
        Some(&access_token),
        None,
    )
    .unwrap();

    let token_fields = MetIdTokenFields::new(Some(id_token), EmptyExtraTokenFields {});

    let token_resp = MetTokenResponse::new(access_token, CoreTokenType::Bearer, token_fields);

    Json(token_resp)
}

// useful for debugging tests
//async fn print_reqs(
//    req: axum::extract::Request,
//    next: axum::middleware::Next,
//) -> axum::response::Response {
//    eprintln!("req: {req:?}");
//
//    let resp = next.run(req).await;
//
//    eprintln!("resp: {resp:?}");
//
//    resp
//}

pub async fn run() -> Result<(), std::io::Error> {
    let app = Router::new()
        .route(
            "/.well-known/openid-configuration",
            get(handle_provider_metadata),
        )
        .route("/jwk", get(handle_jwk))
        .route("/authorize", get(handle_authorize))
        .route("/token", post(handle_token));
    //.layer(axum::middleware::from_fn(print_reqs));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3008").await?;
    axum::serve(listener, app).await?;

    eprintln!("exiting oidc provider server");

    Ok(())
}
