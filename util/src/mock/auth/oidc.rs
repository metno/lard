use axum::{Router, response::Json, routing::get};
use openidconnect::{
    AuthUrl, EmptyAdditionalProviderMetadata, IssuerUrl, JsonWebKeySetUrl, PrivateSigningKey,
    ResponseTypes, Scope, TokenUrl,
    core::{
        CoreJsonWebKeySet, CoreJwsSigningAlgorithm, CoreProviderMetadata, CoreResponseType,
        CoreRsaPrivateSigningKey, CoreSubjectIdentifierType,
    },
};
use rsa::{
    RsaPrivateKey,
    pkcs1::{EncodeRsaPrivateKey, LineEnding},
};

use std::sync::LazyLock;

static PRIVATE_KEY: LazyLock<CoreRsaPrivateSigningKey> = LazyLock::new(|| {
    let mut rng = rand::thread_rng();
    // Don't try this in prod kids lol but a small key is fast to generate
    let private_key = RsaPrivateKey::new(&mut rng, 32).expect("failed to gen private key");
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
        vec![CoreJwsSigningAlgorithm::RsaSsaPssSha256],
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

pub async fn run() -> Result<(), std::io::Error> {
    // TODO:
    // - auth endpoint
    // - token endpoint
    let app = Router::new()
        .route(
            "/.well-known/openid-configuration",
            get(handle_provider_metadata),
        )
        .route("/jwk", get(handle_jwk));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:3008").await?;
    axum::serve(listener, app).await?;

    eprintln!("exiting oidc provider server");

    Ok(())
}
