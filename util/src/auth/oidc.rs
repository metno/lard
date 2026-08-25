use std::sync::OnceLock;

use axum::{extract::Query, http::StatusCode, response::Redirect};
use openidconnect::{
    AccessTokenHash, AdditionalClaims, AuthorizationCode, ClientId, ClientSecret, CsrfToken,
    EmptyExtraTokenFields, EndpointMaybeSet, EndpointNotSet, EndpointSet, IdToken, IdTokenClaims,
    IdTokenFields, IssuerUrl, Nonce, OAuth2TokenResponse, PkceCodeChallenge, PkceCodeVerifier,
    RedirectUrl, Scope, StandardErrorResponse, StandardTokenResponse, TokenResponse,
    core::{
        CoreAuthDisplay, CoreAuthPrompt, CoreAuthenticationFlow, CoreErrorResponseType,
        CoreGenderClaim, CoreJsonWebKey, CoreJweContentEncryptionAlgorithm,
        CoreJwsSigningAlgorithm, CoreProviderMetadata, CoreRevocableToken,
        CoreRevocationErrorResponse, CoreTokenIntrospectionResponse, CoreTokenType,
    },
};
use serde::{Deserialize, Serialize};
use tower_sessions::Session;

use crate::{
    auth::{Auth, Error},
    http_error::internal,
};

// TODO: module doc
// TODO: note about initialising CLIENT

pub static CLIENT: OnceLock<Client> = OnceLock::new();

/// Used to store necessary state of the OIDC login flow in the session cookie as the user is
/// redirected around
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct OidcState {
    pub csrf_token: CsrfToken,
    pub nonce: Nonce,
    pub pkce_verifier: PkceCodeVerifier,
    // cannot be `axum::http::uri::Uri` because it does not implement Serialize
    pub next_url: String,
}
impl OidcState {
    pub(crate) const SESSION_KEY: &'static str = "oidc_state";
}

/// The "groups" claim that we use to define what users are authorized to access is not part of
/// the OIDC spec, so we need this as our AdditionalClaims type to access it
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetClaims {
    pub groups: Vec<String>,
}
impl AdditionalClaims for MetClaims {}

/// Needed for ['Client']
pub type MetIdTokenFields = IdTokenFields<
    MetClaims,
    EmptyExtraTokenFields,
    CoreGenderClaim,
    CoreJweContentEncryptionAlgorithm,
    CoreJwsSigningAlgorithm,
>;

pub type MetIdTokenClaims = IdTokenClaims<MetClaims, CoreGenderClaim>;

pub type MetIdToken =
    IdToken<MetClaims, CoreGenderClaim, CoreJweContentEncryptionAlgorithm, CoreJwsSigningAlgorithm>;

pub type MetTokenResponse = StandardTokenResponse<MetIdTokenFields, CoreTokenType>;

/// Because we have custom additional claims, we can't use the core type aliases like in the
/// example, and instead need to define our own type aliases that injects MetClaims
pub type Client<
    HasAuthUrl = EndpointSet,
    HasDeviceAuthUrl = EndpointNotSet,
    HasIntrospectionUrl = EndpointNotSet,
    HasRevocationUrl = EndpointNotSet,
    HasTokenUrl = EndpointMaybeSet,
    HasUserInfoUrl = EndpointMaybeSet,
> = openidconnect::Client<
    MetClaims,
    CoreAuthDisplay,
    CoreGenderClaim,
    CoreJweContentEncryptionAlgorithm,
    CoreJsonWebKey,
    CoreAuthPrompt,
    StandardErrorResponse<CoreErrorResponseType>,
    MetTokenResponse,
    CoreTokenIntrospectionResponse,
    CoreRevocableToken,
    CoreRevocationErrorResponse,
    HasAuthUrl,
    HasDeviceAuthUrl,
    HasIntrospectionUrl,
    HasRevocationUrl,
    HasTokenUrl,
    HasUserInfoUrl,
>;

/// Query string the OIDC issuer includes when redirecting back to us
#[derive(Debug, Deserialize)]
pub struct RedirectQuery {
    code: String,
    state: String,
    // TODO: can we get away with removing this?
    #[allow(dead_code)]
    session_state: Option<String>,
}

// TODO: document
pub async fn redirect_handler(
    session: Session,
    Query(query): Query<RedirectQuery>,
) -> Result<Redirect, (StatusCode, String)> {
    let state = session
        .get(OidcState::SESSION_KEY)
        .await
        .map_err(internal)?;
    if let Some(OidcState {
        csrf_token,
        nonce,
        pkce_verifier,
        next_url,
    }) = state
    {
        // the oidc state is not valid to be used more than once, so we remove it
        // to prevent that from happening
        session
            .remove::<OidcState>(OidcState::SESSION_KEY)
            .await
            .map_err(internal)?;

        if csrf_token.secret() != &query.state {
            return Err((
                StatusCode::CONFLICT,
                "csrf token state in the redirect handler did not match what we \
                 generated in the auth url"
                    .to_string(),
            ));
        }

        let http_client = openidconnect::reqwest::ClientBuilder::new()
            // Following redirects opens the client up to SSRF vulnerabilities.
            .redirect(openidconnect::reqwest::redirect::Policy::none())
            .build()
            .expect("http client should build");

        let token_response = CLIENT
            .get()
            .expect("must initialize CLIENT before trying to do auth")
            .exchange_code(AuthorizationCode::new(query.code))
            .map_err(internal)?
            .set_pkce_verifier(pkce_verifier)
            .request_async(&http_client)
            .await
            .map_err(internal)?;

        let id_token = token_response
            .id_token()
            .ok_or(Error::IdTokenMissing)
            .map_err(internal)?;
        let id_token_verifier = CLIENT
            .get()
            .expect("must initialize CLIENT before trying to do auth")
            .id_token_verifier();
        let claims = id_token
            .claims(&id_token_verifier, &nonce)
            .map_err(internal)?;

        if let Some(expected_access_token_hash) = claims.access_token_hash() {
            let actual_access_token_hash = AccessTokenHash::from_token(
                token_response.access_token(),
                id_token.signing_alg().map_err(internal)?,
                id_token.signing_key(&id_token_verifier).map_err(internal)?,
            )
            .map_err(internal)?;
            if actual_access_token_hash != *expected_access_token_hash {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "the access token hash we computed did not match the one included in the token"
                        .to_string(),
                ));
            }
        }

        let met_claims: &MetClaims = claims.additional_claims();

        let user = Some(claims.subject().as_str().to_string());
        let cms_base = met_claims.groups.contains(&"/AD/lard-cms-base".to_string());
        // TODO: populate these?
        let permit_roles = Vec::new();
        let station_roles = Vec::new();

        let auth = Auth {
            user,
            cms_base,
            permit_roles,
            station_roles,
        };

        session
            .insert(Auth::SESSION_KEY, auth)
            .await
            .map_err(internal)?;

        Ok(Redirect::to(&next_url.to_string()))
    } else {
        Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "No oidc challenge was found associated with this session, please make sure you are \
             not deleting your cookies, and have gone through the normal login process before \
             reaching this endpoint"
                .to_string(),
        ))
    }
}

pub async fn create_oidc_client(
    issuer_url: String,
    client_id: String,
    client_secret: Option<String>,
    redirect_url: String,
) -> Result<Client, Error> {
    let http_client = openidconnect::reqwest::ClientBuilder::new()
        .redirect(openidconnect::reqwest::redirect::Policy::none())
        .build()
        .expect("http client should build");

    let provider_metadata = CoreProviderMetadata::discover_async(
        IssuerUrl::new(issuer_url).expect("redirect_url must be a valid url"),
        &http_client,
    )
    .await?;
    Ok(Client::from_provider_metadata(
        provider_metadata,
        ClientId::new(client_id),
        client_secret.map(ClientSecret::new),
    )
    .set_redirect_uri(RedirectUrl::new(redirect_url).expect("redirect_url must be a valid url")))
}

/// Start an the oidc auth process.
///
/// Generates an auth url to redirect the user to, and stores state that will be needed in
/// [`redirect_handler`] in a cookie
pub async fn init_auth_challenge(session: &Session, next_url: String) -> Result<Redirect, Error> {
    let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();
    let (auth_url, csrf_token, nonce) = CLIENT
        .get()
        .expect("must initialize CLIENT before trying to do auth")
        .authorize_url(
            CoreAuthenticationFlow::AuthorizationCode,
            CsrfToken::new_random,
            Nonce::new_random,
        )
        // this scope is required to get the groups claim
        .add_scope(Scope::new("groups".to_string()))
        .set_pkce_challenge(pkce_challenge)
        .url();

    session
        .insert(
            OidcState::SESSION_KEY,
            OidcState {
                csrf_token,
                nonce,
                pkce_verifier,
                next_url,
            },
        )
        .await?;

    // send user to the oidc issuer to get an auth code
    Ok(Redirect::to(auth_url.as_str()))
}
