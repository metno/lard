use chrono::{Duration, Utc};
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, encode};

use crate::auth::{Access, Claims, Resource, Roles};

pub fn mock_auth_certs() -> DecodingKey {
    jsonwebtoken::DecodingKey::from_ec_pem(
        b"-----BEGIN PUBLIC KEY-----
MHYwEAYHKoZIzj0CAQYFK4EEACIDYgAETz7rFlJZ8IM7r53QKr7hF6GitWKpY3FN
tqdj2gL4EFqYX459/hpSh7w5hIW8k8mmftDz0Pm12CmV9MyvD1Lv1pucYyoJLobR
wARDennWSrMRamnmbyLO6jno3N9mNFtq
-----END PUBLIC KEY-----",
    )
    .unwrap()
}

pub fn create_mock_jwt(roles: Roles) -> Option<String> {
    let now = Utc::now();
    let expiration_time = now + Duration::weeks(520); // Token valid for 10 years

    let claims = Claims {
        resource_access: Access {
            resource: Resource { roles },
        },
        exp: expiration_time.timestamp() as usize,
    };

    // Create header
    let header = Header::new(Algorithm::ES384);

    // Create encoding key from test private key (this should corresponds to the public key in mock_auth_certs())
    // NOTE: this is just used for testing
    let encoding_key = EncodingKey::from_ec_pem(
        b"-----BEGIN PRIVATE KEY-----
MIG2AgEAMBAGByqGSM49AgEGBSuBBAAiBIGeMIGbAgEBBDDhihKsqOZ3ph6JqXnA
qDsU368kko3rmLDerN8zn3HkERY4cSETRYqXnCSrSEVVwpehZANiAARPPusWUlnw
gzuvndAqvuEXoaK1YqljcU22p2PaAvgQWphfjn3+GlKHvDmEhbyTyaZ+0PPQ+bXY
KZX0zK8PUu/Wm5xjKgkuhtHABEN6edZKsxFqaeZvIs7qOejc32Y0W2o=
-----END PRIVATE KEY-----",
    );
    match encoding_key {
        Ok(key) => encode(&header, &claims, &key).ok(),
        // This is just for testing so we return errors as None
        Err(_) => None,
    }
}
