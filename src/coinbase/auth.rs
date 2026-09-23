//! CDP request signing: a short-lived EdDSA JWT per REST request (sent as a
//! bearer token) or per websocket subscribe (sent in the message).

use anyhow::{Context, Result};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use jsonwebtoken::{Algorithm, EncodingKey, Header, get_current_timestamp};
use serde::Serialize;

/// Host the REST JWT's `uri` claim is bound to.
#[allow(dead_code)] // no REST caller today; kept with `bearer` for the next one
pub const API_HOST: &str = "api.coinbase.com";
/// Coinbase rejects tokens that live longer than two minutes.
const TOKEN_TTL_SECS: u64 = 120;
/// DER of a PKCS#8 v1 `PrivateKeyInfo` for Ed25519, up to the 32-byte seed.
const ED25519_PKCS8_PREFIX: [u8; 16] =
    [0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20];

pub struct Auth {
    pub key_id: String,
    key: EncodingKey,
}

#[derive(Serialize)]
struct Claims<'a> {
    sub: &'a str,
    iss: &'static str,
    nbf: u64,
    exp: u64,
    /// `METHOD host/path` for REST; absent for websocket tokens.
    #[serde(skip_serializing_if = "Option::is_none")]
    uri: Option<String>,
}

impl Auth {
    /// `secret_b64` is the API key's private key as CDP hands it out: base64
    /// of the raw Ed25519 key, either seed ‖ public key (64 bytes) or the
    /// bare seed (32 bytes).
    pub fn new(key_id: String, secret_b64: &str) -> Result<Self> {
        let raw = B64.decode(secret_b64.trim()).context("CDP key secret is not valid base64")?;
        anyhow::ensure!(
            raw.len() == 32 || raw.len() == 64,
            "CDP key secret decodes to {} bytes; expected a 32 or 64 byte Ed25519 key",
            raw.len()
        );
        // jsonwebtoken only loads Ed25519 keys as PKCS#8.
        let pkcs8 = [&ED25519_PKCS8_PREFIX[..], &raw[..32]].concat();
        Ok(Self { key_id, key: EncodingKey::from_ed_der(&pkcs8) })
    }

    /// Bearer token for one REST request. `path` must exclude the query
    /// string: the `uri` claim covers only `METHOD host/path`.
    #[allow(dead_code)] // see `API_HOST`
    pub fn bearer(&self, method: &str, path: &str) -> Result<String> {
        self.token(Some(format!("{method} {API_HOST}{path}")))
    }

    /// Token for one websocket `subscribe` message: the same claims without
    /// `uri`, which Coinbase does not bind websocket tokens to.
    pub fn ws_bearer(&self) -> Result<String> {
        self.token(None)
    }

    fn token(&self, uri: Option<String>) -> Result<String> {
        let mut header = Header::new(Algorithm::EdDSA);
        header.kid = Some(self.key_id.clone());
        header.nonce = Some(format!("{:032x}", rand::random::<u128>()));
        let now = get_current_timestamp();
        let claims = Claims { sub: &self.key_id, iss: "cdp", nbf: now, exp: now + TOKEN_TTL_SECS, uri };
        jsonwebtoken::encode(&header, &claims, &self.key).context("signing CDP token")
    }
}

#[cfg(test)]
mod tests {
    use aws_lc_rs::signature::{Ed25519KeyPair, KeyPair};
    use jsonwebtoken::{DecodingKey, Validation, decode, decode_header};
    use serde_json::Value;

    use super::*;

    const SEED: [u8; 32] = [7; 32];

    #[test]
    fn token_is_eddsa_and_verifies_with_the_public_key() {
        let pair = Ed25519KeyPair::from_seed_unchecked(&SEED).unwrap();
        let public = pair.public_key().as_ref().to_vec();
        // CDP's format: seed followed by the public key.
        let secret = B64.encode([&SEED[..], &public[..]].concat());
        let auth = Auth::new("key-id".into(), &secret).unwrap();
        let token = auth.bearer("GET", "/api/v3/brokerage/products/BTC-USD/candles").unwrap();

        let header = decode_header(&token).unwrap();
        assert_eq!(header.alg, Algorithm::EdDSA);
        assert_eq!(header.kid.as_deref(), Some("key-id"));
        assert_eq!(header.nonce.map(|n| n.len()), Some(32));

        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.set_required_spec_claims(&["exp", "nbf", "sub", "iss"]);
        let claims = decode::<Value>(&token, &DecodingKey::from_ed_der(&public), &validation).unwrap().claims;
        assert_eq!(claims["sub"], "key-id");
        assert_eq!(claims["iss"], "cdp");
        assert_eq!(claims["uri"], "GET api.coinbase.com/api/v3/brokerage/products/BTC-USD/candles");
        assert_eq!(claims["exp"].as_u64().unwrap() - claims["nbf"].as_u64().unwrap(), 120);
    }

    #[test]
    fn websocket_token_has_no_uri() {
        let pair = Ed25519KeyPair::from_seed_unchecked(&SEED).unwrap();
        let public = pair.public_key().as_ref().to_vec();
        let auth = Auth::new("key-id".into(), &B64.encode(SEED)).unwrap();
        let token = auth.ws_bearer().unwrap();
        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.set_required_spec_claims(&["exp", "nbf", "sub", "iss"]);
        let claims = decode::<Value>(&token, &DecodingKey::from_ed_der(&public), &validation).unwrap().claims;
        assert_eq!(claims["sub"], "key-id");
        assert_eq!(claims["iss"], "cdp");
        assert!(claims.get("uri").is_none(), "websocket tokens carry no uri: {claims}");
    }

    #[test]
    fn bare_seed_is_accepted_and_other_lengths_are_not() {
        assert!(Auth::new("k".into(), &B64.encode(SEED)).is_ok());
        assert!(Auth::new("k".into(), &B64.encode([0u8; 48])).is_err());
        assert!(Auth::new("k".into(), "not base64!").is_err());
    }
}
