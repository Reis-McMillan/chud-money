//! Kalshi request signing: RSA-PSS(SHA-256) over `timestamp_ms + METHOD + path`.

use anyhow::{Context, Result};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as B64;
use rsa::RsaPrivateKey;
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::pss::SigningKey;
use rsa::signature::{RandomizedSigner, SignatureEncoding};
use sha2::Sha256;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct Auth {
    pub key_id: String,
    signing_key: SigningKey<Sha256>,
}

impl Auth {
    /// `pem` is the API private key. PKCS#8 (`BEGIN PRIVATE KEY`) is what Kalshi
    /// hands out; PKCS#1 (`BEGIN RSA PRIVATE KEY`) is accepted as a fallback.
    pub fn new(key_id: String, pem: &str) -> Result<Self> {
        let key = RsaPrivateKey::from_pkcs8_pem(pem)
            .or_else(|_| RsaPrivateKey::from_pkcs1_pem(pem))
            .context("private key is not a valid PKCS#8 or PKCS#1 RSA PEM")?;
        Ok(Self { key_id, signing_key: SigningKey::<Sha256>::new(key) })
    }

    /// Returns `(timestamp_ms, base64_signature)` for one request.
    ///
    /// `path` must exclude the query string: sign `/trade-api/v2/markets`, not
    /// `/trade-api/v2/markets?series_ticker=...`.
    pub fn sign(&self, method: &str, path: &str) -> (String, String) {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_millis()
            .to_string();
        let msg = format!("{ts}{method}{path}");
        let sig = self.signing_key.sign_with_rng(&mut rand::thread_rng(), msg.as_bytes());
        (ts, B64.encode(sig.to_bytes()))
    }
}
