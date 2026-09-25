//! Bearer-token guard for protected routes.
//!
//! The SPA logs into Verys itself and exchanges its session for an access
//! token scoped to this API (RFC 8693 token exchange with
//! `audience = CHUD_MONEY_API_CLIENT_ID`). Requests present that token as
//! `Authorization: Bearer <jwt>`; websocket upgrades and SSE streams, which
//! a browser cannot add headers to, pass it as `?access_token=<jwt>` instead.
//!
//! Signature, issuer, audience and expiry are checked against the Verys
//! signing key on every request. Exchanged tokens live for minutes and carry
//! no refresh token, so nothing is cached or refreshed here: the SPA
//! re-exchanges before expiry and an expired token is simply refused.
//!
//! A valid token is not enough on its own: the identity must also carry the
//! [`REQUIRED_ROLE`] in Verys, otherwise the request is refused with 403.

use axum::extract::{FromRequestParts, Request, State};
use axum::http::HeaderValue;
use axum::http::header::AUTHORIZATION;
use axum::http::request::Parts;
use axum::middleware::Next;
use axum::response::Response;
use jsonwebtoken::errors::ErrorKind;
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};

use crate::error::AppError;
use crate::state::AppState;

/// Verys role an identity needs before it may use protected routes.
pub const REQUIRED_ROLE: &str = "chud-money";

/// Query parameter carrying the token on websocket upgrades and SSE streams.
pub const ACCESS_TOKEN_PARAM: &str = "access_token";

/// Clock skew tolerated when checking `exp`.
const LEEWAY_SECS: u64 = 30;

/// The claims this API reads from an exchanged Verys access token. `iss`,
/// `aud` and `exp` are validated by `jsonwebtoken` straight from the payload
/// (where `aud` may be a string or an array), so they need no field here.
#[derive(Debug, Clone, Deserialize)]
pub struct AccessClaims {
    /// Verys identity id.
    pub sub: String,
    #[serde(default)]
    pub roles: Vec<String>,
}

/// The caller of a protected route. Inserted into request extensions by
/// [`authenticated`]; handlers take it as an extractor.
#[derive(Debug, Clone, Serialize)]
pub struct AuthUser {
    pub id: String,
    pub roles: Vec<String>,
}

impl AuthUser {
    pub fn has_role(&self, role: &str) -> bool {
        self.roles.iter().any(|r| r == role)
    }

    #[allow(dead_code)] // no admin-only route yet
    pub fn is_admin(&self) -> bool {
        self.has_role("admin")
    }
}

impl<S: Send + Sync> FromRequestParts<S> for AuthUser {
    type Rejection = AppError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        parts
            .extensions
            .get::<AuthUser>()
            .cloned()
            .ok_or_else(|| AppError::Unauthorized("not authenticated".into()))
    }
}

/// Middleware for protected routes; see the module docs.
pub async fn authenticated(
    State(state): State<AppState>,
    mut req: Request,
    next: Next,
) -> Result<Response, AppError> {
    let token = token_from(&req)?;
    let user = authenticate(&state, &token).await?;
    req.extensions_mut().insert(user);
    Ok(next.run(req).await)
}

/// Full check for one token: signature, issuer, audience and expiry against
/// the Verys signing key, then the required role.
pub async fn authenticate(state: &AppState, token: &str) -> Result<AuthUser, AppError> {
    let key = state.verys_client.public_key().await?;
    let claims = verify_access_token(key, &state.config.verys_issuer, &state.config.client_id, token)?;
    let user = AuthUser { id: claims.sub, roles: claims.roles };
    require_role(&user)?;
    Ok(user)
}

/// Verify signature, issuer, audience and expiry. Kept free of `AppState` so
/// it can be exercised with a throwaway key.
pub fn verify_access_token(
    key: &DecodingKey,
    issuer: &str,
    audience: &str,
    token: &str,
) -> Result<AccessClaims, AppError> {
    let mut validation = Validation::new(Algorithm::EdDSA);
    validation.set_issuer(&[issuer]);
    validation.set_audience(&[audience]);
    validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
    validation.validate_exp = true;
    validation.leeway = LEEWAY_SECS;
    jsonwebtoken::decode::<AccessClaims>(token, key, &validation)
        .map(|data| data.claims)
        .map_err(|e| match e.kind() {
            ErrorKind::ExpiredSignature => AppError::Unauthorized("access token expired".into()),
            _ => AppError::Jwt(e),
        })
}

fn require_role(user: &AuthUser) -> Result<(), AppError> {
    if user.has_role(REQUIRED_ROLE) {
        Ok(())
    } else {
        tracing::warn!(user = %user.id, roles = ?user.roles, "missing required role");
        Err(AppError::Forbidden(format!("this account lacks the `{REQUIRED_ROLE}` role")))
    }
}

/// `Authorization: Bearer …` wins; otherwise `?access_token=…` (websockets).
fn token_from(req: &Request) -> Result<String, AppError> {
    match req.headers().get(AUTHORIZATION) {
        Some(header) => bearer(header),
        None => query_token(req.uri().query()).ok_or_else(|| {
            AppError::Unauthorized(format!("authorization header or {ACCESS_TOKEN_PARAM} query parameter required"))
        }),
    }
}

fn bearer(header: &HeaderValue) -> Result<String, AppError> {
    let header = header
        .to_str()
        .map_err(|_| AppError::Unauthorized("malformed authorization header".into()))?;
    match header.split_once(' ') {
        Some((scheme, token)) if scheme.eq_ignore_ascii_case("bearer") && !token.trim().is_empty() => {
            Ok(token.trim().to_string())
        }
        _ => Err(AppError::Unauthorized("expected a bearer token".into())),
    }
}

/// A JWT is base64url plus dots, all URL-safe, so no percent-decoding is needed.
fn query_token(query: Option<&str>) -> Option<String> {
    query?
        .split('&')
        .filter_map(|kv| kv.split_once('='))
        .find(|(k, _)| *k == ACCESS_TOKEN_PARAM)
        .map(|(_, v)| v.to_string())
        .filter(|v| !v.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    use aws_lc_rs::signature::{Ed25519KeyPair, KeyPair};
    use axum::body::Body;
    use jsonwebtoken::{EncodingKey, Header, get_current_timestamp};
    use serde_json::{Value, json};

    const ISS: &str = "http://localhost:8080";
    const AUD: &str = "chud-money-api";

    fn keys() -> (EncodingKey, DecodingKey) {
        let pair = Ed25519KeyPair::generate().unwrap();
        let pkcs8 = pair.to_pkcs8().unwrap();
        (EncodingKey::from_ed_der(pkcs8.as_ref()), DecodingKey::from_ed_der(pair.public_key().as_ref()))
    }

    fn sign(key: &EncodingKey, claims: &Value) -> String {
        jsonwebtoken::encode(&Header::new(Algorithm::EdDSA), claims, key).unwrap()
    }

    fn claims(exp_offset: i64, aud: Value) -> Value {
        let now = get_current_timestamp() as i64;
        json!({
            "iss": ISS,
            "sub": "6f1c2b4e-3d5a-4c7b-9e8f-0a1b2c3d4e5f",
            "aud": aud,
            "roles": ["chud-money"],
            "scopes": [],
            "iat": now,
            "exp": now + exp_offset,
        })
    }

    #[test]
    fn valid_token_yields_claims() {
        let (enc, dec) = keys();
        let c = verify_access_token(&dec, ISS, AUD, &sign(&enc, &claims(300, json!(AUD)))).unwrap();
        assert_eq!(c.sub, "6f1c2b4e-3d5a-4c7b-9e8f-0a1b2c3d4e5f");
        assert_eq!(c.roles, ["chud-money"]);
    }

    #[test]
    fn audience_may_be_an_array() {
        let (enc, dec) = keys();
        assert!(verify_access_token(&dec, ISS, AUD, &sign(&enc, &claims(300, json!([AUD, "other"])))).is_ok());
    }

    #[test]
    fn expiry_is_enforced_with_leeway() {
        let (enc, dec) = keys();
        assert!(verify_access_token(&dec, ISS, AUD, &sign(&enc, &claims(-10, json!(AUD)))).is_ok());
        match verify_access_token(&dec, ISS, AUD, &sign(&enc, &claims(-120, json!(AUD)))) {
            Err(AppError::Unauthorized(m)) => assert_eq!(m, "access token expired"),
            other => panic!("expected expired, got {other:?}"),
        }
    }

    #[test]
    fn wrong_audience_issuer_key_or_missing_sub_is_rejected() {
        let (enc, dec) = keys();
        let (other_enc, _) = keys();
        let mut no_sub = claims(300, json!(AUD));
        no_sub.as_object_mut().unwrap().remove("sub");

        let cases = [
            sign(&enc, &claims(300, json!("someone-else"))),
            sign(&enc, &{
                let mut c = claims(300, json!(AUD));
                c["iss"] = json!("http://evil.example");
                c
            }),
            sign(&other_enc, &claims(300, json!(AUD))),
            sign(&enc, &no_sub),
        ];
        for token in cases {
            assert!(matches!(verify_access_token(&dec, ISS, AUD, &token), Err(AppError::Jwt(_))), "{token}");
        }
    }

    #[test]
    fn required_role_is_enforced() {
        let user = |roles: &[&str]| AuthUser { id: "id".into(), roles: roles.iter().map(|r| r.to_string()).collect() };
        assert!(require_role(&user(&["chud-money"])).is_ok());
        assert!(require_role(&user(&["admin", "chud-money"])).is_ok());
        assert!(matches!(require_role(&user(&["admin"])), Err(AppError::Forbidden(_))));
        assert!(matches!(require_role(&user(&[])), Err(AppError::Forbidden(_))));
    }

    #[test]
    fn bearer_header_parsing() {
        let parse = |v: &str| bearer(&HeaderValue::from_str(v).unwrap());
        assert_eq!(parse("Bearer abc.def.ghi").unwrap(), "abc.def.ghi");
        assert_eq!(parse("bearer  abc ").unwrap(), "abc");
        assert!(parse("Basic abc").is_err());
        assert!(parse("Bearer ").is_err());
        assert!(parse("abc").is_err());
    }

    #[test]
    fn token_comes_from_header_or_query() {
        let req = |uri: &str, header: Option<&str>| {
            let mut r = Request::builder().uri(uri).body(Body::empty()).unwrap();
            if let Some(v) = header {
                r.headers_mut().insert(AUTHORIZATION, HeaderValue::from_str(v).unwrap());
            }
            r
        };
        assert_eq!(token_from(&req("/ws/x/ticker", Some("Bearer h"))).unwrap(), "h");
        assert_eq!(token_from(&req("/ws/x/ticker?access_token=q", None)).unwrap(), "q");
        assert_eq!(token_from(&req("/ws/x/ticker?foo=1&access_token=q", Some("Bearer h"))).unwrap(), "h");
        assert!(token_from(&req("/ws/x/ticker?access_token=", None)).is_err());
        assert!(token_from(&req("/ws/x/ticker?foo=1", None)).is_err());
        assert!(token_from(&req("/ws/x/ticker", None)).is_err());
    }
}
