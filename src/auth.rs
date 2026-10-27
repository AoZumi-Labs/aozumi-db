//! Database tokens: `adb1.<base64url(claims JSON)>.<base64url(HMAC-SHA256)>`,
//! no padding, the MAC taken over everything before the last dot. Any holder
//! of the secret (the AOZUMI control plane) can mint them.

use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use base64::Engine;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use subtle::ConstantTimeEq;

const PREFIX: &str = "adb1";

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct Claims {
    pub db: String,
    /// The database's token generation when the token was minted. Tokens
    /// below the database's current generation are refused, so rotating it
    /// revokes every token minted before. Absent means 0.
    #[serde(rename = "gen", default, skip_serializing_if = "is_zero")]
    pub generation: u64,
    /// Unix seconds. The token is refused from this second on.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exp: Option<u64>,
    /// Reads only. Absent means read-write.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub read_only: bool,
}

pub fn mint(secret: &[u8], claims: &Claims) -> String {
    let payload = format!("{PREFIX}.{}", B64.encode(serde_json::to_vec(claims).unwrap()));
    let mac = B64.encode(sign(secret, &payload));
    format!("{payload}.{mac}")
}

pub fn verify(secret: &[u8], token: &str, now: u64) -> Option<Claims> {
    let (payload, mac) = token.rsplit_once('.')?;
    let (prefix, claims) = payload.split_once('.')?;
    if prefix != PREFIX {
        return None;
    }
    let mac = B64.decode(mac).ok()?;
    if !bool::from(sign(secret, payload).as_slice().ct_eq(&mac)) {
        return None;
    }
    let claims: Claims = serde_json::from_slice(&B64.decode(claims).ok()?).ok()?;
    match claims.exp {
        Some(exp) if exp <= now => None,
        _ => Some(claims),
    }
}

/// The admin API takes the secret itself as its bearer credential.
pub fn is_admin(secret: &[u8], presented: &str) -> bool {
    bool::from(secret.ct_eq(presented.as_bytes()))
}

fn is_zero(n: &u64) -> bool {
    *n == 0
}

fn sign(secret: &[u8], payload: &str) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("HMAC takes keys of any length");
    mac.update(payload.as_bytes());
    mac.finalize().into_bytes().to_vec()
}
