// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::PublicKeyEntry;
use jwt_simple::algorithms::ECDSAP256PublicKeyLike;
use jwt_simple::algorithms::ECDSAP384PublicKeyLike;
use jwt_simple::algorithms::ES256PublicKey;
use jwt_simple::algorithms::ES384PublicKey;
use jwt_simple::algorithms::Ed25519PublicKey;
use jwt_simple::algorithms::EdDSAPublicKeyLike;
use jwt_simple::algorithms::RS256PublicKey;
use jwt_simple::algorithms::RS384PublicKey;
use jwt_simple::algorithms::RS512PublicKey;
use jwt_simple::algorithms::RSAPublicKeyLike;
use jwt_simple::prelude::JWTClaims;
use jwt_simple::prelude::NoCustomClaims;

/// Minimum RSA key size in bits.
const RSA_MIN_KEY_BITS: usize = 2048;

pub enum PublicKeyType {
    RSA(String), // PEM string; parsed into RS256/RS384/RS512 at verify time
    ES256(ES256PublicKey),
    ES384(ES384PublicKey),
    Ed25519(Ed25519PublicKey),
}

pub fn parse_public_key_pem(pem: &str) -> Result<PublicKeyType> {
    // Try RSA first (RS256/RS384/RS512 all parse from the same RSA PEM).
    // We use RS256 for detection; the actual algorithm is selected at verify time.
    if let Ok(key) = RS256PublicKey::from_pem(pem) {
        let components = key.to_components();
        let key_bits = components.n.len() * 8;
        if key_bits < RSA_MIN_KEY_BITS {
            return Err(ErrorCode::AuthenticateFailure(format!(
                "RSA key must be at least {RSA_MIN_KEY_BITS} bits, got {key_bits} bits"
            )));
        }
        return Ok(PublicKeyType::RSA(pem.to_string()));
    }
    if let Ok(key) = ES256PublicKey::from_pem(pem) {
        return Ok(PublicKeyType::ES256(key));
    }
    if let Ok(key) = ES384PublicKey::from_pem(pem) {
        return Ok(PublicKeyType::ES384(key));
    }
    if let Ok(key) = Ed25519PublicKey::from_pem(pem) {
        return Ok(PublicKeyType::Ed25519(key));
    }
    Err(ErrorCode::AuthenticateFailure(
        "invalid public key: expected PEM-encoded RSA (2048+ bits), ECDSA (P-256/P-384), or Ed25519 public key",
    ))
}

/// Validate a public key input (accepts full PEM or bare base64).
/// Validates using the stored format (normalize → reconstruct PEM) to ensure
/// round-trip correctness. This catches cases like PKCS#1 PEM (`BEGIN RSA PUBLIC KEY`)
/// which would pass initial parsing but fail after storage as SPKI (`BEGIN PUBLIC KEY`).
pub fn validate_public_key_pem(input: &str) -> Result<()> {
    let normalized = databend_common_meta_app::principal::normalize_public_key(input)?;
    let entry = databend_common_meta_app::principal::PublicKeyEntry {
        key: normalized,
        label: String::new(),
        created_at: 0,
    };
    let pem = entry.to_pem();
    parse_public_key_pem(&pem)?;
    Ok(())
}

fn verify_token_with_key(
    token: &str,
    key: &PublicKeyType,
) -> std::result::Result<JWTClaims<NoCustomClaims>, jwt_simple::Error> {
    match key {
        PublicKeyType::RSA(pem) => {
            // Try RS256, RS384, RS512 in order. The JWT's `alg` header determines
            // which hash the signer used; we attempt all three since the underlying
            // RSA key is the same.
            if let Ok(k) = RS256PublicKey::from_pem(pem) {
                if let Ok(claims) = k.verify_token::<NoCustomClaims>(token, None) {
                    return Ok(claims);
                }
            }
            if let Ok(k) = RS384PublicKey::from_pem(pem) {
                if let Ok(claims) = k.verify_token::<NoCustomClaims>(token, None) {
                    return Ok(claims);
                }
            }
            if let Ok(k) = RS512PublicKey::from_pem(pem) {
                if let Ok(claims) = k.verify_token::<NoCustomClaims>(token, None) {
                    return Ok(claims);
                }
            }
            Err(jwt_simple::Error::msg(
                "RSA signature verification failed with RS256, RS384, and RS512",
            ))
        }
        PublicKeyType::ES256(pk) => pk.verify_token::<NoCustomClaims>(token, None),
        PublicKeyType::ES384(pk) => pk.verify_token::<NoCustomClaims>(token, None),
        PublicKeyType::Ed25519(pk) => pk.verify_token::<NoCustomClaims>(token, None),
    }
}

/// Decode JWT claims without signature verification.
/// This is used to extract the subject (username) before looking up the user's public keys.
pub fn decode_jwt_claims_insecure(token: &str) -> Result<JwtClaimsDecoded> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(ErrorCode::AuthenticateFailure("invalid JWT token format"));
    }
    use base64::Engine;
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(parts[1])
        .map_err(|e| ErrorCode::AuthenticateFailure(format!("invalid JWT payload: {e}")))?;
    serde_json::from_slice(&payload)
        .map_err(|e| ErrorCode::AuthenticateFailure(format!("invalid JWT claims: {e}")))
}

#[derive(serde::Deserialize)]
pub struct JwtClaimsDecoded {
    pub sub: Option<String>,
    pub exp: Option<u64>,
}

/// Verify a JWT token against a list of public key entries.
/// Keys are stored as base64 body; PEM is reconstructed for verification.
pub fn verify_key_pair_jwt(token: &str, public_keys: &[PublicKeyEntry]) -> Result<()> {
    let mut last_err = String::new();
    for entry in public_keys {
        let pem = entry.to_pem();
        let key = match parse_public_key_pem(&pem) {
            Ok(k) => k,
            Err(_) => continue,
        };
        match verify_token_with_key(token, &key) {
            Ok(_) => return Ok(()),
            Err(e) => {
                last_err = e.to_string();
            }
        }
    }
    Err(ErrorCode::AuthenticateFailure(format!(
        "key-pair authentication failed: no stored public key could verify the token: {last_err}"
    )))
}
