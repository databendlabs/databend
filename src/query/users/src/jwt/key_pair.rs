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
use jwt_simple::algorithms::ECDSAP256PublicKeyLike;
use jwt_simple::algorithms::ECDSAP384PublicKeyLike;
use jwt_simple::algorithms::ES256PublicKey;
use jwt_simple::algorithms::ES384PublicKey;
use jwt_simple::algorithms::Ed25519PublicKey;
use jwt_simple::algorithms::EdDSAPublicKeyLike;
use jwt_simple::algorithms::RS256PublicKey;
use jwt_simple::algorithms::RSAPublicKeyLike;
use jwt_simple::prelude::NoCustomClaims;
use jwt_simple::prelude::JWTClaims;
use jwt_simple::token::Token;

pub enum PublicKeyType {
    RSA256(Box<RS256PublicKey>),
    ES256(ES256PublicKey),
    ES384(ES384PublicKey),
    Ed25519(Ed25519PublicKey),
}

pub fn parse_public_key_pem(pem: &str) -> Result<PublicKeyType> {
    if let Ok(key) = RS256PublicKey::from_pem(pem) {
        return Ok(PublicKeyType::RSA256(Box::new(key)));
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
        "invalid public key: expected PEM-encoded RSA, ECDSA (P-256/P-384), or Ed25519 public key",
    ))
}

pub fn validate_public_key_pem(pem: &str) -> Result<()> {
    parse_public_key_pem(pem)?;
    Ok(())
}

fn verify_token_with_key(
    token: &str,
    key: &PublicKeyType,
) -> std::result::Result<JWTClaims<NoCustomClaims>, jwt_simple::Error> {
    match key {
        PublicKeyType::RSA256(pk) => pk.verify_token::<NoCustomClaims>(token, None),
        PublicKeyType::ES256(pk) => pk.verify_token::<NoCustomClaims>(token, None),
        PublicKeyType::ES384(pk) => pk.verify_token::<NoCustomClaims>(token, None),
        PublicKeyType::Ed25519(pk) => pk.verify_token::<NoCustomClaims>(token, None),
    }
}

pub fn decode_jwt_subject(token: &str) -> Result<String> {
    let _ = Token::decode_metadata(token)
        .map_err(|e| ErrorCode::AuthenticateFailure(format!("invalid JWT token: {e}")))?;
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() != 3 {
        return Err(ErrorCode::AuthenticateFailure("invalid JWT token format"));
    }
    use base64::Engine;
    #[derive(serde::Deserialize)]
    struct Claims {
        sub: Option<String>,
    }
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(parts[1])
        .map_err(|e| ErrorCode::AuthenticateFailure(format!("invalid JWT payload: {e}")))?;
    let claims: Claims = serde_json::from_slice(&payload)
        .map_err(|e| ErrorCode::AuthenticateFailure(format!("invalid JWT claims: {e}")))?;
    claims.sub.ok_or_else(|| {
        ErrorCode::AuthenticateFailure(
            "JWT authentication failed: 'sub' (subject/username) claim is missing",
        )
    })
}

pub fn verify_key_pair_jwt(token: &str, public_keys: &[String]) -> Result<String> {
    let subject = decode_jwt_subject(token)?;

    let mut last_err = String::new();
    for pem in public_keys {
        let key = match parse_public_key_pem(pem) {
            Ok(k) => k,
            Err(_) => continue,
        };
        match verify_token_with_key(token, &key) {
            Ok(_) => return Ok(subject),
            Err(e) => {
                last_err = e.to_string();
            }
        }
    }
    Err(ErrorCode::AuthenticateFailure(format!(
        "key-pair authentication failed: no stored public key could verify the token: {last_err}"
    )))
}
