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

use std::str::FromStr;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use sha2::Digest;
use sha2::Sha256;

const NO_PASSWORD_STR: &str = "no_password";
const SHA256_PASSWORD_STR: &str = "sha256_password";
const DOUBLE_SHA1_PASSWORD_STR: &str = "double_sha1_password";
const JWT_AUTH_STR: &str = "jwt";
const KEY_PAIR_STR: &str = "key_pair";

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct PublicKeyEntry {
    /// Base64-encoded public key body (no PEM headers/footers)
    pub key: String,
    /// User-provided label for identification
    pub label: String,
    /// ISO 8601 timestamp when the key was added
    pub created_at: i64,
}

impl PublicKeyEntry {
    pub fn fingerprint(&self) -> Result<String> {
        sha256_fingerprint(&self.key)
    }

    /// Reconstruct full PEM from stored base64 body.
    /// Wraps at 64 characters per line as required by PEM format.
    pub fn to_pem(&self) -> String {
        let mut lines = Vec::new();
        let mut i = 0;
        while i < self.key.len() {
            let end = (i + 64).min(self.key.len());
            lines.push(&self.key[i..end]);
            i = end;
        }
        format!(
            "-----BEGIN PUBLIC KEY-----\n{}\n-----END PUBLIC KEY-----",
            lines.join("\n")
        )
    }
}

/// Normalize a public key input: accept full PEM or bare base64, return base64 body only.
/// Validates that the result is valid base64 that decodes to non-empty bytes.
pub fn normalize_public_key(input: &str) -> Result<String> {
    use base64::Engine;
    let trimmed = input.trim();
    let body = if trimmed.starts_with("-----BEGIN") {
        trimmed
            .lines()
            .filter(|line| !line.starts_with("-----"))
            .collect::<Vec<_>>()
            .join("")
    } else {
        trimmed.chars().filter(|c| !c.is_whitespace()).collect()
    };
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(&body)
        .map_err(|e| {
            ErrorCode::AuthenticateFailure(format!("invalid public key: bad base64 encoding: {e}"))
        })?;
    if decoded.is_empty() {
        return Err(ErrorCode::AuthenticateFailure(
            "invalid public key: empty key data",
        ));
    }
    Ok(body)
}

/// Compute fingerprint compatible with OpenSSH: SHA-256 of DER bytes, base64-encoded.
pub fn sha256_fingerprint(key_base64: &str) -> Result<String> {
    use base64::Engine;
    let der_bytes = base64::engine::general_purpose::STANDARD
        .decode(key_base64)
        .map_err(|e| ErrorCode::AuthenticateFailure(format!("invalid public key base64: {e}")))?;
    let digest = Sha256::digest(&der_bytes);
    let encoded = base64::engine::general_purpose::STANDARD_NO_PAD.encode(digest);
    Ok(format!("SHA256:{}", encoded))
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub enum AuthType {
    NoPassword,
    Sha256Password,
    DoubleSha1Password,
    JWT,
    KeyPair,
}

impl FromStr for AuthType {
    type Err = ErrorCode;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            SHA256_PASSWORD_STR => Ok(AuthType::Sha256Password),
            DOUBLE_SHA1_PASSWORD_STR => Ok(AuthType::DoubleSha1Password),
            NO_PASSWORD_STR => Ok(AuthType::NoPassword),
            JWT_AUTH_STR => Ok(AuthType::JWT),
            KEY_PAIR_STR => Ok(AuthType::KeyPair),
            _ => Err(ErrorCode::AuthenticateFailure(AuthType::bad_auth_types(s))),
        }
    }
}

impl AuthType {
    pub fn to_str(&self) -> &str {
        match self {
            AuthType::NoPassword => NO_PASSWORD_STR,
            AuthType::Sha256Password => SHA256_PASSWORD_STR,
            AuthType::DoubleSha1Password => DOUBLE_SHA1_PASSWORD_STR,
            AuthType::JWT => JWT_AUTH_STR,
            AuthType::KeyPair => KEY_PAIR_STR,
        }
    }

    fn bad_auth_types(s: &str) -> String {
        let all = [
            NO_PASSWORD_STR,
            SHA256_PASSWORD_STR,
            DOUBLE_SHA1_PASSWORD_STR,
            JWT_AUTH_STR,
            KEY_PAIR_STR,
        ];
        let all = all
            .iter()
            .map(|s| format!("'{}'", s))
            .collect::<Vec<_>>()
            .join("|");
        format!("Expected auth type {}, found: {}", all, s)
    }

    pub fn get_password_type(self) -> Option<PasswordHashMethod> {
        match self {
            AuthType::Sha256Password => Some(PasswordHashMethod::Sha256),
            AuthType::DoubleSha1Password => Some(PasswordHashMethod::DoubleSha1),
            _ => None,
        }
    }
}

impl From<databend_common_ast::ast::AuthType> for AuthType {
    fn from(t: databend_common_ast::ast::AuthType) -> Self {
        match t {
            databend_common_ast::ast::AuthType::NoPassword => AuthType::NoPassword,
            databend_common_ast::ast::AuthType::Sha256Password => AuthType::Sha256Password,
            databend_common_ast::ast::AuthType::DoubleSha1Password => AuthType::DoubleSha1Password,
            databend_common_ast::ast::AuthType::JWT => AuthType::JWT,
            databend_common_ast::ast::AuthType::KeyPair => AuthType::KeyPair,
        }
    }
}

#[derive(
    serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Default,
)]
pub enum AuthInfo {
    #[default]
    None,
    Password {
        hash_value: Vec<u8>,
        hash_method: PasswordHashMethod,
        need_change: bool,
    },
    JWT,
    KeyPair {
        public_keys: Vec<PublicKeyEntry>,
    },
}

fn calc_sha1(v: &[u8]) -> [u8; 20] {
    let mut m = ::sha1::Sha1::new();
    m.update(v);
    m.finalize().into()
}

fn double_sha1(v: &[u8]) -> [u8; 20] {
    calc_sha1(&calc_sha1(v)[..])
}

impl AuthInfo {
    pub fn new(
        auth_type: AuthType,
        auth_string: &Option<String>,
        need_change: bool,
    ) -> Result<AuthInfo> {
        match auth_type {
            AuthType::NoPassword => Ok(AuthInfo::None),
            AuthType::JWT => Ok(AuthInfo::JWT),
            AuthType::KeyPair => match auth_string {
                Some(key_input) => {
                    let key = normalize_public_key(key_input)?;
                    Ok(AuthInfo::KeyPair {
                        public_keys: vec![PublicKeyEntry {
                            key,
                            label: String::new(),
                            created_at: chrono::Utc::now().timestamp(),
                        }],
                    })
                }
                None => Err(ErrorCode::AuthenticateFailure(
                    "need public key for key_pair authentication".to_string(),
                )),
            },
            AuthType::Sha256Password | AuthType::DoubleSha1Password => match auth_string {
                Some(p) => {
                    let method = auth_type.get_password_type().unwrap();
                    Ok(AuthInfo::Password {
                        hash_value: method.hash(p.as_bytes()),
                        hash_method: method,
                        need_change,
                    })
                }
                None => Err(ErrorCode::AuthenticateFailure("need password".to_string())),
            },
        }
    }

    pub fn create(auth_type: &Option<String>, auth_string: &Option<String>) -> Result<AuthInfo> {
        let default = AuthType::DoubleSha1Password;
        let auth_type = auth_type
            .clone()
            .map(|s| AuthType::from_str(&s))
            .transpose()?
            .unwrap_or(default);
        AuthInfo::new(auth_type, auth_string, false)
    }

    pub fn create2(
        auth_type: &Option<AuthType>,
        auth_string: &Option<String>,
        need_change: bool,
    ) -> Result<AuthInfo> {
        let default = AuthType::DoubleSha1Password;
        let auth_type = auth_type.clone().unwrap_or(default);
        AuthInfo::new(auth_type, auth_string, need_change)
    }

    // create `AuthInfo` and only modify `need_change` field.
    pub fn create_with_need_change(&self, need_change: bool) -> AuthInfo {
        match self {
            AuthInfo::Password {
                hash_value,
                hash_method,
                ..
            } => AuthInfo::Password {
                hash_value: hash_value.clone(),
                hash_method: *hash_method,
                need_change,
            },
            _ => self.clone(),
        }
    }

    pub fn alter(
        &self,
        auth_type: &Option<String>,
        auth_string: &Option<String>,
    ) -> Result<AuthInfo> {
        let old_auth_type = self.get_type();
        let new_auth_type = auth_type
            .clone()
            .map(|s| AuthType::from_str(&s))
            .transpose()?
            .unwrap_or(old_auth_type);
        AuthInfo::new(new_auth_type, auth_string, false)
    }

    pub fn alter2(
        &self,
        auth_type: &Option<AuthType>,
        auth_string: &Option<String>,
        need_change: bool,
    ) -> Result<AuthInfo> {
        let old_auth_type = self.get_type();
        let new_auth_type = auth_type.clone().unwrap_or(old_auth_type);

        AuthInfo::new(new_auth_type, auth_string, need_change)
    }

    pub fn get_type(&self) -> AuthType {
        match self {
            AuthInfo::None => AuthType::NoPassword,
            AuthInfo::JWT => AuthType::JWT,
            AuthInfo::KeyPair { .. } => AuthType::KeyPair,
            AuthInfo::Password { hash_method: t, .. } => match t {
                PasswordHashMethod::Sha256 => AuthType::Sha256Password,
                PasswordHashMethod::DoubleSha1 => AuthType::DoubleSha1Password,
            },
        }
    }

    pub fn get_need_change(&self) -> bool {
        match self {
            AuthInfo::None => false,
            AuthInfo::JWT => false,
            AuthInfo::KeyPair { .. } => false,
            AuthInfo::Password { need_change, .. } => *need_change,
        }
    }

    pub fn get_auth_string(&self) -> String {
        match self {
            AuthInfo::Password {
                hash_value: p,
                hash_method: t,
                ..
            } => t.to_string(p),
            AuthInfo::None | AuthInfo::JWT | AuthInfo::KeyPair { .. } => "".to_string(),
        }
    }

    pub fn get_password(&self) -> Option<Vec<u8>> {
        match self {
            AuthInfo::Password {
                hash_value: p,
                hash_method: _,
                ..
            } => Some(p.to_vec()),
            _ => None,
        }
    }

    pub fn get_password_type(&self) -> Option<PasswordHashMethod> {
        match self {
            AuthInfo::Password {
                hash_value: _,
                hash_method: t,
                ..
            } => Some(*t),
            _ => None,
        }
    }

    fn restore_sha1_mysql(salt: &[u8], input: &[u8], user_password_hash: &[u8]) -> Result<Vec<u8>> {
        // SHA1( password ) XOR SHA1( "20-bytes random data from server" <concat> SHA1( SHA1( password ) ) )
        let mut m = sha1::Sha1::new();
        m.update(salt);
        m.update(user_password_hash);

        let result: [u8; 20] = m.finalize().into();
        if input.len() != result.len() {
            return Err(ErrorCode::SHA1CheckFailed("SHA1 check failed"));
        }
        let mut s = Vec::with_capacity(result.len());
        for i in 0..result.len() {
            s.push(input[i] ^ result[i]);
        }
        Ok(s)
    }

    pub fn auth_mysql(&self, password_input: &[u8], salt: &[u8]) -> Result<bool> {
        match self {
            AuthInfo::None => Ok(true),
            AuthInfo::Password {
                hash_value: p,
                hash_method: t,
                ..
            } => match t {
                PasswordHashMethod::DoubleSha1 => {
                    let password_sha1 = AuthInfo::restore_sha1_mysql(salt, password_input, p)?;
                    Ok(*p == calc_sha1(&password_sha1))
                }
                PasswordHashMethod::Sha256 => Err(ErrorCode::AuthenticateFailure(
                    "login with sha256_password user for mysql protocol not supported yet.",
                )),
            },
            AuthInfo::KeyPair { .. } => Err(ErrorCode::AuthenticateFailure(
                "key-pair authentication is not supported over MySQL protocol, use HTTP instead.",
            )),
            _ => Err(ErrorCode::AuthenticateFailure(format!(
                "user require auth type {}",
                self.get_type().to_str()
            ))),
        }
    }

    pub fn get_public_keys(&self) -> &[PublicKeyEntry] {
        match self {
            AuthInfo::KeyPair { public_keys } => public_keys,
            _ => &[],
        }
    }

    pub fn add_public_key(&mut self, entry: PublicKeyEntry) -> Result<()> {
        match self {
            AuthInfo::KeyPair { public_keys } => {
                // Reject duplicate fingerprints
                let fp = entry.fingerprint()?;
                if public_keys.iter().any(|k| k.fingerprint().ok() == Some(fp.clone())) {
                    return Err(ErrorCode::AuthenticateFailure(format!(
                        "public key with fingerprint '{}' already exists",
                        fp
                    )));
                }
                public_keys.push(entry);
                Ok(())
            }
            _ => Err(ErrorCode::AuthenticateFailure(
                "user is not configured for key-pair authentication, use IDENTIFIED WITH key_pair BY '<key>' first".to_string(),
            )),
        }
    }

    pub fn remove_public_key_by_label(&mut self, label: &str) -> Result<()> {
        match self {
            AuthInfo::KeyPair { public_keys } => {
                let idx = public_keys.iter().position(|k| k.label == label);
                match idx {
                    None => Err(ErrorCode::AuthenticateFailure(format!(
                        "public key with label '{}' not found",
                        label
                    ))),
                    Some(_) if public_keys.len() == 1 => Err(ErrorCode::AuthenticateFailure(
                        "cannot remove the last public key, user must have at least one key for key-pair authentication".to_string(),
                    )),
                    Some(i) => {
                        public_keys.remove(i);
                        Ok(())
                    }
                }
            }
            _ => Err(ErrorCode::AuthenticateFailure(
                "user is not configured for key-pair authentication".to_string(),
            )),
        }
    }

    pub fn remove_public_key_by_fingerprint(&mut self, fingerprint: &str) -> Result<()> {
        match self {
            AuthInfo::KeyPair { public_keys } => {
                let idx = public_keys
                    .iter()
                    .position(|k| k.fingerprint().ok().as_deref() == Some(fingerprint));
                match idx {
                    None => Err(ErrorCode::AuthenticateFailure(format!(
                        "public key with fingerprint '{}' not found",
                        fingerprint
                    ))),
                    Some(_) if public_keys.len() == 1 => Err(ErrorCode::AuthenticateFailure(
                        "cannot remove the last public key, user must have at least one key for key-pair authentication".to_string(),
                    )),
                    Some(i) => {
                        public_keys.remove(i);
                        Ok(())
                    }
                }
            }
            _ => Err(ErrorCode::AuthenticateFailure(
                "user is not configured for key-pair authentication".to_string(),
            )),
        }
    }
}

#[derive(
    serde::Serialize,
    serde::Deserialize,
    Clone,
    Copy,
    Debug,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    num_derive::FromPrimitive,
    Default,
)]
pub enum PasswordHashMethod {
    DoubleSha1 = 1,
    #[default]
    Sha256 = 2,
}

impl PasswordHashMethod {
    pub fn hash(self, user_input: &[u8]) -> Vec<u8> {
        match self {
            PasswordHashMethod::DoubleSha1 => double_sha1(user_input).to_vec(),
            PasswordHashMethod::Sha256 => Sha256::digest(user_input).to_vec(),
        }
    }

    fn to_string(self, hash_value: &[u8]) -> String {
        hex::encode(hash_value)
    }
}
