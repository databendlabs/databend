// Copyright 2021 Datafuse Labs.
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
use common_exception::ErrorCode;
use sha2::Digest;

const NO_PASSWORD_STR: &str = "no_password";
const PLAINTEXT_PASSWORD_STR: &str = "plaintext_password";
const SHA256_PASSWORD_STR: &str = "sha256_password";
const DOUBLE_SHA1_PASSWORD_STR: &str = "double_sha1_password";

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub enum AuthType {
    NoPassword,
    PlaintextPassword,
    Sha256Password,
    DoubleShaPassword,
}

use AuthType::*;

impl AuthType {
    pub fn parse_str(s: &str) -> Result<AuthType, String> {
        match s {
            PLAINTEXT_PASSWORD_STR => Ok(PlaintextPassword),
            SHA256_PASSWORD_STR => Ok(Sha256Password),
            DOUBLE_SHA1_PASSWORD_STR => Ok(DoubleShaPassword),
            NO_PASSWORD_STR => Ok(NoPassword),
            _ => Err(AuthType::bad_auth_types(s)),
        }
    }

    pub fn to_str(&self) -> &str {
        match self {
            NoPassword => NO_PASSWORD_STR,
            PlaintextPassword => PLAINTEXT_PASSWORD_STR,
            Sha256Password => SHA256_PASSWORD_STR,
            DoubleShaPassword => DOUBLE_SHA1_PASSWORD_STR,
        }
    }

    fn bad_auth_types(s: &str) -> String {
        let all = vec![
            NO_PASSWORD_STR,
            PLAINTEXT_PASSWORD_STR,
            SHA256_PASSWORD_STR,
            DOUBLE_SHA1_PASSWORD_STR,
        ];
        let all = all
            .iter()
            .map(|s| format!("'{}'", s))
            .collect::<Vec<_>>()
            .join("|");
        format!("Expected auth type {}, found: {}", all, s)
    }

    fn get_password_type(self) -> Option<PasswordType> {
        match self {
            PlaintextPassword => Some(PasswordType::PlainText),
            Sha256Password => Some(PasswordType::Sha256),
            DoubleShaPassword => Some(PasswordType::DoubleSha1),
            _ => None,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum AuthInfo {
    None,
    Password {
        password: Vec<u8>,
        password_type: PasswordType,
    },
}

impl AuthInfo {
    pub fn get_type(&self) -> AuthType {
        match self {
            AuthInfo::None => NoPassword,
            AuthInfo::Password {
                password: _,
                password_type: t,
            } => match t {
                PasswordType::PlainText => PlaintextPassword,
                PasswordType::Sha256 => Sha256Password,
                PasswordType::DoubleSha1 => DoubleShaPassword,
            },
        }
    }

    pub fn get_password(&self) -> Option<Vec<u8>> {
        match self {
            AuthInfo::Password {
                password: p,
                password_type: _,
            } => Some(p.to_vec()),
            _ => None,
        }
    }

    pub fn get_password_type(&self) -> Option<PasswordType> {
        match self {
            AuthInfo::Password {
                password: _,
                password_type: t,
            } => Some(*t),
            _ => None,
        }
    }

    pub fn check_password(
        password_type: &PasswordType,
        password_saved: &[u8],
        password_to_check: &[u8],
    ) -> Result<bool, ErrorCode> {
        match password_type {
            PasswordType::PlainText => Ok(password_saved == password_to_check),
            // MySQL already did x = sha1(x)
            // so we just check double sha1(x)
            PasswordType::DoubleSha1 => {
                let mut m = ::sha1::Sha1::new();
                m.update(password_to_check);

                let bs = m.digest().bytes();
                let mut m = sha1::Sha1::new();
                m.update(&bs[..]);

                Ok(password_saved == m.digest().bytes().to_vec())
            }
            PasswordType::Sha256 => {
                let result = ::sha2::Sha256::digest(password_saved);
                Ok(password_saved == result.to_vec())
            }
        }
    }
    pub fn auth(&self, password_to_check: &[u8]) -> Result<bool, ErrorCode> {
        match self {
            AuthInfo::None => Ok(true),
            AuthInfo::Password {
                password: p,
                password_type: t,
            } => AuthInfo::check_password(t, p, password_to_check),
        }
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize, Debug, PartialEq)]
pub struct AuthInfoRaw {
    pub arg_with: Option<AuthType>,
    pub arg_by: Option<String>,
}

impl AuthInfoRaw {
    pub fn create_auth_info(&self) -> Result<AuthInfo, String> {
        // 'by' without 'with' means default auth_type
        let default = Sha256Password;
        let auth_type = self.arg_with.clone().unwrap_or(default);
        match auth_type {
            NoPassword => Ok(AuthInfo::None),
            PlaintextPassword | Sha256Password | DoubleShaPassword => {
                match &self.arg_by {
                    Some(p) => Ok(AuthInfo::Password {
                        password: Vec::from(p.to_string()),
                        // safe unwrap
                        password_type: auth_type.get_password_type().unwrap(),
                    }),
                    None => Err("need password".to_string()),
                }
            }
        }
    }

    pub fn alter_auth_info(self, auth_info: &AuthInfo) -> Result<AuthInfo, String> {
        // 'by' without 'with' means old auth_type
        let old_auth_type = auth_info.clone().get_type();
        let new_auth_type = self.arg_with.clone().unwrap_or(old_auth_type);
        AuthInfoRaw {
            arg_with: Some(new_auth_type),
            arg_by: self.arg_by,
        }
        .create_auth_info()
    }
}

impl Default for AuthInfo {
    fn default() -> Self {
        AuthInfo::None
    }
}

#[derive(
    serde::Serialize, serde::Deserialize, Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd,
)]
pub enum PasswordType {
    PlainText = 0,
    DoubleSha1 = 1,
    Sha256 = 2,
}

impl Default for PasswordType {
    fn default() -> Self {
        PasswordType::Sha256
    }
}
