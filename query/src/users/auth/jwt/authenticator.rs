// Copyright 2022 Datafuse Labs.
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
use common_exception::Result;
use jwt_simple::algorithms::RS256PublicKey;
use jwt_simple::algorithms::RSAPublicKeyLike;
use jwt_simple::prelude::JWTClaims;
use serde::Deserialize;
use serde::Serialize;

use crate::configs::Config;
use crate::users::auth::jwt::jwk;

#[derive(Debug, Clone)]
pub enum PubKey {
    RSA256(RS256PublicKey),
}

pub struct JwtAuthenticator {
    //Todo(youngsofun): verify settings, like issuer
    key_store: jwk::JwkKeyStore,
}

#[derive(Default, Deserialize, Serialize)]
pub struct EnsureUser {
    pub roles: Vec<String>,
}

#[derive(Default, Deserialize, Serialize)]
pub struct CustomClaims {
    pub tenant_id: Option<String>,
    pub ensure_user: Option<EnsureUser>,
}

impl CustomClaims {
    pub fn new() -> Self {
        CustomClaims {
            tenant_id: None,
            ensure_user: None,
        }
    }

    pub fn with_tenant_id(mut self, tenant_id: &str) -> Self {
        self.tenant_id = Some(tenant_id.to_string());
        self
    }

    pub fn with_ensure_user(mut self, ensure_user: EnsureUser) -> Self {
        self.ensure_user = Some(ensure_user);
        self
    }
}

impl JwtAuthenticator {
    pub async fn try_create(cfg: Config) -> Result<Option<Self>> {
        if cfg.query.jwt_key_file.is_empty() {
            return Ok(None);
        }
        let key_store = jwk::JwkKeyStore::new(cfg.query.jwt_key_file).await?;
        Ok(Some(JwtAuthenticator { key_store }))
    }

    pub fn parse_jwt_claims(&self, token: &str) -> Result<JWTClaims<CustomClaims>> {
        let pub_key = self.key_store.get_key(None)?;
        match &pub_key {
            PubKey::RSA256(pk) => match pk.verify_token::<CustomClaims>(token, None) {
                Ok(c) => match c.subject {
                    None => Err(ErrorCode::AuthenticateFailure(
                        "missing field `subject` in jwt",
                    )),
                    Some(_) => Ok(c),
                },
                Err(err) => Err(ErrorCode::AuthenticateFailure(err.to_string())),
            },
        }
    }
}
