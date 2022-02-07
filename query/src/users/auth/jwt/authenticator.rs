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

use std::collections::HashMap;

use common_exception::ErrorCode;
use common_exception::Result;
use jwt_simple::algorithms::RS256PublicKey;
use jwt_simple::algorithms::RSAPublicKeyLike;

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

// to use user specified (in config) fields
type CustomClaims = HashMap<String, serde_json::Value>;

impl JwtAuthenticator {
    pub async fn try_create(cfg: Config) -> Result<Option<Self>> {
        if cfg.query.jwt_key_file.is_empty() {
            return Ok(None);
        }
        let key_store = jwk::JwkKeyStore::new(cfg.query.jwt_key_file).await?;
        Ok(Some(JwtAuthenticator { key_store }))
    }

    pub fn get_user(&self, token: &str) -> Result<String> {
        let pub_key = self.key_store.get_key(None)?;
        match &pub_key {
            PubKey::RSA256(pk) => match pk.verify_token::<CustomClaims>(token, None) {
                Ok(c) => match c.subject {
                    None => Err(ErrorCode::AuthenticateFailure(
                        "missing  field `subject` in jwt",
                    )),
                    Some(subject) => Ok(subject),
                },
                Err(err) => Err(ErrorCode::AuthenticateFailure(err.to_string())),
            },
        }
    }
}
