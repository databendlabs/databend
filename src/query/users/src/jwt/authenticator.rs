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

use databend_common_base::base::BuildInfoRef;
use databend_common_config::QueryConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use jwt_simple::algorithms::ECDSAP256PublicKeyLike;
use jwt_simple::algorithms::ES256PublicKey;
use jwt_simple::algorithms::RS256PublicKey;
use jwt_simple::algorithms::RSAPublicKeyLike;
use jwt_simple::prelude::JWTClaims;
use jwt_simple::token::Token;
use serde::Deserialize;
use serde::Serialize;

use super::jwk;

#[derive(Debug, Clone)]
pub enum PubKey {
    RSA256(Box<RS256PublicKey>),
    ES256(ES256PublicKey),
}

pub struct JwtAuthenticator {
    // Todo(youngsofun): verify settings, like issuer
    key_stores: Vec<jwk::JwkKeyStore>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct EnsureUser {
    pub roles: Option<Vec<String>>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct CustomClaims {
    pub tenant_id: Option<String>,
    pub role: Option<String>,
    pub ensure_user: Option<EnsureUser>,
}

impl CustomClaims {
    pub fn new() -> Self {
        CustomClaims {
            tenant_id: None,
            role: None,
            ensure_user: None,
        }
    }

    pub fn empty(&self) -> bool {
        self.role.is_none() && self.tenant_id.is_none() && self.ensure_user.is_none()
    }

    pub fn with_tenant_id(mut self, tenant_id: &str) -> Self {
        self.tenant_id = Some(tenant_id.to_string());
        self
    }

    pub fn with_ensure_user(mut self, ensure_user: EnsureUser) -> Self {
        self.ensure_user = Some(ensure_user);
        self
    }

    pub fn with_role(mut self, role: &str) -> Self {
        self.role = Some(role.to_string());
        self
    }
}

impl JwtAuthenticator {
    pub fn create(cfg: &QueryConfig, version: BuildInfoRef) -> Option<Self> {
        if cfg.jwt_key_file.is_empty() && cfg.jwt_key_files.is_empty() {
            return None;
        }
        let user_agent = format!(
            "Databend/{}/{}/{}",
            version.semantic,
            cfg.tenant_id.tenant_name(),
            cfg.cluster_id
        );

        // init a vec of key store
        let mut key_stores = vec![];
        if !cfg.jwt_key_file.is_empty() {
            key_stores.push(
                jwk::JwkKeyStore::new(cfg.jwt_key_file.clone(), version)
                    .with_user_agent(&user_agent)
                    .with_refresh_interval(cfg.jwks_refresh_interval)
                    .with_refresh_timeout(cfg.jwks_refresh_timeout),
            );
        }
        for u in &cfg.jwt_key_files {
            key_stores.push(
                jwk::JwkKeyStore::new(u.clone(), version)
                    .with_user_agent(&user_agent)
                    .with_refresh_interval(cfg.jwks_refresh_interval)
                    .with_refresh_timeout(cfg.jwks_refresh_timeout),
            );
        }
        Some(JwtAuthenticator { key_stores })
    }

    // parse jwt claims from single source, if custom claim is not matching on desired, claim parsed would be empty
    #[async_backtrace::framed]
    pub async fn parse_jwt_claims_from_store(
        &self,
        token: &str,
        key_store: &jwk::JwkKeyStore,
    ) -> Result<JWTClaims<CustomClaims>> {
        let metadata = Token::decode_metadata(token);
        let key_id = metadata.map_or(None, |e| e.key_id().map(|s| s.to_string()));
        let pub_key = match key_store.get_key(&key_id).await? {
            None => {
                return Err(ErrorCode::AuthenticateFailure(format!(
                    "key id {} not found in jwk store",
                    key_id.unwrap_or_default()
                )));
            }
            Some(pk) => pk,
        };
        let r = match &pub_key {
            PubKey::RSA256(pk) => pk.verify_token::<CustomClaims>(token, None),
            PubKey::ES256(pk) => pk.verify_token::<CustomClaims>(token, None),
        };
        let c = r.map_err(|err| ErrorCode::AuthenticateFailure(err.to_string()))?;
        match c.subject {
            None => Err(ErrorCode::AuthenticateFailure(
                "missing field `subject` in jwt",
            )),
            Some(_) => Ok(c),
        }
    }

    #[async_backtrace::framed]
    pub async fn parse_jwt_claims(&self, token: &str) -> Result<JWTClaims<CustomClaims>> {
        let mut combined_code = ErrorCode::AuthenticateFailure(
            "could not decode token from all available jwt key stores. ",
        );
        for store in &self.key_stores {
            let claim = self.parse_jwt_claims_from_store(token, store).await;
            match claim {
                Ok(e) => return Ok(e),
                Err(e) => {
                    combined_code = combined_code.add_message(format!(
                        "message: {} , source file: {}, ",
                        e,
                        store.url()
                    ));
                    continue;
                }
            }
        }
        Err(combined_code)
    }
}
