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

use std::time::Duration;

use common_exception::ErrorCode;
use common_exception::Result;
use jwtk::jwk::RemoteJwksVerifier;
use jwtk::HeaderAndClaims;
use serde::Deserialize;
use serde::Serialize;

use crate::configs::Config;

pub struct JwtAuthenticator {
    //Todo(youngsofun): verify settings, like issuer
    verifier: RemoteJwksVerifier,
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
        let mut verifier =
            RemoteJwksVerifier::new(cfg.query.jwt_key_file, None, Duration::from_secs(15 * 60));
        verifier.set_require_kid(false);
        Ok(Some(JwtAuthenticator { verifier }))
    }

    pub async fn parse_jwt(&self, token: &str) -> Result<HeaderAndClaims<CustomClaims>> {
        match self.verifier.verify::<CustomClaims>(token).await {
            Ok(c) => match c.claims().sub {
                None => Err(ErrorCode::AuthenticateFailure(
                    "missing field `subject` in jwt",
                )),
                Some(_) => Ok(c),
            },
            Err(e) => Err(ErrorCode::AuthenticateFailure(e.to_string())),
        }
    }
}
