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

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::AuthInfo;
use common_meta_types::UserInfo;

pub use crate::configs::Config;
use crate::users::auth::jwt::JwtAuthenticator;
use crate::users::UserApiProvider;

pub struct AuthMgr {
    tenant: String,
    users: Arc<UserApiProvider>,
    jwt: Option<JwtAuthenticator>,
}

pub enum Credential {
    Jwt {
        token: String,
    },
    Password {
        name: String,
        password: Option<Vec<u8>>,
        hostname: Option<String>,
    },
}

impl AuthMgr {
    pub async fn create(cfg: Config, users: Arc<UserApiProvider>) -> Result<Self> {
        Ok(AuthMgr {
            users,
            tenant: cfg.query.tenant_id.clone(),
            jwt: JwtAuthenticator::try_create(cfg).await?,
        })
    }

    pub async fn no_auth(&self) -> Result<UserInfo> {
        self.users.get_user(&self.tenant, "root", "127.0.0.1").await
    }

    pub async fn auth(&self, credential: &Credential) -> Result<UserInfo> {
        match credential {
            Credential::Jwt { token: t } => {
                let user_name = match &self.jwt {
                    Some(j) => j.get_user(t.as_str())?,
                    None => return Err(ErrorCode::AuthenticateFailure("jwt auth not configured.")),
                };
                self.users.get_user(&self.tenant, &user_name, "%").await
            }
            Credential::Password {
                name: n,
                password: p,
                hostname: h,
            } => {
                let user = self
                    .users
                    .get_user_with_client_ip(
                        &self.tenant,
                        n,
                        h.as_ref().unwrap_or(&"%".to_string()),
                    )
                    .await?;
                match &user.auth_info {
                    AuthInfo::None => Ok(user),
                    AuthInfo::Password {
                        hash_value: h,
                        hash_method: t,
                    } => match p {
                        None => Err(ErrorCode::AuthenticateFailure("password required")),
                        Some(p) => {
                            if *h == t.hash(p) {
                                Ok(user)
                            } else {
                                Err(ErrorCode::AuthenticateFailure("wrong password"))
                            }
                        }
                    },
                    _ => Err(ErrorCode::AuthenticateFailure("wrong auth type")),
                }
            }
        }
    }
}
