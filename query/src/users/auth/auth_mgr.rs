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

use crate::users::auth::jwt::JwtAuthenticator;
use crate::users::UserApiProvider;
pub use crate::Config;

pub struct AuthMgr {
    tenant: String,
    user_mgr: Arc<UserApiProvider>,
    jwt: Option<JwtAuthenticator>,
}

pub enum Credential {
    Jwt {
        token: String,
        hostname: Option<String>,
    },
    Password {
        name: String,
        password: Option<Vec<u8>>,
        hostname: Option<String>,
    },
}

impl AuthMgr {
    pub async fn create(cfg: Config, user_mgr: Arc<UserApiProvider>) -> Result<Self> {
        Ok(AuthMgr {
            user_mgr,
            tenant: cfg.query.tenant_id.clone(),
            jwt: JwtAuthenticator::try_create(cfg).await?,
        })
    }

    pub async fn auth(&self, credential: &Credential) -> Result<(Option<String>, UserInfo)> {
        match credential {
            Credential::Jwt {
                token: t,
                hostname: h,
            } => {
                let jwt = match &self.jwt {
                    Some(j) => j.parse_jwt(t.as_str()).await?,
                    None => return Err(ErrorCode::AuthenticateFailure("jwt auth not configured.")),
                };
                let claims = jwt.claims();
                let user_name = claims.sub.as_ref().unwrap();
                let tenant = claims
                    .extra
                    .tenant_id
                    .clone()
                    .unwrap_or_else(|| self.tenant.clone());
                if let Some(ref ensure_user) = claims.extra.ensure_user {
                    let mut user_info = UserInfo::new(user_name, "%", AuthInfo::JWT);
                    if let Some(ref roles) = ensure_user.roles {
                        for role in roles.clone().into_iter() {
                            user_info.grants.grant_role(role);
                        }
                    }
                    self.user_mgr
                        .add_user(&tenant, user_info.clone(), true)
                        .await?;
                }
                let user = self
                    .user_mgr
                    .get_user_with_client_ip(
                        &tenant,
                        user_name,
                        h.as_ref().unwrap_or(&"%".to_string()),
                    )
                    .await?;
                Ok((Some(tenant.clone()), user))
            }
            Credential::Password {
                name: n,
                password: p,
                hostname: h,
            } => {
                let user = self
                    .user_mgr
                    .get_user_with_client_ip(
                        &self.tenant,
                        n,
                        h.as_ref().unwrap_or(&"%".to_string()),
                    )
                    .await?;
                let user_info = match &user.auth_info {
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
                }?;
                Ok((None, user_info))
            }
        }
    }
}
