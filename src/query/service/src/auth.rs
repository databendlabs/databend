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

pub use common_config::InnerConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::principal::AuthInfo;
use common_meta_app::principal::UserInfo;
use common_users::JwtAuthenticator;
use common_users::UserApiProvider;

use crate::sessions::Session;

pub struct AuthMgr {
    jwt_auth: Option<JwtAuthenticator>,
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
    pub fn create(cfg: &InnerConfig) -> Arc<AuthMgr> {
        Arc::new(AuthMgr {
            jwt_auth: JwtAuthenticator::create(
                cfg.query.jwt_key_file.clone(),
                cfg.query.jwt_key_files.clone(),
            ),
        })
    }

    pub async fn auth(&self, session: Arc<Session>, credential: &Credential) -> Result<()> {
        match credential {
            Credential::Jwt {
                token: t,
                hostname: h,
            } => {
                let jwt_auth = self
                    .jwt_auth
                    .as_ref()
                    .ok_or_else(|| ErrorCode::AuthenticateFailure("jwt auth not configured."))?;
                let jwt = jwt_auth.parse_jwt_claims(t.as_str()).await?;
                let user_name = jwt.subject.ok_or_else(|| {
                    ErrorCode::AuthenticateFailure(
                        "jwt auth not configured correctly, user name is missing.",
                    )
                })?;

                // setup tenant if the JWT claims contain extra.tenant_id
                if let Some(tenant) = jwt.custom.tenant_id {
                    session.set_current_tenant(tenant);
                };
                let tenant = session.get_current_tenant();

                // create user if not exists when the JWT claims contains ensure_user
                if let Some(ref ensure_user) = jwt.custom.ensure_user {
                    let mut user_info = UserInfo::new(&user_name, "%", AuthInfo::JWT);
                    if let Some(ref roles) = ensure_user.roles {
                        for role in roles.clone().into_iter() {
                            user_info.grants.grant_role(role);
                        }
                    }
                    UserApiProvider::instance()
                        .ensure_builtin_roles(&tenant)
                        .await?;
                    UserApiProvider::instance()
                        .add_user(&tenant, user_info.clone(), true)
                        .await?;
                }

                let auth_role = jwt.custom.role.clone();
                let user_info = UserApiProvider::instance()
                    .get_user_with_client_ip(
                        &tenant,
                        &user_name,
                        h.as_ref().unwrap_or(&"%".to_string()),
                    )
                    .await?;
                session.set_authed_user(user_info, auth_role).await?;
            }
            Credential::Password {
                name: n,
                password: p,
                hostname: h,
            } => {
                let tenant = session.get_current_tenant();
                let user = UserApiProvider::instance()
                    .get_user_with_client_ip(&tenant, n, h.as_ref().unwrap_or(&"%".to_string()))
                    .await?;
                let user = match &user.auth_info {
                    AuthInfo::None => user,
                    AuthInfo::Password {
                        hash_value: h,
                        hash_method: t,
                    } => match p {
                        None => return Err(ErrorCode::AuthenticateFailure("password required")),
                        Some(p) => {
                            if *h == t.hash(p) {
                                user
                            } else {
                                return Err(ErrorCode::AuthenticateFailure("wrong password"));
                            }
                        }
                    },
                    _ => return Err(ErrorCode::AuthenticateFailure("wrong auth type")),
                };
                session.set_authed_user(user, None).await?;
            }
        };
        Ok(())
    }
}
