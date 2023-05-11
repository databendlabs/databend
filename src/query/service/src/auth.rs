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

use std::sync::Arc;

use common_base::base::GlobalInstance;
use common_config::InnerConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::principal::AuthInfo;
use common_meta_app::principal::UserIdentity;
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
    },
    Password {
        name: String,
        password: Option<Vec<u8>>,
        hostname: Option<String>,
    },
}

impl AuthMgr {
    pub fn init(cfg: &InnerConfig) -> Result<()> {
        GlobalInstance::set(AuthMgr::create(cfg));
        Ok(())
    }

    pub fn instance() -> Arc<AuthMgr> {
        GlobalInstance::get()
    }

    fn create(cfg: &InnerConfig) -> Arc<AuthMgr> {
        Arc::new(AuthMgr {
            jwt_auth: JwtAuthenticator::create(
                cfg.query.jwt_key_file.clone(),
                cfg.query.jwt_key_files.clone(),
            ),
        })
    }

    #[async_backtrace::framed]
    pub async fn auth(&self, session: Arc<Session>, credential: &Credential) -> Result<()> {
        let user_api = UserApiProvider::instance();
        match credential {
            Credential::Jwt { token: t } => {
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
                let identity = UserIdentity::new(&user_name, "%");

                // create a new user for this identity if not exists
                let user = match user_api.get_user(&tenant, identity.clone()).await {
                    Ok(user_info) => match user_info.auth_info {
                        AuthInfo::JWT => user_info,
                        _ => return Err(ErrorCode::AuthenticateFailure("wrong auth type")),
                    },
                    Err(e) => {
                        if e.code() != ErrorCode::UNKNOWN_USER {
                            return Err(ErrorCode::AuthenticateFailure(e.message()));
                        }
                        let ensure_user = jwt
                            .custom
                            .ensure_user
                            .ok_or(ErrorCode::AuthenticateFailure(e.message()))?;
                        // create a new user if not exists
                        let mut user_info = UserInfo::new(&user_name, "%", AuthInfo::JWT);
                        if let Some(ref roles) = ensure_user.roles {
                            for role in roles.clone().into_iter() {
                                user_info.grants.grant_role(role);
                            }
                        }
                        user_api.add_user(&tenant, user_info.clone(), true).await?;
                        user_info
                    }
                };

                session.set_authed_user(user, jwt.custom.role).await?;
            }
            Credential::Password {
                name: n,
                password: p,
                hostname: h,
            } => {
                let tenant = session.get_current_tenant();
                let user = user_api
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
