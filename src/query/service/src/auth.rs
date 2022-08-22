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
use common_users::CustomClaims;
use common_users::JwtAuthenticator;
use common_users::UserApiProvider;
use jwtk::Claims;

use crate::sessions::SessionRef;
pub use crate::Config;

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
    pub async fn create(cfg: Config) -> Result<Arc<AuthMgr>> {
        Ok(Arc::new(AuthMgr {
            jwt_auth: JwtAuthenticator::try_create(cfg.query.jwt_key_file).await?,
        }))
    }

    pub async fn auth(&self, session: SessionRef, credential: &Credential) -> Result<()> {
        let user_info = match credential {
            Credential::Jwt {
                token: t,
                hostname: h,
            } => {
                let jwt_auth = self
                    .jwt_auth
                    .as_ref()
                    .ok_or_else(|| ErrorCode::AuthenticateFailure("jwt auth not configured."))?;
                let parsed_jwt = jwt_auth.parse_jwt(t.as_str()).await?;
                let (tenant, user_name) = self
                    .process_jwt_claims(&session, parsed_jwt.claims())
                    .await?;
                UserApiProvider::instance()
                    .get_user_with_client_ip(
                        &tenant,
                        &user_name,
                        h.as_ref().unwrap_or(&"%".to_string()),
                    )
                    .await?
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
                }?
            }
        };
        session.set_current_user(user_info);
        Ok(())
    }

    async fn process_jwt_claims(
        &self,
        session: &SessionRef,
        claims: &Claims<CustomClaims>,
    ) -> Result<(String, String)> {
        // setup tenant if the JWT claims contain extra.tenant_id
        if let Some(ref tenant) = claims.extra.tenant_id {
            session.set_current_tenant(tenant.clone());
        }
        let tenant = session.get_current_tenant();

        // take `sub` field in the claims as user name
        let user_name = claims
            .sub
            .clone()
            .ok_or_else(|| ErrorCode::AuthenticateFailure("sub not found in claims"))?;

        // set user auth_role if claims contain extra.role
        if let Some(ref auth_role) = claims.extra.role {
            session.set_auth_role(auth_role.clone());
        }

        // create user if not exists when the JWT claims contains ensure_user
        if let Some(ref ensure_user) = claims.extra.ensure_user {
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
        Ok((tenant, user_name))
    }
}
