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

// Logs from this module will show up as "[AUTH] ...".
databend_common_tracing::register_module_tag!("[AUTH]");

use std::sync::Arc;

use databend_common_base::base::BuildInfoRef;
use databend_common_base::base::GlobalInstance;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::AuthInfo;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;
use databend_common_users::JwtAuthenticator;
use databend_common_users::UserApiProvider;
use fastrace::func_name;
use log::info;
use serde::Serialize;

use crate::servers::http::v1::ClientSessionManager;
use crate::sessions::Session;

pub struct AuthMgr {
    jwt_auth: Option<JwtAuthenticator>,
}

#[derive(Debug, Serialize)]
pub enum CredentialType {
    DatabendToken,
    Jwt,
    Password,
    NoNeed,
}

#[derive(Clone)]
pub enum Credential {
    DatabendToken {
        token: String,
    },
    Jwt {
        token: String,
        client_ip: Option<String>,
    },
    Password {
        name: String,
        password: Option<Vec<u8>>,
        client_ip: Option<String>,
    },
    NoNeed,
}

impl Credential {
    pub fn type_name(&self) -> CredentialType {
        match self {
            Credential::DatabendToken { .. } => CredentialType::DatabendToken,
            Credential::Jwt { .. } => CredentialType::Jwt,
            Credential::Password { .. } => CredentialType::Password,
            Credential::NoNeed => CredentialType::NoNeed,
        }
    }

    pub fn user_name(&self) -> Option<String> {
        match self {
            Credential::DatabendToken { .. } => None,
            Credential::Jwt { .. } => None,
            Credential::Password { name, .. } => Some(name.clone()),
            Credential::NoNeed => None,
        }
    }
}

impl AuthMgr {
    pub fn init(cfg: &InnerConfig, version: BuildInfoRef) -> Result<()> {
        GlobalInstance::set(AuthMgr::create(cfg, version));
        Ok(())
    }

    pub fn instance() -> Arc<AuthMgr> {
        GlobalInstance::get()
    }

    fn create(cfg: &InnerConfig, version: BuildInfoRef) -> Arc<AuthMgr> {
        Arc::new(AuthMgr {
            jwt_auth: JwtAuthenticator::create(&cfg.query, version),
        })
    }

    #[async_backtrace::framed]
    pub async fn auth(
        &self,
        session: &mut Session,
        credential: &Credential,
        need_user_info: bool,
    ) -> Result<(String, Option<String>)> {
        let user_api = UserApiProvider::instance();
        let global_network_policy = session
            .get_settings()
            .get_network_policy()
            .unwrap_or_default();
        match credential {
            Credential::NoNeed => Ok(("".to_string(), None)),
            Credential::DatabendToken { token } => {
                let claim = ClientSessionManager::instance().verify_token(token).await?;
                let tenant = Tenant::new_or_err(claim.tenant.to_string(), func_name!())?;
                if need_user_info {
                    let identity = UserIdentity::new(claim.user.clone(), "%");
                    session.set_current_tenant(tenant.clone());
                    let user_info = user_api.get_user(&tenant, identity.clone()).await?;
                    session.set_authed_user(user_info, claim.auth_role).await?;
                }
                Ok((claim.user, Some(claim.session_id)))
            }
            Credential::Jwt {
                token: t,
                client_ip,
            } => {
                let jwt_auth = self.jwt_auth.as_ref().ok_or_else(|| {
                    ErrorCode::AuthenticateFailure(
                        "JWT authentication failed: JWT auth is not configured on this server",
                    )
                })?;
                let jwt = jwt_auth.parse_jwt_claims(t.as_str()).await?;
                let user_name = jwt.subject.ok_or_else(|| {
                    ErrorCode::AuthenticateFailure(
                        "JWT authentication failed: subject claim (user name) is missing in the token",
                    )
                })?;

                // setup tenant if the JWT claims contain extra.tenant_id
                if let Some(tenant) = jwt.custom.tenant_id {
                    let tenant = Tenant::new_or_err(tenant, func_name!())?;
                    session.set_current_tenant(tenant);
                };

                let tenant = session.get_current_tenant();
                let identity = UserIdentity::new(&user_name, "%");

                // create a new user for this identity if not exists
                let user = match user_api
                    .get_user_with_client_ip(&tenant, identity.clone(), client_ip.as_deref())
                    .await
                {
                    Ok(mut user_info) => {
                        if user_info.auth_info != AuthInfo::JWT {
                            return Err(ErrorCode::AuthenticateFailure(
                                "Authentication failed: user exists but is not configured for JWT authentication",
                            ));
                        }
                        if let Some(ensure_user) = jwt.custom.ensure_user {
                            let current_roles = user_info.grants.roles_vec();
                            // ensure jwt roles to user if not exists
                            if let Some(ref roles) = ensure_user.roles {
                                for role in roles.iter() {
                                    if !current_roles.contains(role) {
                                        info!(
                                            "JWT grant role to user: {} -> {}",
                                            user_info.name, role
                                        );
                                        user_api
                                            .grant_role_to_user(
                                                &tenant,
                                                user_info.identity(),
                                                role.clone(),
                                            )
                                            .await?;
                                        user_info.grants.grant_role(role.clone());
                                    }
                                }
                            }
                            if let Some(ref jwt_default_role) = ensure_user.default_role {
                                // grant default role to user if not exists
                                if !user_info.grants.roles().contains(jwt_default_role) {
                                    info!(
                                        "JWT grant default role to user: {} -> {}",
                                        user_info.name, jwt_default_role
                                    );
                                    user_api
                                        .grant_role_to_user(
                                            &tenant,
                                            user_info.identity(),
                                            jwt_default_role.clone(),
                                        )
                                        .await?;
                                    user_info.grants.grant_role(jwt_default_role.clone());
                                }
                                // ensure default role to jwt role
                                if user_info.option.default_role() != Some(jwt_default_role) {
                                    info!(
                                        "JWT update default role for user: {} -> {}",
                                        user_info.name, jwt_default_role
                                    );
                                    user_api
                                        .update_user_default_role(
                                            &tenant,
                                            user_info.identity(),
                                            Some(jwt_default_role.clone()),
                                        )
                                        .await?;
                                    user_info
                                        .option
                                        .set_default_role(Some(jwt_default_role.clone()));
                                }
                            }
                        }
                        user_info
                    }
                    Err(e) => {
                        match e.code() {
                            ErrorCode::UNKNOWN_USER => {}
                            ErrorCode::META_SERVICE_ERROR => {
                                return Err(e);
                            }
                            _ => {
                                return Err(ErrorCode::AuthenticateFailure(format!(
                                    "Authentication failed: {}",
                                    e.message()
                                )));
                            }
                        }
                        let ensure_user = jwt
                            .custom
                            .ensure_user
                            .ok_or_else(|| ErrorCode::AuthenticateFailure(format!("JWT authentication failed: ensure_user claim is missing and user does not exist: {}", e.message())))?;
                        // create a new user if not exists
                        let mut user_info = UserInfo::new(&user_name, "%", AuthInfo::JWT);
                        if let Some(ref roles) = ensure_user.roles {
                            for role in roles.clone().into_iter() {
                                user_info.grants.grant_role(role);
                            }
                        }
                        if let Some(ref default_role) = ensure_user.default_role {
                            if !user_info.grants.roles().contains(default_role) {
                                user_info.grants.grant_role(default_role.clone());
                            }
                            user_info
                                .option
                                .set_default_role(Some(default_role.clone()));
                        }
                        info!("JWT create user: {}", user_info.name);
                        user_api
                            .create_user(
                                &tenant,
                                user_info.clone(),
                                &CreateOption::CreateIfNotExists,
                            )
                            .await?;
                        user_info
                    }
                };

                // check global network policy if user is not account admin
                if !user.is_account_admin() && !global_network_policy.is_empty() {
                    user_api
                        .enforce_network_policy(
                            &tenant,
                            &global_network_policy,
                            client_ip.as_deref(),
                        )
                        .await?;
                }

                session.set_authed_user(user, jwt.custom.role).await?;
                Ok((user_name, None))
            }
            Credential::Password {
                name,
                password: p,
                client_ip,
            } => {
                let tenant = session.get_current_tenant();
                let identity = UserIdentity::new(name, "%");
                let mut user = user_api
                    .get_user_with_client_ip(&tenant, identity.clone(), client_ip.as_deref())
                    .await?;

                // check global network policy if user is not account admin
                if !user.is_account_admin() && !global_network_policy.is_empty() {
                    user_api
                        .enforce_network_policy(
                            &tenant,
                            &global_network_policy,
                            client_ip.as_deref(),
                        )
                        .await?;
                }

                // Check password policy for login
                let need_change = UserApiProvider::instance()
                    .check_login_password(&tenant, identity.clone(), &user)
                    .await?;
                if need_change {
                    user.update_auth_need_change_password();
                }

                let authed = match &user.auth_info {
                    AuthInfo::None => Ok(()),
                    AuthInfo::Password {
                        hash_value: h,
                        hash_method: t,
                        ..
                    } => match p {
                        None => Err(ErrorCode::AuthenticateFailure(
                            "Authentication failed: password is required but was not provided",
                        )),
                        Some(p) => {
                            if *h == t.hash(p) {
                                Ok(())
                            } else {
                                Err(ErrorCode::AuthenticateFailure(
                                    "Authentication failed: incorrect password",
                                ))
                            }
                        }
                    },
                    _ => Err(ErrorCode::AuthenticateFailure(
                        "Authentication failed: user exists but is not configured for password authentication",
                    )),
                };
                UserApiProvider::instance()
                    .update_user_login_result(tenant, identity, authed.is_ok(), &user)
                    .await?;

                authed?;

                session.set_authed_user(user, None).await?;
                Ok((name.to_string(), None))
            }
        }
    }
}
