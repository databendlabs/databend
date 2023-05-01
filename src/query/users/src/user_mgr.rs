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

use common_exception::ErrorCode;
use common_exception::Result;
use common_management::UserApi;
use common_meta_app::principal::AuthInfo;
use common_meta_app::principal::GrantObject;
use common_meta_app::principal::UserIdentity;
use common_meta_app::principal::UserInfo;
use common_meta_app::principal::UserOption;
use common_meta_app::principal::UserPrivilegeSet;
use common_meta_types::MatchSeq;

use crate::role_mgr::BUILTIN_ROLE_ACCOUNT_ADMIN;
use crate::UserApiProvider;

impl UserApiProvider {
    // Get one user from by tenant.
    #[async_backtrace::framed]
    pub async fn get_user(&self, tenant: &str, user: UserIdentity) -> Result<UserInfo> {
        if user.is_root() {
            let mut user_info = UserInfo::new_no_auth(&user.username, &user.hostname);
            if user.is_localhost() {
                user_info.grants.grant_privileges(
                    &GrantObject::Global,
                    UserPrivilegeSet::available_privileges_on_global(),
                );
                user_info
                    .grants
                    .grant_role(BUILTIN_ROLE_ACCOUNT_ADMIN.to_string());
                user_info
                    .option
                    .set_default_role(Some(BUILTIN_ROLE_ACCOUNT_ADMIN.to_string()));
                user_info.option.set_all_flag();
            } else {
                return Err(ErrorCode::UnknownUser(format!(
                    "only accept root from localhost, current: '{}'@'{}'",
                    user.username, user.hostname
                )));
            }
            Ok(user_info)
        } else if let Some(auth_info) = self.get_configured_user(&user.username) {
            let mut user_info = UserInfo::new(&user.username, "%", auth_info.clone());
            user_info.grants.grant_privileges(
                &GrantObject::Global,
                UserPrivilegeSet::available_privileges_on_global(),
            );
            Ok(user_info)
        } else {
            let client = self.get_user_api_client(tenant)?;
            let get_user = client.get_user(user, MatchSeq::GE(0));
            Ok(get_user.await?.data)
        }
    }

    /// find the matched user with the client ip address, like 'u1'@'127.0.0.1', if the specific
    /// user@host is not found, try 'u1'@'%'.
    #[async_backtrace::framed]
    pub async fn get_user_with_client_ip(
        &self,
        tenant: &str,
        username: &str,
        client_ip: &str,
    ) -> Result<UserInfo> {
        let user = self
            .get_user(tenant, UserIdentity::new(username, client_ip))
            .await
            .map(Some)
            .or_else(|e| {
                if e.code() == ErrorCode::UNKNOWN_USER && !client_ip.eq("%") {
                    Ok(None)
                } else {
                    Err(e)
                }
            })?;
        match user {
            Some(user) => Ok(user),
            None => {
                self.get_user(tenant, UserIdentity::new(username, "%"))
                    .await
            }
        }
    }

    // Get the tenant all users list.
    #[async_backtrace::framed]
    pub async fn get_users(&self, tenant: &str) -> Result<Vec<UserInfo>> {
        let client = self.get_user_api_client(tenant)?;
        let get_users = client.get_users();

        let mut res = vec![];
        match get_users.await {
            Err(e) => Err(e.add_message_back("(while get users).")),
            Ok(seq_users_info) => {
                for seq_user_info in seq_users_info {
                    res.push(seq_user_info.data);
                }

                Ok(res)
            }
        }
    }

    // Add a new user info.
    #[async_backtrace::framed]
    pub async fn add_user(
        &self,
        tenant: &str,
        user_info: UserInfo,
        if_not_exists: bool,
    ) -> Result<u64> {
        if self.get_configured_user(&user_info.name).is_some() {
            return Err(ErrorCode::UserAlreadyExists(
                "same name with configured user",
            ));
        }
        let client = self.get_user_api_client(tenant)?;
        let add_user = client.add_user(user_info);
        match add_user.await {
            Ok(res) => Ok(res),
            Err(e) => {
                if if_not_exists && e.code() == ErrorCode::USER_ALREADY_EXISTS {
                    Ok(0)
                } else {
                    Err(e.add_message_back("(while add user)"))
                }
            }
        }
    }

    #[async_backtrace::framed]
    pub async fn grant_privileges_to_user(
        &self,
        tenant: &str,
        user: UserIdentity,
        object: GrantObject,
        privileges: UserPrivilegeSet,
    ) -> Result<Option<u64>> {
        let client = self.get_user_api_client(tenant)?;
        client
            .update_user_with(user, MatchSeq::GE(1), |ui: &mut UserInfo| {
                ui.grants.grant_privileges(&object, privileges)
            })
            .await
            .map_err(|e| e.add_message_back("(while set user privileges)"))
    }

    #[async_backtrace::framed]
    pub async fn revoke_privileges_from_user(
        &self,
        tenant: &str,
        user: UserIdentity,
        object: GrantObject,
        privileges: UserPrivilegeSet,
    ) -> Result<Option<u64>> {
        let client = self.get_user_api_client(tenant)?;
        client
            .update_user_with(user, MatchSeq::GE(1), |ui: &mut UserInfo| {
                ui.grants.revoke_privileges(&object, privileges)
            })
            .await
            .map_err(|e| e.add_message_back("(while revoke user privileges)"))
    }

    #[async_backtrace::framed]
    pub async fn grant_role_to_user(
        &self,
        tenant: &str,
        user: UserIdentity,
        grant_role: String,
    ) -> Result<Option<u64>> {
        let client = self.get_user_api_client(tenant)?;
        client
            .update_user_with(user, MatchSeq::GE(1), |ui: &mut UserInfo| {
                ui.grants.grant_role(grant_role)
            })
            .await
            .map_err(|e| e.add_message_back("(while grant role to user)"))
    }

    #[async_backtrace::framed]
    pub async fn revoke_role_from_user(
        &self,
        tenant: &str,
        user: UserIdentity,
        revoke_role: String,
    ) -> Result<Option<u64>> {
        let client = self.get_user_api_client(tenant)?;
        client
            .update_user_with(user, MatchSeq::GE(1), |ui: &mut UserInfo| {
                ui.grants.revoke_role(&revoke_role)
            })
            .await
            .map_err(|e| e.add_message_back("(while revoke role from user)"))
    }

    // Drop a user by name and hostname.
    #[async_backtrace::framed]
    pub async fn drop_user(&self, tenant: &str, user: UserIdentity, if_exists: bool) -> Result<()> {
        let client = self.get_user_api_client(tenant)?;
        let drop_user = client.drop_user(user, MatchSeq::GE(1));
        match drop_user.await {
            Ok(res) => Ok(res),
            Err(e) => {
                if if_exists && e.code() == ErrorCode::UNKNOWN_USER {
                    Ok(())
                } else {
                    Err(e.add_message_back("(while set drop user)"))
                }
            }
        }
    }

    // Update an user by name and hostname.
    #[async_backtrace::framed]
    pub async fn update_user(
        &self,
        tenant: &str,
        user: UserIdentity,
        auth_info: Option<AuthInfo>,
        user_option: Option<UserOption>,
    ) -> Result<Option<u64>> {
        let client = self.get_user_api_client(tenant)?;
        let update_user = client
            .update_user_with(user, MatchSeq::GE(1), |ui: &mut UserInfo| {
                ui.update_auth_option(auth_info, user_option)
            })
            .await;

        match update_user {
            Ok(res) => Ok(res),
            Err(e) => Err(e.add_message_back("(while alter user).")),
        }
    }

    // Update an user's default role
    #[async_backtrace::framed]
    pub async fn update_user_default_role(
        &self,
        tenant: &str,
        user_name: UserIdentity,
        default_role: Option<String>,
    ) -> Result<Option<u64>> {
        let mut user = self.get_user(tenant, user_name.clone()).await?;
        user.option.set_default_role(default_role);
        self.update_user(tenant, user_name, None, Some(user.option))
            .await
    }
}
