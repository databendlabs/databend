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

use core::net::Ipv4Addr;

use chrono::DateTime;
use chrono::Utc;
use cidr::Ipv4Cidr;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_management::UserApi;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserPrivilegeSet;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::SeqV;

use crate::role_mgr::BUILTIN_ROLE_ACCOUNT_ADMIN;
use crate::UserApiProvider;

impl UserApiProvider {
    // Get one user from by tenant.
    #[async_backtrace::framed]
    pub async fn get_user(&self, tenant: &Tenant, user: UserIdentity) -> Result<UserInfo> {
        if let Some(auth_info) = self.get_configured_user(&user.username) {
            let mut user_info = UserInfo::new(&user.username, "%", auth_info.clone());
            user_info.grants.grant_privileges(
                &GrantObject::Global,
                UserPrivilegeSet::available_privileges_on_global(),
            );
            // Grant admin role to all configured users.
            user_info
                .grants
                .grant_role(BUILTIN_ROLE_ACCOUNT_ADMIN.to_string());
            user_info
                .option
                .set_default_role(Some(BUILTIN_ROLE_ACCOUNT_ADMIN.to_string()));
            user_info.option.set_all_flag();
            Ok(user_info)
        } else {
            Ok(self.get_meta_user(tenant, user).await?.data)
        }
    }

    pub async fn get_meta_user(
        &self,
        tenant: &Tenant,
        user: UserIdentity,
    ) -> Result<SeqV<UserInfo>> {
        let client = self.user_api(tenant);
        let get_user = client.get_user(user, MatchSeq::GE(0)).await?;
        Ok(get_user)
    }

    // Get one user and check client ip if has network policy.
    #[async_backtrace::framed]
    pub async fn get_user_with_client_ip(
        &self,
        tenant: &Tenant,
        user: UserIdentity,
        client_ip: Option<&str>,
    ) -> Result<UserInfo> {
        let user_info = self.get_user(tenant, user).await?;

        if let Some(disabled) = user_info.option.disabled() {
            if *disabled {
                return Err(ErrorCode::AuthenticateFailure(format!(
                    "AuthenticateFailure: user {} is disabled. Not allowed to login",
                    user_info.name
                )));
            }
        }

        if let Some(name) = user_info.option.network_policy() {
            self.enforce_network_policy(tenant, name, client_ip).await?;
        }
        Ok(user_info)
    }

    pub async fn enforce_network_policy(
        &self,
        tenant: &Tenant,
        policy: &str,
        client_ip: Option<&str>,
    ) -> Result<()> {
        let ip_addr: Ipv4Addr = match client_ip {
            Some(client_ip) => client_ip.parse()?,
            None => {
                return Err(ErrorCode::AuthenticateFailure("Unknown client ip"));
            }
        };

        let whitelist = &GlobalConfig::instance().query.network_policy_whitelist;
        for whitelist_ip in whitelist {
            let cidr: Ipv4Cidr = whitelist_ip.parse()?;
            if cidr.contains(&ip_addr) {
                return Ok(());
            }
        }

        let network_policy = self.get_network_policy(tenant, policy).await?;
        for blocked_ip in network_policy.blocked_ip_list {
            let blocked_cidr: Ipv4Cidr = blocked_ip.parse()?;
            if blocked_cidr.contains(&ip_addr) {
                return Err(ErrorCode::AuthenticateFailure(format!(
                    "client ip `{}` is blocked",
                    ip_addr
                )));
            }
        }
        let mut allow = false;
        for allowed_ip in network_policy.allowed_ip_list {
            let allowed_cidr: Ipv4Cidr = allowed_ip.parse()?;
            if allowed_cidr.contains(&ip_addr) {
                allow = true;
                break;
            }
        }
        if !allow {
            return Err(ErrorCode::AuthenticateFailure(format!(
                "client ip `{}` is not allowed to login",
                ip_addr
            )));
        }

        Ok(())
    }

    // Get the tenant all users list.
    #[async_backtrace::framed]
    pub async fn get_users(&self, tenant: &Tenant) -> Result<Vec<UserInfo>> {
        let client = self.user_api(tenant);
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
        tenant: &Tenant,
        user_info: UserInfo,
        create_option: &CreateOption,
    ) -> Result<()> {
        if let Some(name) = user_info.option.network_policy() {
            if self.get_network_policy(tenant, name).await.is_err() {
                return Err(ErrorCode::UnknownNetworkPolicy(format!(
                    "network policy `{}` is not exist",
                    name
                )));
            }
        }
        if let Some(name) = user_info.option.password_policy() {
            if self.get_password_policy(tenant, name).await.is_err() {
                return Err(ErrorCode::UnknownPasswordPolicy(format!(
                    "password policy `{}` is not exist",
                    name
                )));
            }
        }
        if self.get_configured_user(&user_info.name).is_some() {
            return Err(ErrorCode::UserAlreadyExists(format!(
                "Same name with configured user `{}`",
                user_info.name
            )));
        }
        let client = self.user_api(tenant);
        client.add_user(user_info, create_option).await
    }

    #[async_backtrace::framed]
    pub async fn grant_privileges_to_user(
        &self,
        tenant: &Tenant,
        user: UserIdentity,
        object: GrantObject,
        privileges: UserPrivilegeSet,
    ) -> Result<Option<u64>> {
        if self.get_configured_user(&user.username).is_some() {
            return Err(ErrorCode::UserAlreadyExists(format!(
                "Cannot grant privileges to built-in user `{}`",
                user.username
            )));
        }

        // Because of https://github.com/databendlabs/databend/pull/18400
        // We can allow grant connection|seq to user in 2026.07
        if matches!(
            object,
            GrantObject::Warehouse(_)
                | GrantObject::Connection(_)
                | GrantObject::Sequence(_)
                | GrantObject::Procedure(_)
        ) {
            return Err(ErrorCode::IllegalUser(format!(
                "Cannot grant warehouse|connection|Sequence|Procedure privileges to user `{}`",
                user.username
            )));
        }
        let client = self.user_api(tenant);
        client
            .update_user_with(user, MatchSeq::GE(1), |ui: &mut UserInfo| {
                ui.update_user_time();
                ui.grants.grant_privileges(&object, privileges)
            })
            .await
            .map_err(|e| e.add_message_back("(while set user privileges)"))
    }

    #[async_backtrace::framed]
    pub async fn revoke_privileges_from_user(
        &self,
        tenant: &Tenant,
        user: UserIdentity,
        object: GrantObject,
        privileges: UserPrivilegeSet,
    ) -> Result<Option<u64>> {
        if self.get_configured_user(&user.username).is_some() {
            return Err(ErrorCode::UserAlreadyExists(format!(
                "Cannot revoke privileges from built-in user `{}`",
                user.username
            )));
        }
        if let GrantObject::Warehouse(_) = object {
            return Err(ErrorCode::IllegalUser(format!(
                "Cannot revoke warehouse privileges to user `{}`",
                user.username
            )));
        }
        let client = self.user_api(tenant);
        client
            .update_user_with(user, MatchSeq::GE(1), |ui: &mut UserInfo| {
                ui.update_user_time();
                ui.grants.revoke_privileges(&object, privileges)
            })
            .await
            .map_err(|e| e.add_message_back("(while revoke user privileges)"))
    }

    #[async_backtrace::framed]
    pub async fn grant_role_to_user(
        &self,
        tenant: &Tenant,
        user: UserIdentity,
        grant_role: String,
    ) -> Result<Option<u64>> {
        if self.get_configured_user(&user.username).is_some() {
            return Err(ErrorCode::UserAlreadyExists(format!(
                "Cannot grant role to built-in user `{}`",
                user.username
            )));
        }
        let client = self.user_api(tenant);
        client
            .update_user_with(user, MatchSeq::GE(1), |ui: &mut UserInfo| {
                ui.update_user_time();
                ui.grants.grant_role(grant_role)
            })
            .await
            .map_err(|e| e.add_message_back("(while grant role to user)"))
    }

    #[async_backtrace::framed]
    pub async fn revoke_role_from_user(
        &self,
        tenant: &Tenant,
        user: UserIdentity,
        revoke_role: String,
    ) -> Result<Option<u64>> {
        if self.get_configured_user(&user.username).is_some() {
            return Err(ErrorCode::UserAlreadyExists(format!(
                "Cannot revoke role from built-in user `{}`",
                user.username
            )));
        }
        let client = self.user_api(tenant);
        client
            .update_user_with(user, MatchSeq::GE(1), |ui: &mut UserInfo| {
                ui.update_user_time();
                ui.grants.revoke_role(&revoke_role)
            })
            .await
            .map_err(|e| e.add_message_back("(while revoke role from user)"))
    }

    // Drop a user by name and hostname.
    #[async_backtrace::framed]
    pub async fn drop_user(
        &self,
        tenant: &Tenant,
        user: UserIdentity,
        if_exists: bool,
    ) -> Result<()> {
        if self.get_configured_user(&user.username).is_some() {
            return Err(ErrorCode::UserAlreadyExists(format!(
                "Built-in user `{}` cannot be dropped",
                user.username
            )));
        }
        let client = self.user_api(tenant);
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

    #[async_backtrace::framed]
    pub async fn alter_user(
        &self,
        tenant: &Tenant,
        user_info: &UserInfo,
        seq: u64,
    ) -> Result<Option<u64>> {
        let user_option = &user_info.option;
        if let Some(name) = user_option.network_policy() {
            if self.get_network_policy(tenant, name).await.is_err() {
                return Err(ErrorCode::UnknownNetworkPolicy(format!(
                    "network policy `{}` is not exist",
                    name
                )));
            }
        }
        if let Some(name) = user_option.password_policy() {
            if self.get_password_policy(tenant, name).await.is_err() {
                return Err(ErrorCode::UnknownPasswordPolicy(format!(
                    "password policy `{}` is not exist",
                    name
                )));
            }
        }

        let client = self.user_api(tenant);
        let seq = client
            .upsert_user_info(user_info, MatchSeq::Exact(seq))
            .await;
        match seq {
            Ok(s) => Ok(Some(s)),
            Err(e) => Err(e.add_message_back("(while alter user).")),
        }
    }

    // Update an user's default role
    #[async_backtrace::framed]
    pub async fn update_user_default_role(
        &self,
        tenant: &Tenant,
        user: UserIdentity,
        default_role: Option<String>,
    ) -> Result<Option<u64>> {
        let user = self.get_meta_user(tenant, user.clone()).await?;
        let seq = user.seq;
        let mut user_info = user.data;
        user_info.option.set_default_role(default_role);
        user_info.update_user_time();

        let client = self.user_api(tenant);
        let seq = client
            .upsert_user_info(&user_info, MatchSeq::Exact(seq))
            .await;
        match seq {
            Ok(s) => Ok(Some(s)),
            Err(e) => Err(e.add_message_back("(while alter user).")),
        }
    }

    #[async_backtrace::framed]
    pub async fn update_user_login_result(
        &self,
        tenant: Tenant,
        user: UserIdentity,
        authed: bool,
        user_info: &UserInfo,
    ) -> Result<()> {
        if self.get_configured_user(&user.username).is_some()
            || user_info.option.password_policy().is_none()
        {
            return Ok(());
        }

        let client = self.user_api(&tenant);
        let update_user = client
            .update_user_with(user, MatchSeq::GE(1), |ui: &mut UserInfo| {
                if authed {
                    ui.clear_login_fail_history()
                } else {
                    ui.update_login_fail_history()
                }
            })
            .await;

        match update_user {
            Ok(_) => Ok(()),
            Err(e) => Err(e.add_message_back("(while update user login result).")),
        }
    }

    #[async_backtrace::framed]
    pub async fn update_user_lockout_time(
        &self,
        tenant: &Tenant,
        user: UserIdentity,
        lockout_time: DateTime<Utc>,
    ) -> Result<()> {
        if self.get_configured_user(&user.username).is_some() {
            return Ok(());
        }
        let client = self.user_api(tenant);
        let update_user = client
            .update_user_with(user, MatchSeq::GE(1), |ui: &mut UserInfo| {
                ui.update_lockout_time(lockout_time);
            })
            .await;

        match update_user {
            Ok(_) => Ok(()),
            Err(e) => Err(e.add_message_back("(while update user lockout time).")),
        }
    }
}
