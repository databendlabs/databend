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
use databend_common_management::errors::meta_service_error;
use databend_common_meta_app::principal::AuthInfo;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserPrivilegeSet;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;
use databend_meta_client::types::SeqV;

use crate::BUILTIN_ROLE_ACCOUNT_ADMIN;
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
            // Only the data is returned, so it may come from the meta read cache.
            let client = self.cached_user_api(tenant);
            Ok(client
                .get_user(&user)
                .await
                .map_err(meta_service_error)?
                .ok_or_else(|| {
                    ErrorCode::UnknownUser(format!("User {} does not exist.", user.display()))
                })?
                .data)
        }
    }

    /// Read the user and its seq from meta-service, bypassing the meta read cache: callers use
    /// the seq for a conditional write.
    pub async fn get_meta_user(
        &self,
        tenant: &Tenant,
        user: UserIdentity,
    ) -> Result<SeqV<UserInfo>> {
        let client = self.user_api(tenant);
        client
            .get_user(&user)
            .await
            .map_err(meta_service_error)?
            .ok_or_else(|| {
                ErrorCode::UnknownUser(format!("User {} does not exist.", user.display()))
            })
    }

    /// Whether [`Self::get_user`] may answer `user` from the meta read cache, which can lag a
    /// change made by another node. A configured user never comes from it.
    fn may_be_cached(&self, user: &UserIdentity) -> bool {
        self.meta_read_cache_enabled() && self.get_configured_user(&user.username).is_none()
    }

    /// Get one user and check that it may log in from `client_ip`.
    ///
    /// The user may come from the meta read cache. If that record may not log in, the one in
    /// meta-service decides: another node may have enabled the user or changed its network
    /// policy.
    #[async_backtrace::framed]
    pub async fn get_user_with_client_ip(
        &self,
        tenant: &Tenant,
        user: UserIdentity,
        client_ip: Option<&str>,
    ) -> Result<UserInfo> {
        let user_info = self.get_user(tenant, user.clone()).await?;
        let Err(rejected) = self
            .check_login_allowed(tenant, &user_info, client_ip)
            .await
        else {
            return Ok(user_info);
        };
        if !self.may_be_cached(&user) {
            return Err(rejected);
        }
        let fresh = self.get_meta_user(tenant, user).await?.data;
        if fresh == user_info {
            return Err(rejected);
        }
        self.check_login_allowed(tenant, &fresh, client_ip).await?;
        Ok(fresh)
    }

    /// Reject a disabled user, or a client outside the user's network policy.
    pub async fn check_login_allowed(
        &self,
        tenant: &Tenant,
        user_info: &UserInfo,
        client_ip: Option<&str>,
    ) -> Result<()> {
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
        Ok(())
    }

    /// Log `identity` in with a password: load the user, check that it may log in from
    /// `client_ip`, and check the password with `verify`, which tells whether it matches.
    /// `global_network_policy` applies unless the user is an account admin; empty means none.
    ///
    /// The loaded user may come from the meta read cache, which can lag a change made by another
    /// node. If `verify` rejects it, the user in meta-service goes through the same checks and
    /// `verify` again. The outcome is recorded for the password policy.
    ///
    /// A user with a password policy is always read from meta-service: its failed logins and
    /// its lockout are recorded there by every node, and a lagging record would let a locked
    /// user in and clear the lockout.
    ///
    /// Returns the user and what `verify` returned for it. An error before `verify` ran, such as
    /// a disabled user or a blocked client, is returned as the error.
    pub async fn login_with_password(
        &self,
        tenant: &Tenant,
        identity: UserIdentity,
        client_ip: Option<&str>,
        global_network_policy: &str,
        verify: impl Fn(&UserInfo) -> Result<bool>,
    ) -> Result<(UserInfo, Result<bool>)> {
        let mut user = self
            .get_user_with_client_ip(tenant, identity.clone(), client_ip)
            .await?;
        if user.option.password_policy().is_some() && self.may_be_cached(&identity) {
            let fresh = self.get_meta_user(tenant, identity.clone()).await?.data;
            if fresh != user {
                self.check_login_allowed(tenant, &fresh, client_ip).await?;
                user = fresh;
            }
        }
        self.check_password_login(
            tenant,
            &identity,
            &mut user,
            client_ip,
            global_network_policy,
        )
        .await?;
        let mut verified = verify(&user);
        if !matches!(verified, Ok(true)) {
            if let Some(mut fresh) = self
                .refresh_rejected_user(tenant, identity.clone(), &user, client_ip)
                .await?
            {
                self.check_password_login(
                    tenant,
                    &identity,
                    &mut fresh,
                    client_ip,
                    global_network_policy,
                )
                .await?;
                user = fresh;
                verified = verify(&user);
            }
        }
        self.update_user_login_result(
            tenant.clone(),
            identity,
            matches!(verified, Ok(true)),
            &user,
        )
        .await?;
        Ok((user, verified))
    }

    /// The checks of a password login after those of [`Self::check_login_allowed`]: the global
    /// network policy unless `user` is an account admin, and the password policy, which may
    /// require `user` to change the password.
    async fn check_password_login(
        &self,
        tenant: &Tenant,
        identity: &UserIdentity,
        user: &mut UserInfo,
        client_ip: Option<&str>,
        global_network_policy: &str,
    ) -> Result<()> {
        if !user.is_account_admin() && !global_network_policy.is_empty() {
            self.enforce_network_policy(tenant, global_network_policy, client_ip)
                .await?;
        }
        if self
            .check_login_password(tenant, identity.clone(), user)
            .await?
        {
            user.update_auth_need_change_password();
        }
        Ok(())
    }

    /// After the credentials of `rejected` failed: it may come from the meta read cache, which
    /// can lag a change made by another node. Return the record in meta-service if it has other
    /// credentials and may log in, so the caller checks the credentials against it.
    ///
    /// A configured user never comes from the cache, and its configured credentials take
    /// precedence over a meta user with the same name.
    pub async fn refresh_rejected_user(
        &self,
        tenant: &Tenant,
        user: UserIdentity,
        rejected: &UserInfo,
        client_ip: Option<&str>,
    ) -> Result<Option<UserInfo>> {
        if !self.may_be_cached(&user) {
            return Ok(None);
        }
        let Ok(fresh) = self.get_meta_user(tenant, user).await else {
            return Ok(None);
        };
        if fresh.data.auth_info == rejected.auth_info {
            return Ok(None);
        }
        // The login checks ran on the cached record; the change may also have disabled the user
        // or restricted its network.
        self.check_login_allowed(tenant, &fresh.data, client_ip)
            .await?;
        Ok(Some(fresh.data))
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

        let whitelist = &GlobalConfig::instance()
            .query
            .common
            .network_policy_whitelist;
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
        let seq_users_info = client
            .get_users()
            .await
            .map_err(|e| meta_service_error(e).add_message_back("(while get users)."))?;

        let res = seq_users_info.into_iter().map(|s| s.data).collect();
        Ok(res)
    }

    // Add a new user info.
    #[async_backtrace::framed]
    pub async fn create_user(
        &self,
        tenant: &Tenant,
        user_info: UserInfo,
        create_option: &CreateOption,
    ) -> Result<()> {
        self.validate_key_pair_keys(&user_info.name, &user_info.auth_info)?;
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
        let overriding = create_option.is_overriding();
        let res = client
            .create_user(user_info.clone(), overriding)
            .await
            .map_err(meta_service_error)?;

        match (res, create_option) {
            (Ok(_), _) => Ok(()),
            (Err(_), CreateOption::CreateIfNotExists) => Ok(()),
            (Err(_), _) => Err(ErrorCode::UserAlreadyExists(format!(
                "User {} already exists.",
                user_info.identity().display()
            ))),
        }
    }

    #[async_backtrace::framed]
    pub async fn grant_privileges_to_user(
        &self,
        tenant: &Tenant,
        user: UserIdentity,
        object: GrantObject,
        privileges: UserPrivilegeSet,
    ) -> Result<u64> {
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
                | GrantObject::Integration(_)
                | GrantObject::Connection(_)
                | GrantObject::Sequence(_)
                | GrantObject::Procedure(_)
                | GrantObject::MaskingPolicy(_)
                | GrantObject::RowAccessPolicy(_)
        ) {
            return Err(ErrorCode::IllegalUser(format!(
                "Cannot grant Warehouse|Connection|Sequence|Procedure|MaskingPolicy|RowAccessPolicy privileges to user `{}`",
                user.username
            )));
        }
        let client = self.user_api(tenant);
        Ok(client
            .update_user_with(&user, |ui: &mut UserInfo| {
                ui.update_user_time();
                ui.grants.grant_privileges(&object, privileges)
            })
            .await
            .map_err(|e| meta_service_error(e).add_message_back("(while set user privileges)"))??)
    }

    #[async_backtrace::framed]
    pub async fn revoke_privileges_from_user(
        &self,
        tenant: &Tenant,
        user: UserIdentity,
        object: GrantObject,
        privileges: UserPrivilegeSet,
    ) -> Result<u64> {
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
        Ok(client
            .update_user_with(&user, |ui: &mut UserInfo| {
                ui.update_user_time();
                ui.grants.revoke_privileges(&object, privileges)
            })
            .await
            .map_err(|e| {
                meta_service_error(e).add_message_back("(while revoke user privileges)")
            })??)
    }

    #[async_backtrace::framed]
    pub async fn grant_role_to_user(
        &self,
        tenant: &Tenant,
        user: UserIdentity,
        grant_role: String,
    ) -> Result<u64> {
        if self.get_configured_user(&user.username).is_some() {
            return Err(ErrorCode::UserAlreadyExists(format!(
                "Cannot grant role to built-in user `{}`",
                user.username
            )));
        }
        let client = self.user_api(tenant);
        Ok(client
            .update_user_with(&user, |ui: &mut UserInfo| {
                ui.update_user_time();
                ui.grants.grant_role(grant_role)
            })
            .await
            .map_err(|e| meta_service_error(e).add_message_back("(while grant role to user)"))??)
    }

    #[async_backtrace::framed]
    pub async fn revoke_role_from_user(
        &self,
        tenant: &Tenant,
        user: UserIdentity,
        revoke_role: String,
    ) -> Result<u64> {
        if self.get_configured_user(&user.username).is_some() {
            return Err(ErrorCode::UserAlreadyExists(format!(
                "Cannot revoke role from built-in user `{}`",
                user.username
            )));
        }
        let client = self.user_api(tenant);
        Ok(client
            .update_user_with(&user, |ui: &mut UserInfo| {
                ui.update_user_time();
                ui.grants.revoke_role(&revoke_role)
            })
            .await
            .map_err(|e| {
                meta_service_error(e).add_message_back("(while revoke role from user)")
            })??)
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
        let dropped = client.drop_user(&user).await.map_err(meta_service_error)?;

        match dropped {
            Ok(_) => Ok(()),
            Err(_) => {
                if if_exists {
                    Ok(())
                } else {
                    Err(ErrorCode::UnknownUser(format!(
                        "User {} does not exist.",
                        user.display()
                    )))
                }
            }
        }
    }

    #[async_backtrace::framed]
    pub async fn alter_user(&self, tenant: &Tenant, user_info: &UserInfo, seq: u64) -> Result<u64> {
        self.validate_key_pair_keys(&user_info.name, &user_info.auth_info)?;
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
        Ok(client
            .upsert_user_info(user_info, seq)
            .await
            .map_err(|e| meta_service_error(e).add_message_back("(while alter user)."))??)
    }

    /// Validate all public keys in a KeyPair auth info are parseable.
    /// Also rejects key-pair auth for the root user.
    /// This is the persistence-level guard: no invalid key can be stored.
    fn validate_key_pair_keys(&self, username: &str, auth_info: &AuthInfo) -> Result<()> {
        if let AuthInfo::KeyPair { public_keys } = auth_info {
            if username == "root" {
                return Err(ErrorCode::InvalidArgument(
                    "key-pair authentication is not supported for the root user",
                ));
            }
            for entry in public_keys {
                crate::validate_public_key_pem(&entry.key)
                    .map_err(|e| ErrorCode::InvalidArgument(e.message()))?;
            }
        }
        Ok(())
    }

    // Update an user's default role
    #[async_backtrace::framed]
    pub async fn update_user_default_role(
        &self,
        tenant: &Tenant,
        user: UserIdentity,
        default_role: Option<String>,
    ) -> Result<u64> {
        let seqv = self.get_meta_user(tenant, user.clone()).await?;
        let seq = seqv.seq;
        let mut user_info = seqv.data;
        user_info.option.set_default_role(default_role);
        user_info.update_user_time();

        let client = self.user_api(tenant);
        Ok(client
            .upsert_user_info(&user_info, seq)
            .await
            .map_err(|e| meta_service_error(e).add_message_back("(while alter user)."))??)
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
        client
            .update_user_with(&user, |ui: &mut UserInfo| {
                if authed {
                    ui.clear_login_fail_history()
                } else {
                    ui.update_login_fail_history()
                }
            })
            .await
            .map_err(|e| {
                meta_service_error(e).add_message_back("(while update user login result).")
            })?
            .map_err(|e| {
                ErrorCode::from(e).add_message_back("(while update user login result).")
            })?;
        Ok(())
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
        client
            .update_user_with(&user, |ui: &mut UserInfo| {
                ui.update_lockout_time(lockout_time);
            })
            .await
            .map_err(|e| {
                meta_service_error(e).add_message_back("(while update user lockout time).")
            })?
            .map_err(|e| {
                ErrorCode::from(e).add_message_back("(while update user lockout time).")
            })?;
        Ok(())
    }
}
