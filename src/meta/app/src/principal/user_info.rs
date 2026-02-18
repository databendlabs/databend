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

use core::fmt;
use std::convert::TryFrom;

use chrono::DateTime;
use chrono::Utc;
use databend_common_ast::ast::UserOptionItem;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use enumflags2::BitFlags;
use enumflags2::bitflags;
use serde::Deserialize;
use serde::Serialize;

use crate::principal::AuthInfo;
use crate::principal::BUILTIN_ROLE_ACCOUNT_ADMIN;
use crate::principal::UserGrantSet;
use crate::principal::UserIdentity;
use crate::principal::UserQuota;

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Default)]
#[serde(default)]
pub struct UserInfo {
    pub name: String,

    pub hostname: String,

    pub auth_info: AuthInfo,

    pub grants: UserGrantSet,

    pub quota: UserQuota,

    pub option: UserOption,

    // Recently changed history passwords,
    // used to detect whether the newly changed password
    // is repeated with the history passwords.
    pub history_auth_infos: Vec<AuthInfo>,

    // The time of the most recent failed login with wrong passwords,
    // used to detect whether the number of failed logins exceeds the limit,
    // if so, the login will be locked for a while.
    pub password_fails: Vec<DateTime<Utc>>,

    // The time of the last password change,
    // used to check if the minimum allowed time has been exceeded when changing the password,
    // and to check if the maximum time that must be changed has been exceeded when login.
    pub password_update_on: Option<DateTime<Utc>>,

    // Login lockout time, records the end time of login lockout due to multiple password fails.
    pub lockout_time: Option<DateTime<Utc>>,

    pub created_on: DateTime<Utc>,
    pub update_on: DateTime<Utc>,
}

impl UserInfo {
    pub fn new(name: &str, hostname: &str, auth_info: AuthInfo) -> Self {
        // Default is no privileges.
        let grants = UserGrantSet::default();
        let quota = UserQuota::no_limit();
        let option = UserOption::default();
        let now = Utc::now();

        UserInfo {
            name: name.to_string(),
            hostname: hostname.to_string(),
            auth_info,
            grants,
            quota,
            option,
            history_auth_infos: Vec::new(),
            password_fails: Vec::new(),
            password_update_on: None,
            lockout_time: None,
            created_on: now,
            update_on: now,
        }
    }

    pub fn new_no_auth(name: &str, hostname: &str) -> Self {
        UserInfo::new(name, hostname, AuthInfo::None)
    }

    pub fn identity(&self) -> UserIdentity {
        UserIdentity {
            username: self.name.clone(),
            hostname: self.hostname.clone(),
        }
    }

    pub fn is_account_admin(&self) -> bool {
        self.grants
            .roles()
            .contains(&BUILTIN_ROLE_ACCOUNT_ADMIN.to_string())
    }

    pub fn has_option_flag(&self, flag: UserOptionFlag) -> bool {
        self.option.has_option_flag(flag)
    }

    pub fn update_auth_option(&mut self, auth: Option<AuthInfo>, option: Option<UserOption>) {
        if let Some(auth_info) = auth {
            self.auth_info = auth_info;
        };
        if let Some(user_option) = option {
            self.option = user_option;
        };
    }

    pub fn update_auth_history(&mut self, auth: Option<AuthInfo>) {
        if let Some(auth_info) = auth {
            if matches!(auth_info, AuthInfo::Password { .. }) {
                // Update password change history
                self.history_auth_infos.push(auth_info);
                // Maximum 24 password records
                if self.history_auth_infos.len() > 24 {
                    self.history_auth_infos.remove(0);
                }
                self.password_update_on = Some(Utc::now());
            }
        }
    }

    pub fn update_auth_need_change_password(&mut self) {
        if let AuthInfo::Password {
            hash_value,
            hash_method,
            ..
        } = self.auth_info.clone()
        {
            self.auth_info = AuthInfo::Password {
                hash_value,
                hash_method,
                need_change: true,
            };
        }
    }

    pub fn update_user_time(&mut self) {
        self.update_on = Utc::now();
    }

    pub fn update_login_fail_history(&mut self) {
        self.password_fails.push(Utc::now());
        // Maximum 10 failed login password records
        if self.password_fails.len() > 10 {
            self.password_fails.remove(0);
        }
    }

    pub fn clear_login_fail_history(&mut self) {
        self.password_fails = Vec::new();
        self.lockout_time = None;
    }

    pub fn update_lockout_time(&mut self, lockout_time: DateTime<Utc>) {
        self.lockout_time = Some(lockout_time);
    }
}

impl TryFrom<Vec<u8>> for UserInfo {
    type Error = ErrorCode;

    fn try_from(value: Vec<u8>) -> Result<Self> {
        match serde_json::from_slice(&value) {
            Ok(user_info) => Ok(user_info),
            Err(serialize_error) => Err(ErrorCode::IllegalUserInfoFormat(format!(
                "Cannot deserialize user info from bytes. cause {}",
                serialize_error
            ))),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, Eq, PartialEq, Default)]
#[serde(default)]
pub struct UserOption {
    flags: BitFlags<UserOptionFlag>,
    default_role: Option<String>,
    default_warehouse: Option<String>,
    network_policy: Option<String>,
    password_policy: Option<String>,
    workload_group: Option<String>,
    disabled: Option<bool>,
    must_change_password: Option<bool>,
}

impl UserOption {
    pub fn new(flags: BitFlags<UserOptionFlag>) -> Self {
        Self {
            flags,
            default_role: None,
            default_warehouse: None,
            network_policy: None,
            password_policy: None,
            workload_group: None,
            disabled: None,
            must_change_password: None,
        }
    }

    pub fn empty() -> Self {
        Default::default()
    }

    pub fn with_flags(mut self, flags: BitFlags<UserOptionFlag>) -> Self {
        self.flags = flags;
        self
    }

    pub fn with_default_role(mut self, default_role: Option<String>) -> Self {
        self.default_role = default_role;
        self
    }

    pub fn with_default_warehouse(mut self, default_warehouse: Option<String>) -> Self {
        self.default_warehouse = default_warehouse;
        self
    }

    pub fn with_network_policy(mut self, network_policy: Option<String>) -> Self {
        self.network_policy = network_policy;
        self
    }

    pub fn with_password_policy(mut self, password_policy: Option<String>) -> Self {
        self.password_policy = password_policy;
        self
    }

    pub fn with_workload_group(mut self, workload_group: Option<String>) -> Self {
        self.workload_group = workload_group;
        self
    }

    pub fn with_disabled(mut self, disabled: Option<bool>) -> Self {
        self.disabled = disabled;
        self
    }

    pub fn with_must_change_password(mut self, must_change_password: Option<bool>) -> Self {
        self.must_change_password = must_change_password;
        self
    }

    pub fn with_set_flag(mut self, flag: UserOptionFlag) -> Self {
        self.flags.insert(flag);
        self
    }

    pub fn flags(&self) -> &BitFlags<UserOptionFlag> {
        &self.flags
    }

    pub fn default_role(&self) -> Option<&String> {
        self.default_role.as_ref()
    }

    pub fn default_warehouse(&self) -> Option<&String> {
        self.default_warehouse.as_ref()
    }

    pub fn network_policy(&self) -> Option<&String> {
        self.network_policy.as_ref()
    }

    pub fn password_policy(&self) -> Option<&String> {
        self.password_policy.as_ref()
    }

    // This is workload_group id
    pub fn workload_group(&self) -> Option<&String> {
        self.workload_group.as_ref()
    }

    pub fn disabled(&self) -> Option<&bool> {
        self.disabled.as_ref()
    }

    pub fn must_change_password(&self) -> Option<&bool> {
        self.must_change_password.as_ref()
    }

    pub fn set_default_role(&mut self, default_role: Option<String>) {
        self.default_role = default_role;
    }

    pub fn set_default_warehouse(&mut self, default_warehouse: Option<String>) {
        self.default_warehouse = default_warehouse;
    }

    pub fn set_network_policy(&mut self, network_policy: Option<String>) {
        self.network_policy = network_policy;
    }

    pub fn set_password_policy(&mut self, password_policy: Option<String>) {
        self.password_policy = password_policy;
    }

    pub fn set_disabled(&mut self, disabled: Option<bool>) {
        self.disabled = disabled;
    }

    pub fn set_must_change_password(&mut self, must_change_password: Option<bool>) {
        self.must_change_password = must_change_password;
    }

    pub fn set_all_flag(&mut self) {
        self.flags = BitFlags::all();
    }

    pub fn set_option_flag(&mut self, flag: UserOptionFlag) {
        self.flags.insert(flag);
    }

    pub fn switch_option_flag(&mut self, flag: UserOptionFlag, on: bool) {
        if on {
            self.flags.insert(flag);
        } else {
            self.flags.remove(flag);
        }
    }

    pub fn unset_option_flag(&mut self, flag: UserOptionFlag) {
        self.flags.remove(flag);
    }

    pub fn has_option_flag(&self, flag: UserOptionFlag) -> bool {
        self.flags.contains(flag)
    }

    pub fn apply(&mut self, alter: &UserOptionItem) {
        match alter {
            UserOptionItem::TenantSetting(enabled) => {
                if *enabled {
                    self.flags.insert(UserOptionFlag::TenantSetting);
                } else {
                    self.flags.remove(UserOptionFlag::TenantSetting);
                }
            }
            UserOptionItem::DefaultRole(v) => self.default_role = Some(v.clone()),
            UserOptionItem::DefaultWarehouse(v) => self.default_warehouse = Some(v.clone()),
            UserOptionItem::SetNetworkPolicy(v) => self.network_policy = Some(v.clone()),
            UserOptionItem::UnsetNetworkPolicy => self.network_policy = None,
            UserOptionItem::SetPasswordPolicy(v) => self.password_policy = Some(v.clone()),
            UserOptionItem::UnsetPasswordPolicy => self.password_policy = None,
            UserOptionItem::Disabled(v) => self.disabled = Some(*v),
            UserOptionItem::MustChangePassword(v) => self.must_change_password = Some(*v),
            UserOptionItem::SetWorkloadGroup(v) => self.workload_group = Some(v.clone()),
            UserOptionItem::UnsetWorkloadGroup => self.workload_group = None,
        }
    }
}

#[bitflags]
#[repr(u64)]
#[derive(Serialize, Deserialize, Clone, Copy, Debug, Eq, PartialEq, num_derive::FromPrimitive)]
pub enum UserOptionFlag {
    TenantSetting = 1 << 0,
}

impl std::fmt::Display for UserOptionFlag {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            UserOptionFlag::TenantSetting => write!(f, "TENANTSETTING"),
        }
    }
}

#[cfg(test)]
mod tests {
    use enumflags2::BitFlags;

    use crate::principal::AuthInfo;
    use crate::principal::UserInfo;
    use crate::principal::UserOption;

    #[test]
    fn test_user_update_auth_option() -> anyhow::Result<()> {
        let mut u = UserInfo::new("a", "b", AuthInfo::None);

        // None does not take effect
        {
            let mut u2 = u.clone();
            u2.update_auth_option(None, None);
            assert_eq!(u2, u);
        }

        // Some updates the corresponding fields
        {
            u.update_auth_option(Some(AuthInfo::JWT), Some(UserOption::new(BitFlags::all())));
            assert_eq!(AuthInfo::JWT, u.auth_info);
            assert_eq!(BitFlags::all(), u.option.flags);
        }

        Ok(())
    }

    #[test]
    fn test_user_option_default_warehouse() -> anyhow::Result<()> {
        use databend_common_ast::ast::UserOptionItem;

        // default is None
        let opt = UserOption::empty();
        assert_eq!(opt.default_warehouse(), None);

        // with_default_warehouse builder
        let opt = UserOption::empty().with_default_warehouse(Some("wh1".to_string()));
        assert_eq!(opt.default_warehouse(), Some(&"wh1".to_string()));

        // set_default_warehouse
        let mut opt = UserOption::empty();
        opt.set_default_warehouse(Some("wh2".to_string()));
        assert_eq!(opt.default_warehouse(), Some(&"wh2".to_string()));
        opt.set_default_warehouse(None);
        assert_eq!(opt.default_warehouse(), None);

        // apply DefaultWarehouse
        let mut opt = UserOption::empty();
        opt.apply(&UserOptionItem::DefaultWarehouse("wh3".to_string()));
        assert_eq!(opt.default_warehouse(), Some(&"wh3".to_string()));

        Ok(())
    }
}
