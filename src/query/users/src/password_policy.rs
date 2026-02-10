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

use core::cmp::Ordering;

use chrono::Duration;
use chrono::Utc;
use databend_common_ast::ast::AuthOption;
use databend_common_exception::ErrorCode;
use databend_common_exception::ErrorCodeResultExt;
use databend_common_exception::Result;
use databend_common_meta_api::crud::CrudError;
use databend_common_meta_app::principal::AuthInfo;
use databend_common_meta_app::principal::PasswordPolicy;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserOption;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;
use databend_meta_types::MatchSeq;
use log::warn;
use passwords::analyzer;

use crate::UserApiProvider;
use crate::meta_service_error;

// default value of password policy options
pub const DEFAULT_PASSWORD_MIN_LENGTH: u64 = 8;
pub const DEFAULT_PASSWORD_MAX_LENGTH: u64 = 256;
pub const DEFAULT_PASSWORD_MIN_CHARS: u64 = 1;
pub const DEFAULT_PASSWORD_MIN_SPECIAL_CHARS: u64 = 0;
pub const DEFAULT_PASSWORD_MIN_AGE_DAYS: u64 = 0;
pub const DEFAULT_PASSWORD_MAX_AGE_DAYS: u64 = 90;
pub const DEFAULT_PASSWORD_MAX_RETRIES: u64 = 5;
pub const DEFAULT_PASSWORD_LOCKOUT_TIME_MINS: u64 = 15;
pub const DEFAULT_PASSWORD_HISTORY: u64 = 0;

// minimal value of password policy options
pub const MIN_PASSWORD_LENGTH: u64 = 8;
pub const MIN_PASSWORD_CHARS: u64 = 0;
pub const MIN_PASSWORD_AGE_DAYS: u64 = 0;
pub const MIN_PASSWORD_MAX_RETRIES: u64 = 1;
pub const MIN_PASSWORD_LOCKOUT_TIME_MINS: u64 = 1;
pub const MIN_PASSWORD_HISTORY: u64 = 0;

// maximum value of password policy options
pub const MAX_PASSWORD_LENGTH: u64 = 256;
pub const MAX_PASSWORD_CHARS: u64 = 256;
pub const MAX_PASSWORD_AGE_DAYS: u64 = 999;
pub const MAX_PASSWORD_MAX_RETRIES: u64 = 10;
pub const MAX_PASSWORD_LOCKOUT_TIME_MINS: u64 = 999;
pub const MAX_PASSWORD_HISTORY: u64 = 24;

impl UserApiProvider {
    // Add a new password policy.
    #[async_backtrace::framed]
    pub async fn add_password_policy(
        &self,
        tenant: &Tenant,
        password_policy: PasswordPolicy,
        create_option: &CreateOption,
    ) -> Result<()> {
        check_password_policy(&password_policy)?;

        let client = self.password_policy_api(tenant);
        client.add(password_policy, create_option).await?;
        Ok(())
    }

    // Update password policy.
    #[async_backtrace::framed]
    #[allow(clippy::too_many_arguments)]
    pub async fn update_password_policy(
        &self,
        tenant: &Tenant,
        name: &str,
        min_length: Option<u64>,
        max_length: Option<u64>,
        min_upper_case_chars: Option<u64>,
        min_lower_case_chars: Option<u64>,
        min_numeric_chars: Option<u64>,
        min_special_chars: Option<u64>,
        min_age_days: Option<u64>,
        max_age_days: Option<u64>,
        max_retries: Option<u64>,
        lockout_time_mins: Option<u64>,
        history: Option<u64>,
        comment: Option<String>,
        if_exists: bool,
    ) -> Result<Option<u64>> {
        let client = self.password_policy_api(tenant);
        let seq_password_policy = match client.get(name, MatchSeq::GE(0)).await {
            Ok(seq_password_policy) => seq_password_policy,
            Err(e) => match e {
                CrudError::ApiError(meta_err) => {
                    return Err(meta_service_error(meta_err)
                        .add_message_back(" (while alter password policy)"));
                }
                CrudError::Business(unknown) => {
                    if if_exists {
                        return Ok(None);
                    } else {
                        return Err(ErrorCode::from(unknown)
                            .add_message_back(" (while alter password policy)"));
                    }
                }
            },
        };

        let seq = seq_password_policy.seq;
        let mut password_policy = seq_password_policy.data;
        if let Some(min_length) = min_length {
            password_policy.min_length = min_length;
        }
        if let Some(max_length) = max_length {
            password_policy.max_length = max_length;
        }
        if let Some(min_upper_case_chars) = min_upper_case_chars {
            password_policy.min_upper_case_chars = min_upper_case_chars;
        }
        if let Some(min_lower_case_chars) = min_lower_case_chars {
            password_policy.min_lower_case_chars = min_lower_case_chars;
        }
        if let Some(min_numeric_chars) = min_numeric_chars {
            password_policy.min_numeric_chars = min_numeric_chars;
        }
        if let Some(min_special_chars) = min_special_chars {
            password_policy.min_special_chars = min_special_chars;
        }
        if let Some(min_age_days) = min_age_days {
            password_policy.min_age_days = min_age_days;
        }
        if let Some(max_age_days) = max_age_days {
            password_policy.max_age_days = max_age_days;
        }
        if let Some(max_retries) = max_retries {
            password_policy.max_retries = max_retries;
        }
        if let Some(lockout_time_mins) = lockout_time_mins {
            password_policy.lockout_time_mins = lockout_time_mins;
        }
        if let Some(history) = history {
            password_policy.history = history;
        }
        if let Some(comment) = comment {
            password_policy.comment = comment;
        }
        check_password_policy(&password_policy)?;

        password_policy.update_on = Some(Utc::now());

        match client.update(password_policy, MatchSeq::Exact(seq)).await {
            Ok(res) => Ok(Some(res)),
            Err(e) => Err(ErrorCode::from(e).add_message_back(" (while alter password policy).")),
        }
    }

    // Drop a password policy by name.
    #[async_backtrace::framed]
    pub async fn drop_password_policy(
        &self,
        tenant: &Tenant,
        name: &str,
        if_exists: bool,
    ) -> Result<()> {
        let user_infos = self.get_users(tenant).await?;
        for user_info in user_infos {
            if let Some(network_policy) = user_info.option.password_policy() {
                if network_policy == name {
                    return Err(ErrorCode::PasswordPolicyIsUsedByUser(format!(
                        "password policy `{}` is used by user",
                        name,
                    )));
                }
            }
        }

        let client = self.password_policy_api(tenant);
        match client.remove(name, MatchSeq::GE(1)).await {
            Ok(res) => Ok(res),
            Err(e) => match e {
                CrudError::ApiError(meta_err) => {
                    return Err(meta_service_error(meta_err)
                        .add_message_back(" (while drop password policy)"));
                }
                CrudError::Business(unknown) => {
                    if if_exists {
                        return Ok(());
                    } else {
                        return Err(ErrorCode::from(unknown)
                            .add_message_back(" (while drop password policy)"));
                    }
                }
            },
        }
    }

    // Check whether a password policy is exist.
    #[async_backtrace::framed]
    pub async fn exists_password_policy(&self, tenant: &Tenant, name: &str) -> Result<bool> {
        Ok(self
            .get_password_policy(tenant, name)
            .await
            .or_unknown_password_policy()?
            .is_some())
    }

    // Get a password_policy by tenant.
    #[async_backtrace::framed]
    pub async fn get_password_policy(&self, tenant: &Tenant, name: &str) -> Result<PasswordPolicy> {
        let client = self.password_policy_api(tenant);
        let password_policy = client.get(name, MatchSeq::GE(0)).await?.data;
        Ok(password_policy)
    }

    // Get all password policies by tenant.
    #[async_backtrace::framed]
    pub async fn get_password_policies(&self, tenant: &Tenant) -> Result<Vec<PasswordPolicy>> {
        let client = self.password_policy_api(tenant);
        let password_policies = client.list(None).await.map_err(|e| {
            meta_service_error(e).add_message_back(" (while get password policies).")
        })?;
        Ok(password_policies)
    }

    // Verify the password according to the options of the password policy.
    // Both creating and changing password must meet the password complexity requirements.
    // And two additional conditions must meet for changing passwords:
    // 1. the current time must exceed the minimum number of days allowed for password changing.
    // 2. the password must not be repeated with recently history passwords.
    #[async_backtrace::framed]
    pub async fn verify_password(
        &self,
        tenant: &Tenant,
        user_option: &UserOption,
        auth_option: &AuthOption,
        user_info: Option<&UserInfo>,
        auth_info: Option<&AuthInfo>,
    ) -> Result<()> {
        if let (Some(name), Some(password)) = (user_option.password_policy(), &auth_option.password)
        {
            if let Ok(password_policy) = self.get_password_policy(tenant, name).await {
                // For password changes, check the number of days allowed and repeated with history passwords
                if let (Some(user_info), Some(auth_info)) = (user_info, auth_info) {
                    if password_policy.min_age_days > 0 {
                        if let Some(password_update_on) = user_info.password_update_on {
                            let allow_change_time = password_update_on
                                .checked_add_signed(Duration::days(
                                    password_policy.min_age_days as i64,
                                ))
                                .unwrap();

                            let now = Utc::now();
                            if let Ordering::Greater = allow_change_time.cmp(&now) {
                                return Err(ErrorCode::InvalidPassword(format!(
                                    "The time since the last change is too short, the password cannot be changed again before {}",
                                    allow_change_time
                                )));
                            }
                        }
                    }

                    if password_policy.history > 0 {
                        let auth_type = auth_info.get_type();
                        let password = auth_info.get_password();

                        for (i, history_auth_info) in
                            user_info.history_auth_infos.iter().rev().enumerate()
                        {
                            if i >= password_policy.history as usize {
                                break;
                            }

                            let history_auth_type = history_auth_info.get_type();
                            let history_password = history_auth_info.get_password();

                            // Using hash value of plain password to check, which may have false positives
                            if auth_type == history_auth_type && password == history_password {
                                return Err(ErrorCode::InvalidPassword(format!(
                                    "The newly changed password cannot be repeated with the last {} passwords.",
                                    password_policy.history
                                )));
                            }
                        }
                    }
                }

                // Verify the password complexity meets the requirements of the password policy
                let analyzed = analyzer::analyze(password);

                let mut invalids = Vec::new();
                if analyzed.length() < password_policy.min_length as usize
                    || analyzed.length() > password_policy.max_length as usize
                {
                    invalids.push(format!(
                        "expect length range {} to {}, but got {}",
                        password_policy.min_length,
                        password_policy.max_length,
                        analyzed.length()
                    ));
                }
                if analyzed.uppercase_letters_count()
                    < password_policy.min_upper_case_chars as usize
                {
                    invalids.push(format!(
                        "expect {} uppercase chars, but got {}",
                        password_policy.min_upper_case_chars,
                        analyzed.uppercase_letters_count()
                    ));
                }
                if analyzed.lowercase_letters_count()
                    < password_policy.min_lower_case_chars as usize
                {
                    invalids.push(format!(
                        "expect {} lowercase chars, but got {}",
                        password_policy.min_lower_case_chars,
                        analyzed.lowercase_letters_count()
                    ));
                }
                if analyzed.numbers_count() < password_policy.min_numeric_chars as usize {
                    invalids.push(format!(
                        "expect {} numeric chars, but got {}",
                        password_policy.min_numeric_chars,
                        analyzed.numbers_count()
                    ));
                }
                if analyzed.symbols_count() < password_policy.min_special_chars as usize {
                    invalids.push(format!(
                        "expect {} special chars, but got {}",
                        password_policy.min_special_chars,
                        analyzed.symbols_count()
                    ));
                }
                if !invalids.is_empty() {
                    return Err(ErrorCode::InvalidPassword(format!(
                        "Invalid password: {}",
                        invalids.join(", ")
                    )));
                }
            }
        }
        Ok(())
    }

    // Check login password meets the password policy options.
    // There are three conditions that need to be met in order to log in.
    // 1. Cannot be in a lockout period where logins are not allowed.
    // 2. the number of recent failed login attempts must not exceed the maximum retries,
    //    otherwise the user will be locked out.
    // 3. must be within the maximum allowed number of days since last password changed,
    //    otherwise the user can login but must change password first.
    #[async_backtrace::framed]
    pub async fn check_login_password(
        &self,
        tenant: &Tenant,
        identity: UserIdentity,
        user_info: &UserInfo,
    ) -> Result<bool> {
        let now = Utc::now();
        // Locked users cannot login for the duration of the lockout
        if let Some(lockout_time) = user_info.lockout_time {
            if let Ordering::Greater = lockout_time.cmp(&now) {
                warn!(
                    "user {} can not login until {} because too many password fails",
                    identity.display(),
                    lockout_time
                );
                return Err(ErrorCode::InvalidPassword(format!(
                    "Disable login before {} because of too many password fails",
                    lockout_time
                )));
            }
        }

        if let Some(name) = user_info.option.password_policy() {
            if let Ok(password_policy) = self.get_password_policy(tenant, name).await {
                // Check the number of login password fails
                if !user_info.password_fails.is_empty() && password_policy.max_retries > 0 {
                    let check_time = now
                        .checked_sub_signed(Duration::minutes(
                            password_policy.lockout_time_mins as i64,
                        ))
                        .unwrap();

                    // Only the most recent login fails are considered, outdated fails can be ignored
                    let failed_retries = user_info
                        .password_fails
                        .iter()
                        .filter(|t| t.cmp(&&check_time) == Ordering::Greater)
                        .count();

                    if failed_retries >= password_policy.max_retries as usize {
                        warn!(
                            "user {} can not login because password fails for {} time retries",
                            identity.display(),
                            failed_retries
                        );
                        let lockout_time = now
                            .checked_add_signed(Duration::minutes(
                                password_policy.lockout_time_mins as i64,
                            ))
                            .unwrap();
                        self.update_user_lockout_time(tenant, identity, lockout_time)
                            .await?;

                        return Err(ErrorCode::InvalidPassword(format!(
                            "Disable login before {} because of too many password fails",
                            lockout_time
                        )));
                    }
                }

                if password_policy.max_age_days > 0 {
                    if let Some(password_update_on) = user_info.password_update_on {
                        let max_change_time = password_update_on
                            .checked_add_signed(Duration::days(password_policy.max_age_days as i64))
                            .unwrap();

                        // Password has not been changed for more than max age days,
                        // allow login, but must change password first.
                        if let Ordering::Less = max_change_time.cmp(&now) {
                            warn!(
                                "user {} must change password first because password has expired in {}",
                                identity.display(),
                                max_change_time
                            );
                            return Ok(true);
                        }
                    }
                }
            }
        }
        Ok(false)
    }
}

// Check whether the values of options in the password policy are valid
fn check_password_policy(password_policy: &PasswordPolicy) -> Result<()> {
    if !(MIN_PASSWORD_LENGTH..=MAX_PASSWORD_LENGTH).contains(&password_policy.min_length) {
        return Err(ErrorCode::InvalidArgument(format!(
            "invalid password min length, supported range: {} to {}, but got {}",
            MIN_PASSWORD_LENGTH, MAX_PASSWORD_LENGTH, password_policy.min_length
        )));
    }

    if !(MIN_PASSWORD_LENGTH..=MAX_PASSWORD_LENGTH).contains(&password_policy.max_length) {
        return Err(ErrorCode::InvalidArgument(format!(
            "invalid password max length, supported range: {} to {}, but got {}",
            MIN_PASSWORD_LENGTH, MAX_PASSWORD_LENGTH, password_policy.max_length
        )));
    }

    // min length can't greater than max length
    if password_policy.min_length > password_policy.max_length {
        return Err(ErrorCode::InvalidArgument(format!(
            "invalid password length, min length must be less than max length, but got {} and {}",
            password_policy.min_length, password_policy.max_length
        )));
    }

    if !(MIN_PASSWORD_CHARS..=MAX_PASSWORD_CHARS).contains(&password_policy.min_upper_case_chars) {
        return Err(ErrorCode::InvalidArgument(format!(
            "invalid password min upper case chars, supported range: {} to {}, but got {}",
            MIN_PASSWORD_CHARS, MAX_PASSWORD_CHARS, password_policy.min_upper_case_chars
        )));
    }

    if !(MIN_PASSWORD_CHARS..=MAX_PASSWORD_CHARS).contains(&password_policy.min_lower_case_chars) {
        return Err(ErrorCode::InvalidArgument(format!(
            "invalid password min lower case chars, supported range: {} to {}, but got {}",
            MIN_PASSWORD_CHARS, MAX_PASSWORD_CHARS, password_policy.min_lower_case_chars
        )));
    }

    if !(MIN_PASSWORD_CHARS..=MAX_PASSWORD_CHARS).contains(&password_policy.min_numeric_chars) {
        return Err(ErrorCode::InvalidArgument(format!(
            "invalid password min numeric chars, supported range: {} to {}, but got {}",
            MIN_PASSWORD_CHARS, MAX_PASSWORD_CHARS, password_policy.min_numeric_chars
        )));
    }

    if !(MIN_PASSWORD_CHARS..=MAX_PASSWORD_CHARS).contains(&password_policy.min_special_chars) {
        return Err(ErrorCode::InvalidArgument(format!(
            "invalid password min special chars, supported range: {} to {}, but got {}",
            MIN_PASSWORD_CHARS, MAX_PASSWORD_CHARS, password_policy.min_special_chars
        )));
    }

    // sum min length of chars can't greater than max length
    let char_length = password_policy.min_upper_case_chars
        + password_policy.min_lower_case_chars
        + password_policy.min_numeric_chars
        + password_policy.min_special_chars;
    if char_length > password_policy.max_length {
        return Err(ErrorCode::InvalidArgument(format!(
            "invalid password length, sum of min chars length must be less than max length, but got 
            min upper case chars {}, min lower case chars {}, min numeric chars {}, min special chars {} 
            and max length {}",
            password_policy.min_upper_case_chars, password_policy.min_lower_case_chars,
            password_policy.min_numeric_chars, password_policy.min_special_chars, password_policy.max_length
        )));
    }

    if !(MIN_PASSWORD_AGE_DAYS..=MAX_PASSWORD_AGE_DAYS).contains(&password_policy.min_age_days) {
        return Err(ErrorCode::InvalidArgument(format!(
            "invalid password min age days, supported range: {} to {}, but got {}",
            MIN_PASSWORD_AGE_DAYS, MAX_PASSWORD_AGE_DAYS, password_policy.min_age_days
        )));
    }

    if !(MIN_PASSWORD_AGE_DAYS..=MAX_PASSWORD_AGE_DAYS).contains(&password_policy.max_age_days) {
        return Err(ErrorCode::InvalidArgument(format!(
            "invalid password max age days, supported range: {} to {}, but got {}",
            MIN_PASSWORD_AGE_DAYS, MAX_PASSWORD_AGE_DAYS, password_policy.max_age_days
        )));
    }

    if password_policy.min_age_days > password_policy.max_age_days {
        return Err(ErrorCode::InvalidArgument(format!(
            "invalid password age days, min age days must be less than max age days, but got {} and {}",
            password_policy.min_age_days, password_policy.max_age_days
        )));
    }

    if !(MIN_PASSWORD_MAX_RETRIES..=MAX_PASSWORD_MAX_RETRIES).contains(&password_policy.max_retries)
    {
        return Err(ErrorCode::InvalidArgument(format!(
            "invalid password max retries, supported range: {} to {}, but got {}",
            MIN_PASSWORD_MAX_RETRIES, MAX_PASSWORD_MAX_RETRIES, password_policy.max_retries
        )));
    }

    if !(MIN_PASSWORD_LOCKOUT_TIME_MINS..=MAX_PASSWORD_LOCKOUT_TIME_MINS)
        .contains(&password_policy.lockout_time_mins)
    {
        return Err(ErrorCode::InvalidArgument(format!(
            "invalid password lockout time mins, supported range: {} to {}, but got {}",
            MIN_PASSWORD_LOCKOUT_TIME_MINS,
            MAX_PASSWORD_LOCKOUT_TIME_MINS,
            password_policy.lockout_time_mins
        )));
    }

    if !(MIN_PASSWORD_HISTORY..=MAX_PASSWORD_HISTORY).contains(&password_policy.history) {
        return Err(ErrorCode::InvalidArgument(format!(
            "invalid password history, supported range: {} to {}, but got {}",
            MIN_PASSWORD_HISTORY, MAX_PASSWORD_HISTORY, password_policy.history
        )));
    }

    Ok(())
}
