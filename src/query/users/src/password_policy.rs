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

use chrono::Utc;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_management::PasswordPolicyApi;
use databend_common_meta_app::principal::PasswordPolicy;
use databend_common_meta_types::MatchSeq;

use crate::UserApiProvider;

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
        tenant: &str,
        password_policy: PasswordPolicy,
        if_not_exists: bool,
    ) -> Result<u64> {
        check_password_policy(&password_policy)?;

        if if_not_exists
            && self
                .exists_password_policy(tenant, password_policy.name.as_str())
                .await?
        {
            return Ok(0);
        }

        let client = self.get_password_policy_api_client(tenant)?;
        let add_password_policy = client.add_password_policy(password_policy);
        match add_password_policy.await {
            Ok(res) => Ok(res),
            Err(e) => {
                if if_not_exists && e.code() == ErrorCode::PASSWORD_POLICY_ALREADY_EXISTS {
                    Ok(0)
                } else {
                    Err(e.add_message_back("(while add password policy)"))
                }
            }
        }
    }

    // Update password policy.
    #[async_backtrace::framed]
    #[allow(clippy::too_many_arguments)]
    pub async fn update_password_policy(
        &self,
        tenant: &str,
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
        let client = self.get_password_policy_api_client(tenant)?;
        let seq_password_policy = match client.get_password_policy(name, MatchSeq::GE(0)).await {
            Ok(seq_password_policy) => seq_password_policy,
            Err(e) => {
                if if_exists && e.code() == ErrorCode::UNKNOWN_PASSWORD_POLICY {
                    return Ok(None);
                } else {
                    return Err(e.add_message_back(" (while alter password policy)"));
                }
            }
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

        match client
            .update_password_policy(password_policy, MatchSeq::Exact(seq))
            .await
        {
            Ok(res) => Ok(Some(res)),
            Err(e) => Err(e.add_message_back(" (while alter password policy).")),
        }
    }

    // Drop a password policy by name.
    #[async_backtrace::framed]
    pub async fn drop_password_policy(
        &self,
        tenant: &str,
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

        let client = self.get_password_policy_api_client(tenant)?;
        match client.drop_password_policy(name, MatchSeq::GE(1)).await {
            Ok(res) => Ok(res),
            Err(e) => {
                if if_exists && e.code() == ErrorCode::UNKNOWN_PASSWORD_POLICY {
                    Ok(())
                } else {
                    Err(e.add_message_back(" (while drop password policy)"))
                }
            }
        }
    }

    // Check whether a password policy is exist.
    #[async_backtrace::framed]
    pub async fn exists_password_policy(&self, tenant: &str, name: &str) -> Result<bool> {
        match self.get_password_policy(tenant, name).await {
            Ok(_) => Ok(true),
            Err(e) => {
                if e.code() == ErrorCode::UNKNOWN_PASSWORD_POLICY {
                    Ok(false)
                } else {
                    Err(e)
                }
            }
        }
    }

    // Get a password_policy by tenant.
    #[async_backtrace::framed]
    pub async fn get_password_policy(&self, tenant: &str, name: &str) -> Result<PasswordPolicy> {
        let client = self.get_password_policy_api_client(tenant)?;
        let password_policy = client
            .get_password_policy(name, MatchSeq::GE(0))
            .await?
            .data;
        Ok(password_policy)
    }

    // Get all password policies by tenant.
    #[async_backtrace::framed]
    pub async fn get_password_policies(&self, tenant: &str) -> Result<Vec<PasswordPolicy>> {
        let client = self.get_password_policy_api_client(tenant)?;
        let password_policies = client
            .get_password_policies()
            .await
            .map_err(|e| e.add_message_back(" (while get password policies)."))?;
        Ok(password_policies)
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
