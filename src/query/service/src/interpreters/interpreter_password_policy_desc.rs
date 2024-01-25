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

use databend_common_exception::Result;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_sql::plans::DescPasswordPolicyPlan;
use databend_common_users::UserApiProvider;
use databend_common_users::DEFAULT_PASSWORD_HISTORY;
use databend_common_users::DEFAULT_PASSWORD_LOCKOUT_TIME_MINS;
use databend_common_users::DEFAULT_PASSWORD_MAX_AGE_DAYS;
use databend_common_users::DEFAULT_PASSWORD_MAX_LENGTH;
use databend_common_users::DEFAULT_PASSWORD_MAX_RETRIES;
use databend_common_users::DEFAULT_PASSWORD_MIN_AGE_DAYS;
use databend_common_users::DEFAULT_PASSWORD_MIN_CHARS;
use databend_common_users::DEFAULT_PASSWORD_MIN_LENGTH;
use databend_common_users::DEFAULT_PASSWORD_MIN_SPECIAL_CHARS;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

#[derive(Debug)]
pub struct DescPasswordPolicyInterpreter {
    ctx: Arc<QueryContext>,
    plan: DescPasswordPolicyPlan,
}

impl DescPasswordPolicyInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DescPasswordPolicyPlan) -> Result<Self> {
        Ok(DescPasswordPolicyInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DescPasswordPolicyInterpreter {
    fn name(&self) -> &str {
        "DescPasswordPolicyInterpreter"
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let tenant = self.ctx.get_tenant();
        let user_mgr = UserApiProvider::instance();

        let password_policy = user_mgr
            .get_password_policy(&tenant, self.plan.name.as_str())
            .await?;

        let properties = vec![
            "NAME".to_string(),
            "COMMENT".to_string(),
            "PASSWORD_MIN_LENGTH".to_string(),
            "PASSWORD_MAX_LENGTH".to_string(),
            "PASSWORD_MIN_UPPER_CASE_CHARS".to_string(),
            "PASSWORD_MIN_LOWER_CASE_CHARS".to_string(),
            "PASSWORD_MIN_NUMERIC_CHARS".to_string(),
            "PASSWORD_MIN_SPECIAL_CHARS".to_string(),
            "PASSWORD_MIN_AGE_DAYS".to_string(),
            "PASSWORD_MAX_AGE_DAYS".to_string(),
            "PASSWORD_MAX_RETRIES".to_string(),
            "PASSWORD_LOCKOUT_TIME_MINS".to_string(),
            "PASSWORD_HISTORY".to_string(),
        ];

        let min_length = format!("{}", password_policy.min_length);
        let max_length = format!("{}", password_policy.max_length);
        let min_upper_case_chars = format!("{}", password_policy.min_upper_case_chars);
        let min_lower_case_chars = format!("{}", password_policy.min_lower_case_chars);
        let min_numeric_chars = format!("{}", password_policy.min_numeric_chars);
        let min_special_chars = format!("{}", password_policy.min_special_chars);
        let min_age_days = format!("{}", password_policy.min_age_days);
        let max_age_days = format!("{}", password_policy.max_age_days);
        let max_retries = format!("{}", password_policy.max_retries);
        let lockout_time_mins = format!("{}", password_policy.lockout_time_mins);
        let history = format!("{}", password_policy.history);

        let values = vec![
            password_policy.name.clone(),
            password_policy.comment.clone(),
            min_length.clone(),
            max_length.clone(),
            min_upper_case_chars.clone(),
            min_lower_case_chars.clone(),
            min_numeric_chars.clone(),
            min_special_chars.clone(),
            min_age_days.clone(),
            max_age_days.clone(),
            max_retries.clone(),
            lockout_time_mins.clone(),
            history.clone(),
        ];

        let defaults = vec![
            None,
            None,
            Some(DEFAULT_PASSWORD_MIN_LENGTH),
            Some(DEFAULT_PASSWORD_MAX_LENGTH),
            Some(DEFAULT_PASSWORD_MIN_CHARS),
            Some(DEFAULT_PASSWORD_MIN_CHARS),
            Some(DEFAULT_PASSWORD_MIN_CHARS),
            Some(DEFAULT_PASSWORD_MIN_SPECIAL_CHARS),
            Some(DEFAULT_PASSWORD_MIN_AGE_DAYS),
            Some(DEFAULT_PASSWORD_MAX_AGE_DAYS),
            Some(DEFAULT_PASSWORD_MAX_RETRIES),
            Some(DEFAULT_PASSWORD_LOCKOUT_TIME_MINS),
            Some(DEFAULT_PASSWORD_HISTORY),
        ];

        let descriptions = vec![
            "Name of password policy.".to_string(),
            "Comment of password policy.".to_string(),
            "Minimum length of new password.".to_string(),
            "Maximum length of new password.".to_string(),
            "Minimum number of uppercase characters in new password.".to_string(),
            "Minimum number of lowercase characters in new password.".to_string(),
            "Minimum number of numeric characters in new password.".to_string(),
            "Minimum number of special characters in new password.".to_string(),
            "Period after a password is changed during which a password cannot be changed again, in days.".to_string(),
            "Period after which password must be changed, in days.".to_string(),
            "Number of attempts users have to enter the correct password before their account is locked.".to_string(),
            "Period of time for which users will be locked after entering their password incorrectly many times (specified by MAX_RETRIES), in minutes.".to_string(),
            "Number of most recent passwords that may not be repeated by the user.".to_string(),
        ];

        PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
            StringType::from_data(properties),
            StringType::from_data(values),
            UInt64Type::from_opt_data(defaults),
            StringType::from_data(descriptions),
        ])])
    }
}
