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
            "NAME".as_bytes().to_vec(),
            "COMMENT".as_bytes().to_vec(),
            "PASSWORD_MIN_LENGTH".as_bytes().to_vec(),
            "PASSWORD_MAX_LENGTH".as_bytes().to_vec(),
            "PASSWORD_MIN_UPPER_CASE_CHARS".as_bytes().to_vec(),
            "PASSWORD_MIN_LOWER_CASE_CHARS".as_bytes().to_vec(),
            "PASSWORD_MIN_NUMERIC_CHARS".as_bytes().to_vec(),
            "PASSWORD_MIN_SPECIAL_CHARS".as_bytes().to_vec(),
            "PASSWORD_MIN_AGE_DAYS".as_bytes().to_vec(),
            "PASSWORD_MAX_AGE_DAYS".as_bytes().to_vec(),
            "PASSWORD_MAX_RETRIES".as_bytes().to_vec(),
            "PASSWORD_LOCKOUT_TIME_MINS".as_bytes().to_vec(),
            "PASSWORD_HISTORY".as_bytes().to_vec(),
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
            password_policy.name.as_bytes().to_vec(),
            password_policy.comment.as_bytes().to_vec(),
            min_length.as_bytes().to_vec(),
            max_length.as_bytes().to_vec(),
            min_upper_case_chars.as_bytes().to_vec(),
            min_lower_case_chars.as_bytes().to_vec(),
            min_numeric_chars.as_bytes().to_vec(),
            min_special_chars.as_bytes().to_vec(),
            min_age_days.as_bytes().to_vec(),
            max_age_days.as_bytes().to_vec(),
            max_retries.as_bytes().to_vec(),
            lockout_time_mins.as_bytes().to_vec(),
            history.as_bytes().to_vec(),
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
            "Name of password policy.".as_bytes().to_vec(),
            "Comment of password policy.".as_bytes().to_vec(),
            "Minimum length of new password.".as_bytes().to_vec(),
            "Maximum length of new password.".as_bytes().to_vec(),
            "Minimum number of uppercase characters in new password.".as_bytes().to_vec(),
            "Minimum number of lowercase characters in new password.".as_bytes().to_vec(),
            "Minimum number of numeric characters in new password.".as_bytes().to_vec(),
            "Minimum number of special characters in new password.".as_bytes().to_vec(),
            "Period after a password is changed during which a password cannot be changed again, in days.".as_bytes().to_vec(),
            "Period after which password must be changed, in days.".as_bytes().to_vec(),
            "Number of attempts users have to enter the correct password before their account is locked.".as_bytes().to_vec(),
            "Period of time for which users will be locked after entering their password incorrectly many times (specified by MAX_RETRIES), in minutes.".as_bytes().to_vec(),
            "Number of most recent passwords that may not be repeated by the user.".as_bytes().to_vec(),
        ];

        PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
            StringType::from_data(properties),
            StringType::from_data(values),
            UInt64Type::from_opt_data(defaults),
            StringType::from_data(descriptions),
        ])])
    }
}
