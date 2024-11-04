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

use chrono::Utc;
use databend_common_exception::Result;
use databend_common_meta_app::principal::PasswordPolicy;
use databend_common_sql::plans::CreatePasswordPolicyPlan;
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
use log::debug;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

#[derive(Debug)]
pub struct CreatePasswordPolicyInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreatePasswordPolicyPlan,
}

impl CreatePasswordPolicyInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreatePasswordPolicyPlan) -> Result<Self> {
        Ok(CreatePasswordPolicyInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CreatePasswordPolicyInterpreter {
    fn name(&self) -> &str {
        "CreatePasswordPolicyInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "create_password_policy_execute");

        let plan = self.plan.clone();
        let tenant = self.ctx.get_tenant();
        let user_mgr = UserApiProvider::instance();

        let min_length = plan
            .set_options
            .min_length
            .unwrap_or(DEFAULT_PASSWORD_MIN_LENGTH);
        let max_length = plan
            .set_options
            .max_length
            .unwrap_or(DEFAULT_PASSWORD_MAX_LENGTH);
        let min_upper_case_chars = plan
            .set_options
            .min_upper_case_chars
            .unwrap_or(DEFAULT_PASSWORD_MIN_CHARS);
        let min_lower_case_chars = plan
            .set_options
            .min_lower_case_chars
            .unwrap_or(DEFAULT_PASSWORD_MIN_CHARS);
        let min_numeric_chars = plan
            .set_options
            .min_numeric_chars
            .unwrap_or(DEFAULT_PASSWORD_MIN_CHARS);
        let min_special_chars = plan
            .set_options
            .min_special_chars
            .unwrap_or(DEFAULT_PASSWORD_MIN_SPECIAL_CHARS);
        let min_age_days = plan
            .set_options
            .min_age_days
            .unwrap_or(DEFAULT_PASSWORD_MIN_AGE_DAYS);
        let max_age_days = plan
            .set_options
            .max_age_days
            .unwrap_or(DEFAULT_PASSWORD_MAX_AGE_DAYS);
        let max_retries = plan
            .set_options
            .max_retries
            .unwrap_or(DEFAULT_PASSWORD_MAX_RETRIES);
        let lockout_time_mins = plan
            .set_options
            .lockout_time_mins
            .unwrap_or(DEFAULT_PASSWORD_LOCKOUT_TIME_MINS);
        let history = plan.set_options.history.unwrap_or(DEFAULT_PASSWORD_HISTORY);

        let comment = plan.set_options.comment.clone().unwrap_or_default();

        let password_policy = PasswordPolicy {
            name: plan.name,
            min_length,
            max_length,
            min_upper_case_chars,
            min_lower_case_chars,
            min_numeric_chars,
            min_special_chars,
            min_age_days,
            max_age_days,
            max_retries,
            lockout_time_mins,
            history,
            comment,
            create_on: Utc::now(),
            update_on: None,
        };
        user_mgr
            .add_password_policy(&tenant, password_policy, &plan.create_option)
            .await?;

        Ok(PipelineBuildResult::create())
    }
}
