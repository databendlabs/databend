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

use databend_common_ast::ast::AlterPasswordAction;
use databend_common_exception::Result;
use databend_common_sql::plans::AlterPasswordPolicyPlan;
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
pub struct AlterPasswordPolicyInterpreter {
    ctx: Arc<QueryContext>,
    plan: AlterPasswordPolicyPlan,
}

impl AlterPasswordPolicyInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: AlterPasswordPolicyPlan) -> Result<Self> {
        Ok(AlterPasswordPolicyInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for AlterPasswordPolicyInterpreter {
    fn name(&self) -> &str {
        "AlterPasswordPolicyInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "alter_password_policy_execute");

        let plan = self.plan.clone();
        let tenant = self.ctx.get_tenant();
        let user_mgr = UserApiProvider::instance();

        match plan.action {
            AlterPasswordAction::SetOptions(set_options) => {
                user_mgr
                    .update_password_policy(
                        &tenant,
                        &plan.name,
                        set_options.min_length,
                        set_options.max_length,
                        set_options.min_upper_case_chars,
                        set_options.min_lower_case_chars,
                        set_options.min_numeric_chars,
                        set_options.min_special_chars,
                        set_options.min_age_days,
                        set_options.max_age_days,
                        set_options.max_retries,
                        set_options.lockout_time_mins,
                        set_options.history,
                        set_options.comment.clone(),
                        plan.if_exists,
                    )
                    .await?;
            }
            AlterPasswordAction::UnSetOptions(unset_options) => {
                // convert unset options to default values
                let min_length = if unset_options.min_length {
                    Some(DEFAULT_PASSWORD_MIN_LENGTH)
                } else {
                    None
                };
                let max_length = if unset_options.max_length {
                    Some(DEFAULT_PASSWORD_MAX_LENGTH)
                } else {
                    None
                };
                let min_upper_case_chars = if unset_options.min_upper_case_chars {
                    Some(DEFAULT_PASSWORD_MIN_CHARS)
                } else {
                    None
                };
                let min_lower_case_chars = if unset_options.min_lower_case_chars {
                    Some(DEFAULT_PASSWORD_MIN_CHARS)
                } else {
                    None
                };
                let min_numeric_chars = if unset_options.min_numeric_chars {
                    Some(DEFAULT_PASSWORD_MIN_CHARS)
                } else {
                    None
                };
                let min_special_chars = if unset_options.min_special_chars {
                    Some(DEFAULT_PASSWORD_MIN_SPECIAL_CHARS)
                } else {
                    None
                };
                let min_age_days = if unset_options.min_age_days {
                    Some(DEFAULT_PASSWORD_MIN_AGE_DAYS)
                } else {
                    None
                };
                let max_age_days = if unset_options.max_age_days {
                    Some(DEFAULT_PASSWORD_MAX_AGE_DAYS)
                } else {
                    None
                };
                let max_retries = if unset_options.max_retries {
                    Some(DEFAULT_PASSWORD_MAX_RETRIES)
                } else {
                    None
                };
                let lockout_time_mins = if unset_options.lockout_time_mins {
                    Some(DEFAULT_PASSWORD_LOCKOUT_TIME_MINS)
                } else {
                    None
                };
                let history = if unset_options.history {
                    Some(DEFAULT_PASSWORD_HISTORY)
                } else {
                    None
                };
                let comment = if unset_options.comment {
                    Some("".to_string())
                } else {
                    None
                };
                user_mgr
                    .update_password_policy(
                        &tenant,
                        &plan.name,
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
                        plan.if_exists,
                    )
                    .await?;
            }
        }

        Ok(PipelineBuildResult::create())
    }
}
