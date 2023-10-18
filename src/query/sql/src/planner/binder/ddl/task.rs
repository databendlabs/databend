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

use std::str::FromStr;

use chrono_tz;
use common_ast::ast::CreateTaskStmt;
use common_exception::ErrorCode;
use common_exception::Result;
use cron;

use crate::plans::CreateTaskPlan;
use crate::plans::Plan;
use crate::Binder;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_task(
        &mut self,
        stmt: &CreateTaskStmt,
    ) -> Result<Plan> {
        let CreateTaskStmt {
            if_not_exists,
            name,
            warehouse_opts,
            schedule_opts,
            suspend_task_after_num_failures,
            comments,
            sql,
        } = stmt;

        if let common_ast::ast::ScheduleOptions::CronExpression(cron_expr, time_zone) =
            schedule_opts
        {
            if cron::Schedule::from_str(cron_expr).is_err() {
                return Err(ErrorCode::SemanticError(format!(
                    "invalid cron expression {}",
                    cron_expr
                )));
            }
            if let Some(time_zone) = time_zone &&  !time_zone.is_empty() && chrono_tz::Tz::from_str(time_zone).is_err() {
                    return Err(ErrorCode::SemanticError(format!(
                        "invalid time zone {}",
                        time_zone
                    )));
                }
        }

        let tenant = self.ctx.get_tenant();
        let plan = CreateTaskPlan {
            if_not_exists: *if_not_exists,
            tenant,
            task_name: name.to_string(),
            warehouse_opts: warehouse_opts.clone(),
            schedule_opts: schedule_opts.clone(),
            suspend_task_after_num_failures: *suspend_task_after_num_failures,
            comment: comments.clone(),
            sql: sql.clone(),
        };
        Ok(Plan::CreateTask(Box::new(plan)))
    }
}
