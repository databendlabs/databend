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
use std::sync::Arc;

use chrono_tz;
use cron;
use databend_common_ast::ast::AlterTaskOptions;
use databend_common_ast::ast::AlterTaskStmt;
use databend_common_ast::ast::CreateTaskStmt;
use databend_common_ast::ast::DescribeTaskStmt;
use databend_common_ast::ast::DropTaskStmt;
use databend_common_ast::ast::ExecuteTaskStmt;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::ScheduleOptions;
use databend_common_ast::ast::ShowTasksStmt;
use databend_common_ast::ast::Statement;
use databend_common_ast::ast::TaskSql;
use databend_common_ast::parser::ParseMode;
use databend_common_ast::parser::parse_sql;
use databend_common_ast::parser::run_parser;
use databend_common_ast::parser::script::script_block_or_stmt;
use databend_common_ast::parser::tokenize_sql;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use parking_lot::RwLock;

use crate::Binder;
use crate::Metadata;
use crate::plans::AlterTaskPlan;
use crate::plans::CreateTaskPlan;
use crate::plans::DescribeTaskPlan;
use crate::plans::DropTaskPlan;
use crate::plans::ExecuteTaskPlan;
use crate::plans::Plan;
use crate::plans::ShowTasksPlan;

fn verify_scheduler_option(schedule_opts: &Option<ScheduleOptions>) -> Result<()> {
    if schedule_opts.is_none() {
        return Ok(());
    }
    let schedule_opts = schedule_opts.clone().unwrap();
    if let ScheduleOptions::CronExpression(cron_expr, time_zone) = &schedule_opts {
        if cron::Schedule::from_str(cron_expr).is_err() {
            return Err(ErrorCode::SemanticError(format!(
                "invalid cron expression {}",
                cron_expr
            )));
        }
        if let Some(time_zone) = time_zone
            && !time_zone.is_empty()
            && chrono_tz::Tz::from_str(time_zone).is_err()
        {
            return Err(ErrorCode::SemanticError(format!(
                "invalid time zone {}",
                time_zone
            )));
        }
    }

    // ONLY allow milliseconds_interval value between
    // [500, 1000)
    if let ScheduleOptions::IntervalSecs(_, ms) = schedule_opts {
        if ms != 0 && !(500..1000).contains(&ms) {
            return Err(ErrorCode::SemanticError(format!(
                "invalid milliseconds_interval value {}, must be in [500, 1000)",
                ms
            )));
        }
    }

    Ok(())
}

impl Binder {
    /// Validate the SQL carried by a task at CREATE/ALTER time.
    ///
    /// Each statement is parsed once and checked in two layers:
    /// 1. Syntax: the statement (and, for `EXECUTE IMMEDIATE`, its script body) must parse.
    /// 2. Semantics (best-effort): the statement is bound (name/type resolution, logical
    ///    plan), but only `SemanticError` is surfaced. Other binding errors are ignored on
    ///    purpose because task SQL is validated on a best-effort basis and referenced objects
    ///    may not exist yet when the task is created. The interpreter is never invoked, so
    ///    this does not execute the task or cause any of its side effects.
    async fn verify_task_sql(&self, sql: &TaskSql) -> Result<()> {
        match sql {
            TaskSql::SingleStatement(stmt) => self.verify_task_statement(stmt).await,
            TaskSql::ScriptBlock(stmts) => {
                for stmt in stmts {
                    self.verify_task_statement(stmt).await?;
                }
                Ok(())
            }
        }
    }

    async fn verify_task_statement(&self, sql: &str) -> Result<()> {
        // Parse once, reused for both the syntax and the semantic check.
        let tokens = tokenize_sql(sql).map_err(|e| {
            ErrorCode::SyntaxException(format!(
                "syntax error for task formatted sql: {}, error: {:?}",
                sql, e
            ))
        })?;
        let (stmt, _) = parse_sql(&tokens, self.dialect).map_err(|e| {
            ErrorCode::SyntaxException(format!(
                "syntax error for task formatted sql: {}, error: {:?}",
                sql, e
            ))
        })?;

        // `EXECUTE IMMEDIATE $$ ... $$` keeps its script body as a raw string literal,
        // so parsing the outer statement does not validate the script itself. Check its
        // syntax here too.
        if let Statement::ExecuteImmediate(execute) = &stmt {
            self.verify_execute_immediate_script(&execute.script)?;
        }

        self.verify_statement_semantic(stmt).await
    }

    fn verify_execute_immediate_script(&self, script: &Expr) -> Result<()> {
        // Only a constant string literal can be syntax-checked ahead of time.
        let Expr::Literal {
            value: Literal::String(script),
            ..
        } = script
        else {
            return Ok(());
        };

        let tokens = tokenize_sql(script).map_err(|e| {
            ErrorCode::SyntaxException(format!(
                "syntax error for task execute immediate script: {}, error: {:?}",
                script, e
            ))
        })?;
        run_parser(
            &tokens,
            self.dialect,
            ParseMode::Template,
            false,
            script_block_or_stmt,
        )
        .map_err(|e| {
            ErrorCode::SyntaxException(format!(
                "syntax error for task execute immediate script: {}, error: {:?}",
                script, e
            ))
        })?;
        Ok(())
    }

    async fn verify_statement_semantic(&self, stmt: Statement) -> Result<()> {
        // Use a fresh binder with isolated metadata so the outer CREATE/ALTER TASK
        // binding is not polluted. Disable materialized-view rewrite to avoid extra
        // catalog work irrelevant to validation.
        let binder = Binder::new(
            self.ctx.clone(),
            self.catalogs.clone(),
            self.name_resolution_ctx.clone(),
            Arc::new(RwLock::new(Metadata::default())),
        )
        .with_materialized_view_rewrite(false);

        if let Err(e) = binder.bind(&stmt).await {
            // Only reject on semantic errors (e.g. unsupported accessor, type mismatch).
            // Missing objects and other error kinds are tolerated on purpose.
            if e.code() == ErrorCode::SEMANTIC_ERROR {
                return Err(e);
            }
        }
        Ok(())
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_task(
        &mut self,
        stmt: &CreateTaskStmt,
    ) -> Result<Plan> {
        let CreateTaskStmt {
            create_option,
            name,
            warehouse,
            schedule_opts,
            suspend_task_after_num_failures,
            comments,
            after,
            when_condition,
            error_integration,
            sql,
            session_parameters,
        } = stmt;
        if schedule_opts.is_some() && !after.is_empty() {
            return Err(ErrorCode::SyntaxException(
                "task must be defined with either given time schedule as a root task or run after other task as a DAG".to_string(),
            ));
        }
        verify_scheduler_option(schedule_opts)?;
        self.verify_task_sql(sql).await?;

        let tenant = self.ctx.get_tenant();

        let plan = CreateTaskPlan {
            create_option: create_option.clone().into(),
            tenant,
            task_name: name.clone(),
            warehouse: warehouse.clone(),
            schedule_opts: schedule_opts.clone(),
            suspend_task_after_num_failures: *suspend_task_after_num_failures,
            after: after.clone(),
            when_condition: when_condition.as_ref().map(|expr| expr.to_string()),
            comment: comments.clone(),
            session_parameters: session_parameters.clone(),
            error_integration: error_integration.clone(),
            sql: sql.clone(),
        };
        Ok(Plan::CreateTask(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_alter_task(
        &mut self,
        stmt: &AlterTaskStmt,
    ) -> Result<Plan> {
        let AlterTaskStmt {
            if_exists,
            name,
            options,
        } = stmt;

        if let AlterTaskOptions::Set {
            warehouse,
            schedule,
            suspend_task_after_num_failures,
            comments,
            session_parameters,
            error_integration,
        } = options
        {
            if warehouse.is_none()
                && schedule.is_none()
                && suspend_task_after_num_failures.is_none()
                && comments.is_none()
                && session_parameters.is_none()
                && error_integration.is_none()
            {
                return Err(ErrorCode::SyntaxException(
                    "alter task must set at least one option".to_string(),
                ));
            }
            if schedule.is_some() {
                verify_scheduler_option(schedule)?;
            }
        }

        if let AlterTaskOptions::ModifyAs(sql) = options {
            self.verify_task_sql(sql).await?;
        }

        let tenant = self.ctx.get_tenant();

        let plan = AlterTaskPlan {
            if_exists: *if_exists,
            tenant,
            task_name: name.clone(),
            alter_options: options.clone(),
        };
        Ok(Plan::AlterTask(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_drop_task(
        &mut self,
        stmt: &DropTaskStmt,
    ) -> Result<Plan> {
        let DropTaskStmt { if_exists, name } = stmt;

        let tenant = self.ctx.get_tenant();

        let plan = DropTaskPlan {
            if_exists: *if_exists,
            tenant,
            task_name: name.clone(),
        };
        Ok(Plan::DropTask(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_describe_task(
        &mut self,
        stmt: &DescribeTaskStmt,
    ) -> Result<Plan> {
        let tenant = self.ctx.get_tenant();

        let plan = DescribeTaskPlan {
            tenant,
            task_name: stmt.name.to_string(),
        };
        Ok(Plan::DescribeTask(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_execute_task(
        &mut self,
        stmt: &ExecuteTaskStmt,
    ) -> Result<Plan> {
        let tenant = self.ctx.get_tenant();

        let plan = ExecuteTaskPlan {
            tenant,
            task_name: stmt.name.to_string(),
        };
        Ok(Plan::ExecuteTask(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_tasks(
        &mut self,
        stmt: &ShowTasksStmt,
    ) -> Result<Plan> {
        let ShowTasksStmt { limit } = stmt;

        let tenant = self.ctx.get_tenant();

        let plan = ShowTasksPlan {
            tenant,
            limit: limit.clone(),
        };
        Ok(Plan::ShowTasks(Box::new(plan)))
    }
}
