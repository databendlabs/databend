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
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::ScheduleOptions;
use databend_common_ast::ast::ShowTasksStmt;
use databend_common_ast::ast::Statement;
use databend_common_ast::ast::TaskSql;
use databend_common_ast::parser::parse_sql;
use databend_common_ast::parser::tokenize_sql;
use databend_common_ast::visit::VisitControl;
use databend_common_ast::visit::Visitor;
use databend_common_ast::visit::Walk;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_script::compile_block;
use databend_common_script::ir::ScriptIR;
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

/// Stop at a runtime script variable rather than passing an unbound template to SQL binding.
struct ScriptVariableFinder;

impl Visitor for ScriptVariableFinder {
    fn visit_expr(&mut self, expr: &Expr) -> std::result::Result<VisitControl, !> {
        Ok(if matches!(expr, Expr::Hole { .. }) {
            VisitControl::Break(())
        } else {
            VisitControl::Continue
        })
    }

    fn visit_identifier(&mut self, ident: &Identifier) -> std::result::Result<VisitControl, !> {
        Ok(if ident.is_hole() {
            VisitControl::Break(())
        } else {
            VisitControl::Continue
        })
    }
}

/// Whether a bind failure is inconclusive during definition-time task validation.
///
/// Task validation is best-effort: it uses the current planning context and does not
/// execute task statements. Known catalog, privilege, and environment-dependent failures
/// are deferred until execution. Other errors found by this validation pass are reported,
/// but successful validation does not guarantee that runtime planning or execution succeeds.
fn is_deferrable_bind_error(code: u16) -> bool {
    matches!(
        code,
        // Objects the task may legitimately create, or have created for it, later on.
        ErrorCode::UNKNOWN_DATABASE
            | ErrorCode::UNKNOWN_DATABASE_ID
            | ErrorCode::UNKNOWN_TABLE
            | ErrorCode::UNKNOWN_TABLE_ID
            | ErrorCode::UNKNOWN_VIEW
            | ErrorCode::UNKNOWN_COLUMN
            | ErrorCode::UNKNOWN_CATALOG
            | ErrorCode::UNKNOWN_STREAM
            | ErrorCode::UNKNOWN_STREAM_ID
            | ErrorCode::UNKNOWN_SEQUENCE
            | ErrorCode::UNKNOWN_INDEX
            | ErrorCode::UNKNOWN_DICTIONARY
            | ErrorCode::UNKNOWN_STAGE
            | ErrorCode::UNKNOWN_CONNECTION
            | ErrorCode::UNKNOWN_FILE_FORMAT
            | ErrorCode::UNKNOWN_PROCEDURE
            | ErrorCode::UNKNOWN_DATAMASK
            | ErrorCode::UNKNOWN_MASK_POLICY
            | ErrorCode::UNKNOWN_ROW_ACCESS_POLICY
            | ErrorCode::UNKNOWN_WAREHOUSE
            | ErrorCode::UNKNOWN_WORKLOAD
            // A user-defined function that does not exist yet resolves as a builtin lookup,
            // so a forward reference to it surfaces as an unknown (aggregate) function.
            | ErrorCode::UNKNOWN_U_D_F
            | ErrorCode::UNKNOWN_FUNCTION
            | ErrorCode::UNKNOWN_AGGREGATE_FUNCTION
            // Privileges the task owner may be granted before the task runs.
            | ErrorCode::UNKNOWN_USER
            | ErrorCode::UNKNOWN_ROLE
            | ErrorCode::PERMISSION_DENIED
            | ErrorCode::STAGE_PERMISSION_DENIED
            | ErrorCode::STORAGE_PERMISSION_DENIED
            | ErrorCode::MANAGEMENT_MODE_PERMISSION_DENIED
            // Environment failures that say nothing about the SQL itself.
            | ErrorCode::CANNOT_CONNECT_NODE
            | ErrorCode::META_SERVICE_ERROR
            | ErrorCode::META_STORAGE_ERROR
            | ErrorCode::STORAGE_NOT_FOUND
            | ErrorCode::STORAGE_UNAVAILABLE
            | ErrorCode::STORAGE_OTHER
    )
}

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
    /// Best-effort bind statements and compile constant `EXECUTE IMMEDIATE` blocks,
    /// including binding their static SQL and expressions in the current planning context.
    /// This pass does not reproduce every runtime setting or planning facility. Known
    /// inconclusive failures are deferred (see [`is_deferrable_bind_error`]), and accepted
    /// task SQL is bound again in its runtime context. No task statement is executed here.
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

        self.verify_statement_semantic(stmt).await
    }

    async fn verify_statement_semantic(&self, stmt: Statement) -> Result<()> {
        let mut pending = vec![stmt];
        while let Some(stmt) = pending.pop() {
            // Applying statement settings here would mutate the outer CREATE/ALTER context,
            // while binding without applying them can reject valid task SQL. Syntax has
            // already been checked, so leave these statements to runtime planning.
            if matches!(&stmt, Statement::StatementWithSettings { .. }) {
                continue;
            }

            // Isolate metadata from both the outer CREATE/ALTER TASK and other script
            // statements. Avoid materialized-view catalog work irrelevant to validation.
            let binder = Binder::new(
                self.ctx.clone(),
                self.catalogs.clone(),
                self.name_resolution_ctx.clone(),
                Arc::new(RwLock::new(Metadata::default())),
            )
            .with_materialized_view_rewrite(false);

            match binder.bind(&stmt).await {
                Ok(Plan::ExecuteImmediate(plan)) => {
                    // Compilation checks script scopes/control flow and lowers expressions
                    // and SQL in every branch to Query instructions. Do not run the IR.
                    let compiled = compile_block(plan.script_block)
                        .map_err(|e| e.display_with_sql(&plan.script))?;
                    for instruction in compiled.into_iter().rev() {
                        if let ScriptIR::Query { stmt, .. } = instruction {
                            // Holes need runtime values (including dynamic identifiers).
                            // Substituting dummy values could reject valid scripts or hide
                            // type errors, so only bind fully static templates here.
                            if matches!(
                                stmt.stmt.walk(&mut ScriptVariableFinder)?,
                                VisitControl::Continue
                            ) {
                                pending.push(stmt.stmt);
                            }
                        }
                    }
                }
                Err(e) if !is_deferrable_bind_error(e.code()) => return Err(e),
                // Deferrable failures, such as objects that do not exist yet, stay
                // best-effort: the task may still bind successfully when it runs.
                _ => {}
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
