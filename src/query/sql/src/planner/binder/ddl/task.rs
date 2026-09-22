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
use databend_common_ast::ast::ScriptBlock;
use databend_common_ast::ast::ShowTasksStmt;
use databend_common_ast::ast::Statement;
use databend_common_ast::ast::TaskSql;
use databend_common_ast::parser::Dialect;
use databend_common_ast::parser::ParseMode;
use databend_common_ast::parser::parse_sql;
use databend_common_ast::parser::run_parser;
use databend_common_ast::parser::script::ScriptBlockOrStmt;
use databend_common_ast::parser::script::script_block_or_stmt;
use databend_common_ast::parser::tokenize_sql;
use databend_common_exception::ErrorCode;
use databend_common_exception::ParseError;
use databend_common_exception::Result;
use databend_common_script::compile_block;
use databend_common_script::ir::ScriptIR;

use crate::Binder;
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

/// Task bodies are normalised to PostgreSQL-dialect SQL when the `CREATE TASK` statement
/// is parsed, so that is the dialect they are re-parsed with here.
const TASK_SQL_DIALECT: Dialect = Dialect::PostgreSQL;

/// Validate every statement of a task body.
///
/// `CREATE TASK` and `ALTER TASK ... MODIFY AS` run this pass over the task body. It is
/// deliberately static: it parses SQL, expands constant `EXECUTE IMMEDIATE` scripts and
/// compiles script blocks, recursively for nested constant scripts. It never binds statements, never resolves catalog objects or
/// UDFs, never executes anything and never touches the session context, so the task body
/// is rejected only for errors that are certain regardless of the runtime environment.
///
/// This is *not* a semantic check. Unknown tables or columns, type errors, missing UDFs
/// and anything else that needs a catalog or schema are only detected when the task runs.
///
/// Binding the task body here to catch those errors early was tried and dropped, because
/// the production `Binder` cannot be used as a side-effect-free checker:
///
/// - It executes things. Constant arguments of immutable server UDFs are folded by
///   calling the UDF server, sandboxed script UDFs provision a cloud worker, dynamic
///   `PIVOT` and `MATERIALIZED` CTEs run subqueries, and DML binding takes table locks.
///   A `CREATE TASK` must not do any of this.
/// - It mutates the shared `QueryContext`. For example `WITH CONSUME` registers a stream
///   ref that the outer `CREATE TASK` binding then rejects as its own stream consumption.
/// - The runtime environment differs from the definition-time one. Tasks run in a fresh
///   session with their own session parameters, current database and role, and
///   frequently reference tables, UDFs or streams that do not exist yet. Deciding which
///   bind errors are "real" therefore needs an allowlist of error codes, which is fragile
///   and still rejects valid tasks.
///
/// Until the binder offers an explicit validation mode that guarantees none of the above,
/// definition-time validation stays purely syntactic.
fn validate_task_sql(sql: &TaskSql) -> Result<()> {
    match sql {
        TaskSql::SingleStatement(stmt) => validate_statement_sql(stmt),
        TaskSql::ScriptBlock(stmts) => stmts
            .iter()
            .try_for_each(|stmt| validate_statement_sql(stmt)),
    }
}

fn validate_statement_sql(sql: &str) -> Result<()> {
    let stmt = parse_statement(sql)?;
    validate_statement(stmt)
}

fn parse_statement(sql: &str) -> Result<Statement> {
    let syntax_error = |e: ParseError| {
        ErrorCode::SyntaxException(format!(
            "syntax error for task formatted sql: {}, error: {:?}",
            sql, e
        ))
    };
    let tokens = tokenize_sql(sql).map_err(syntax_error)?;
    let (stmt, _) = parse_sql(&tokens, TASK_SQL_DIALECT).map_err(syntax_error)?;
    Ok(stmt)
}

/// Walk a statement and, recursively, the constant scripts it carries.
///
/// Only `EXECUTE IMMEDIATE` with a string literal is expanded. Any other script
/// expression needs evaluation, which is out of scope for a static check, so those
/// statements are accepted here and planned at runtime.
fn validate_statement(stmt: Statement) -> Result<()> {
    // Each level is a string literal inside the previous one, so the input strictly
    // shrinks and the expansion always terminates.
    let mut pending = vec![stmt];
    while let Some(stmt) = pending.pop() {
        let Some(script) = constant_execute_immediate(&stmt) else {
            continue;
        };
        match parse_script(script)? {
            ScriptBlockOrStmt::Statement(nested) => pending.push(nested),
            ScriptBlockOrStmt::ScriptBlock(block) => {
                pending.extend(compile_script_block(block, script)?);
            }
        }
    }
    Ok(())
}

fn parse_script(script: &str) -> Result<ScriptBlockOrStmt> {
    let tokens = tokenize_sql(script)?;
    Ok(run_parser(
        &tokens,
        TASK_SQL_DIALECT,
        ParseMode::Template,
        false,
        script_block_or_stmt,
    )?)
}

/// The script of a constant `EXECUTE IMMEDIATE`, looking through a `SETTINGS` wrapper.
fn constant_execute_immediate(stmt: &Statement) -> Option<&str> {
    match stmt {
        Statement::StatementWithSettings { stmt, .. } => constant_execute_immediate(stmt),
        Statement::ExecuteImmediate(execute) => match &execute.script {
            Expr::Literal {
                value: Literal::String(script),
                ..
            } => Some(script),
            _ => None,
        },
        _ => None,
    }
}

/// Compile a script block and collect the SQL statements it lowers to.
///
/// Compilation checks declarations, scopes and control flow (for example `BREAK` outside
/// a loop or a `RETURN` of an undeclared variable). Expressions and SQL in every branch
/// are lowered to `Query` instructions; the resulting IR is never run. The statements may
/// still contain `:var` holes that are only filled at runtime.
fn compile_script_block(block: ScriptBlock, script: &str) -> Result<Vec<Statement>> {
    let compiled = compile_block(block).map_err(|e| e.display_with_sql(script))?;
    Ok(compiled
        .into_iter()
        .filter_map(|instruction| match instruction {
            ScriptIR::Query { stmt, .. } => Some(stmt.stmt),
            _ => None,
        })
        .collect())
}

impl Binder {
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
        // Syntax and script structure only. The body is deliberately not bound here, so
        // semantic errors (unknown tables or columns, type mismatches, missing UDFs, ...)
        // are reported when the task runs. See `validate_task_sql` for why.
        validate_task_sql(sql)?;

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
            // Same as CREATE TASK: syntax and script structure only, no semantic check.
            validate_task_sql(sql)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn validate(sql: &str) -> Result<()> {
        validate_task_sql(&TaskSql::SingleStatement(sql.to_string()))
    }

    #[test]
    fn constant_scripts_are_parsed_and_compiled() {
        let rejected = [
            ("SELECT FROM", ErrorCode::SYNTAX_EXCEPTION),
            (
                "EXECUTE IMMEDIATE 'SELECT FROM'",
                ErrorCode::SYNTAX_EXCEPTION,
            ),
            (
                "EXECUTE IMMEDIATE $$ BEGIN RETURN unknown_variable; END; $$",
                ErrorCode::SCRIPT_SEMANTIC_ERROR,
            ),
            // Nested constant scripts are expanded and compiled on their own.
            (
                "EXECUTE IMMEDIATE $$ BEGIN LOOP EXECUTE IMMEDIATE 'BEGIN CONTINUE; END;'; END LOOP; END; $$",
                ErrorCode::SCRIPT_SEMANTIC_ERROR,
            ),
        ];
        for (sql, code) in rejected {
            let err = validate(sql).unwrap_err();
            assert_eq!(err.code(), code, "{sql}: {err}");
        }

        // Nothing is bound and non-literal scripts are not evaluated.
        let accepted = [
            "EXECUTE IMMEDIATE 'SELECT ' || 'FROM'",
            "SELECT udf_not_created_yet(no_such_col) FROM not_created_yet",
            "EXECUTE IMMEDIATE $$ DECLARE t := 'x'; BEGIN SELECT * FROM IDENTIFIER(:t); RETURN t; END; $$",
        ];
        for sql in accepted {
            validate(sql).unwrap_or_else(|e| panic!("{sql}: {e}"));
        }

        // Every statement of a task script block is checked.
        let block = TaskSql::ScriptBlock(vec![
            "SELECT 1".to_string(),
            "EXECUTE IMMEDIATE 'SELECT FROM'".to_string(),
        ]);
        assert_eq!(
            validate_task_sql(&block).unwrap_err().code(),
            ErrorCode::SYNTAX_EXCEPTION
        );
    }
}
