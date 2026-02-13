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

// Logs from this module will show up as "[INTERPRETER] ...".
databend_common_tracing::register_module_tag!("[INTERPRETER]");

use std::sync::Arc;

use databend_common_ast::ast::AlterTableAction;
use databend_common_ast::ast::AlterTableStmt;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::ModifyColumnAction;
use databend_common_ast::ast::OptimizeTableAction;
use databend_common_ast::ast::OptimizeTableStmt;
use databend_common_ast::ast::Statement;
use databend_common_base::base::short_sql;
use databend_common_catalog::query_kind::QueryKind;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_exception::ResultExt;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::SendableDataBlockStream;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_pipeline::core::ExecutionInfo;
use databend_common_pipeline::core::SourcePipeBuilder;
use databend_common_pipeline::core::always_callback;
use databend_common_sql::PlanExtras;
use databend_common_sql::Planner;
use databend_common_sql::plans::Plan;
use databend_storages_common_cache::CacheManager;
use derive_visitor::DriveMut;
use derive_visitor::VisitorMut;
use md5::Digest;
use md5::Md5;

use super::hook::vacuum_hook::hook_clear_m_cte_temp_table;
use super::hook::vacuum_hook::hook_disk_temp_dir;
use super::hook::vacuum_hook::hook_vacuum_temp_files;
use crate::interpreters::common::log_query_finished;
use crate::interpreters::common::log_query_start;
use crate::interpreters::interpreter_txn_commit::execute_commit_statement;
use crate::pipelines::PipelineBuildResult;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::pipelines::executor::PipelinePullingExecutor;
use crate::schedulers::ServiceQueryExecutor;
use crate::sessions::AcquireQueueGuard;
use crate::sessions::QueriesQueueManager;
use crate::sessions::QueryContext;
use crate::sessions::QueryEntry;
use crate::stream::DataBlockStream;
use crate::stream::ProgressStream;
use crate::stream::PullingExecutorStream;

#[async_trait::async_trait]
/// Interpreter is a trait for different PlanNode
/// Each type of planNode has its own corresponding interpreter
pub trait Interpreter: Sync + Send {
    /// Return the name of Interpreter, such as "CreateDatabaseInterpreter"
    fn name(&self) -> &str;

    fn is_txn_command(&self) -> bool {
        false
    }

    fn is_ddl(&self) -> bool;

    /// The core of the databend processor which will execute the logical plan and get the DataBlock
    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn execute(&self, ctx: Arc<QueryContext>) -> Result<SendableDataBlockStream> {
        log_query_start(&ctx);
        match self.execute_inner(ctx.clone()).await {
            Ok(stream) => Ok(stream),
            Err(err) => {
                log_query_finished(&ctx, Some(err.clone()));
                Err(err)
            }
        }
    }

    async fn get_dynamic_schema(&self) -> Option<DataSchemaRef> {
        None
    }

    async fn execute_inner(&self, ctx: Arc<QueryContext>) -> Result<SendableDataBlockStream> {
        {
            let mutation_status = ctx.get_mutation_status();
            let mut mutation_status = mutation_status.write();
            mutation_status.insert_rows = 0;
            mutation_status.deleted_rows = 0;
            mutation_status.update_rows = 0;
        }

        let make_error = || "failed to execute interpreter";

        ctx.set_status_info("Building execution pipeline");
        ctx.check_aborting().with_context(make_error)?;

        let allow_disk_cache = {
            let license_key = ctx.get_license_key();
            match LicenseManagerSwitch::instance().check_license(license_key.clone()) {
                Ok(_) => true,
                Err(e) if !license_key.is_empty() => {
                    let msg = format!(
                        "CRITICAL ALERT: License validation FAILED - enterprise features DISABLED, System may operate in DEGRADED MODE with LIMITED CAPABILITIES and REDUCED PERFORMANCE. Please contact us at https://www.databend.com/contact-us/ or email hi@databend.com to restore full functionality: {}",
                        e
                    );
                    log::error!("{msg}");

                    // Also log at warning level to ensure the message could be propagated to client applications
                    // (e.g., BendSQL and MySQL interactive sessions)
                    log::warn!("{msg}");
                    false
                }
                _ => false,
            }
        };

        CacheManager::instance().set_allows_disk_cache(allow_disk_cache);

        let mut build_res = match self.execute2().await {
            Ok(build_res) => build_res,
            Err(err) => {
                return Err(err);
            }
        };

        if build_res.main_pipeline.is_empty() {
            log_query_finished(&ctx, None);
            return Ok(Box::pin(DataBlockStream::create(None, vec![])));
        }

        let query_ctx = ctx.clone();
        build_res
            .main_pipeline
            .set_on_finished(always_callback(move |info: &ExecutionInfo| {
                on_execution_finished(info, query_ctx)
            }));

        ctx.set_status_info("Executing pipeline");

        let settings = ctx.get_settings();
        build_res.set_max_threads(settings.get_max_threads()? as usize);
        let settings = ExecutorSettings::try_create(ctx.clone())?;

        if build_res.main_pipeline.is_complete_pipeline()? {
            let mut pipelines = build_res.sources_pipelines;
            pipelines.push(build_res.main_pipeline);

            let complete_executor = PipelineCompleteExecutor::from_pipelines(pipelines, settings)?;

            ctx.set_executor(complete_executor.get_inner())?;
            complete_executor.execute()?;
            self.inject_result()
        } else {
            let pulling_executor = PipelinePullingExecutor::from_pipelines(build_res, settings)?;

            ctx.set_executor(pulling_executor.get_inner())?;
            Ok(Box::pin(ProgressStream::try_create(
                Box::pin(PullingExecutorStream::create(pulling_executor)?),
                ctx.get_result_progress(),
            )?))
        }
    }

    /// The core of the databend processor which will execute the logical plan and build the pipeline
    async fn execute2(&self) -> Result<PipelineBuildResult>;

    fn set_source_pipe_builder(&self, _builder: Option<SourcePipeBuilder>) -> Result<()> {
        Err(ErrorCode::Unimplemented(format!(
            "UnImplement set_source_pipe_builder method for {:?}",
            self.name()
        )))
    }

    fn inject_result(&self) -> Result<SendableDataBlockStream> {
        Ok(Box::pin(DataBlockStream::create(None, vec![])))
    }
}

pub type InterpreterPtr = Arc<dyn Interpreter>;

/// There are two steps to execute a query:
/// 1. Plan the SQL
/// 2. Execute the plan -- interpreter
///
/// This function is used to plan the SQL. If an error occurs, we will log the query start and finished.
pub async fn interpreter_plan_sql(
    ctx: Arc<QueryContext>,
    sql: &str,
    acquire_queue: bool,
) -> Result<(Plan, PlanExtras, AcquireQueueGuard)> {
    let result = plan_sql(ctx.clone(), sql, acquire_queue).await;
    let short_sql = short_sql(
        sql.to_string(),
        ctx.get_settings().get_short_sql_max_length()?,
    );
    let mut stmt = if let Ok((_, extras, _)) = &result {
        Some(extras.statement.clone())
    } else {
        // Only log if there's an error
        ctx.attach_query_str(QueryKind::Unknown, short_sql.to_string());
        log_query_start(&ctx);
        log_query_finished(&ctx, result.as_ref().err().cloned());
        None
    };

    attach_query_hash(&ctx, &mut stmt, &short_sql);

    result
}

pub async fn auto_commit_if_not_allowed_in_transaction(
    ctx: Arc<QueryContext>,
    stmt: &Statement,
) -> Result<()> {
    if !stmt.allowed_in_multi_statement() {
        execute_commit_statement(ctx.clone()).await?;
    }
    if !stmt.is_transaction_command() && ctx.txn_mgr().lock().is_fail() {
        let err = ErrorCode::CurrentTransactionIsAborted(
            "Current transaction is aborted, commands ignored until end of transaction block",
        );
        return Err(err);
    }
    Ok(())
}

// Note: Please synchronize the changes made in the plan_sql function to src/query/service/src/interpreters/interpreter_report_issue.rs
async fn plan_sql(
    ctx: Arc<QueryContext>,
    sql: &str,
    acquire_queue: bool,
) -> Result<(Plan, PlanExtras, AcquireQueueGuard)> {
    let mut planner = Planner::new_with_query_executor(
        ctx.clone(),
        Arc::new(ServiceQueryExecutor::new(QueryContext::create_from(
            ctx.as_ref(),
        ))),
    );

    // Parse the SQL query, get extract additional information.
    let extras = planner.parse_sql(sql)?;
    auto_commit_if_not_allowed_in_transaction(ctx.clone(), &extras.statement).await?;
    if !acquire_queue {
        // If queue guard is not required, plan the statement directly.
        let plan = planner.plan_stmt(&extras.statement, false).await?;
        return Ok((plan, extras, AcquireQueueGuard::create(vec![])));
    }

    let need_acquire_lock = need_acquire_lock(ctx.clone(), &extras.statement);
    if need_acquire_lock {
        // If a lock is required, acquire the queue guard before
        // planning the statement, to avoid potential deadlocks.
        // See PR https://github.com/databendlabs/databend/pull/16632
        let query_entry = QueryEntry::create_entry(&ctx, &extras, true)?;
        let guard = QueriesQueueManager::instance().acquire(query_entry).await?;
        let plan = planner.plan_stmt(&extras.statement, false).await?;
        Ok((plan, extras, guard))
    } else {
        // No lock is needed, plan the statement first, then acquire the queue guard.
        let plan = planner.plan_stmt(&extras.statement, false).await?;
        let query_entry = QueryEntry::create(&ctx, &plan, &extras)?;
        let guard = QueriesQueueManager::instance().acquire(query_entry).await?;
        Ok((plan, extras, guard))
    }
}

fn attach_query_hash(ctx: &Arc<QueryContext>, stmt: &mut Option<Statement>, sql: &str) {
    let (query_hash, query_parameterized_hash) = if let Some(stmt) = stmt {
        let query_hash = format!("{:x}", Md5::digest(stmt.to_string()));
        // Use Literal::Null replace literal. Ignore Literal.
        // SELECT * FROM t1 WHERE name = 'data' => SELECT * FROM t1 WHERE name = NULL
        // SELECT * FROM t1 WHERE name = 'bend' => SELECT * FROM t1 WHERE name = NULL
        #[derive(VisitorMut)]
        #[visitor(Literal(enter))]
        struct AstVisitor;

        impl AstVisitor {
            fn enter_literal(&mut self, lit: &mut Literal) {
                *lit = Literal::Null;
            }
        }

        stmt.drive_mut(&mut AstVisitor);

        (query_hash, format!("{:x}", Md5::digest(stmt.to_string())))
    } else {
        let hash = format!("{:x}", Md5::digest(sql));
        (hash.to_string(), hash)
    };

    ctx.attach_query_hash(query_hash, query_parameterized_hash);
}

fn run_hooks(query_ctx: Arc<QueryContext>) -> Result<()> {
    hook_clear_m_cte_temp_table(&query_ctx)?;
    hook_vacuum_temp_files(&query_ctx)?;
    hook_disk_temp_dir(&query_ctx)
}

#[fastrace::trace]
pub fn on_execution_finished(info: &ExecutionInfo, query_ctx: Arc<QueryContext>) -> Result<()> {
    query_ctx.add_query_profiles(&info.profiling);

    let hooks_res = run_hooks(query_ctx.clone());

    log_query_finished(&query_ctx, info.res.clone().err());

    info.res.clone().and(hooks_res)
}

/// Check if the statement need acquire a table lock.
fn need_acquire_lock(ctx: Arc<QueryContext>, stmt: &Statement) -> bool {
    if !ctx.get_settings().get_enable_table_lock().unwrap_or(false) {
        return false;
    }

    match stmt {
        Statement::Replace(_)
        | Statement::MergeInto(_)
        | Statement::Update(_)
        | Statement::Delete(_)
        | Statement::TruncateTable(_) => true,
        Statement::OptimizeTable(OptimizeTableStmt { action, .. }) => matches!(
            action,
            OptimizeTableAction::All | OptimizeTableAction::Compact { .. }
        ),
        Statement::AlterTable(AlterTableStmt { action, .. }) => matches!(
            action,
            AlterTableAction::ReclusterTable { .. }
                | AlterTableAction::ModifyColumn {
                    action: ModifyColumnAction::SetDataType(_),
                }
        ),
        _ => false,
    }
}
