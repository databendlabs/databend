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

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use databend_common_ast::ast::Query;
use databend_common_ast::ast::Statement;
use databend_common_catalog::lock::LockTableOption;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_sql::executor::PhysicalPlanBuilder;
use databend_common_sql::optimizer::SExpr;
use databend_common_sql::plans::set_update_stream_columns;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::Recluster;
use databend_common_sql::MetadataRef;
use databend_common_sql::Planner;
use log::error;
use log::warn;

use crate::interpreters::hook::vacuum_hook::hook_disk_temp_dir;
use crate::interpreters::hook::vacuum_hook::hook_vacuum_temp_files;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterClusteringHistory;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct ReclusterTableInterpreter {
    ctx: Arc<QueryContext>,
    s_expr: SExpr,
    hilbert_query: Option<Box<Query>>,
    lock_opt: LockTableOption,
    is_final: bool,
}

impl ReclusterTableInterpreter {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        s_expr: SExpr,
        hilbert_query: Option<Box<Query>>,
        lock_opt: LockTableOption,
        is_final: bool,
    ) -> Result<Self> {
        Ok(Self {
            ctx,
            s_expr,
            hilbert_query,
            lock_opt,
            is_final,
        })
    }
}

#[async_trait::async_trait]
impl Interpreter for ReclusterTableInterpreter {
    fn name(&self) -> &str {
        "ReclusterTableInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let ctx = self.ctx.clone();
        let recluster_timeout_secs = ctx.get_settings().get_recluster_timeout_secs()?;

        let mut times = 0;
        let start = SystemTime::now();
        let timeout = Duration::from_secs(recluster_timeout_secs);
        let plan: Recluster = self.s_expr.plan().clone().try_into()?;
        loop {
            if let Err(err) = ctx.check_aborting() {
                error!(
                    "execution of recluster statement aborted. server is shutting down or the query was killed",
                );
                return Err(err.with_context("failed to execute"));
            }

            let res = self.execute_recluster(plan.clone()).await;

            match res {
                Ok(is_break) => {
                    if is_break {
                        break;
                    }
                }
                Err(e) => {
                    if self.is_final
                        && matches!(
                            e.code(),
                            ErrorCode::TABLE_LOCK_EXPIRED
                                | ErrorCode::TABLE_ALREADY_LOCKED
                                | ErrorCode::TABLE_VERSION_MISMATCHED
                                | ErrorCode::UNRESOLVABLE_CONFLICT
                        )
                    {
                        warn!("Execute recluster error: {:?}", e);
                    } else {
                        return Err(e);
                    }
                }
            }

            let elapsed_time = SystemTime::now().duration_since(start).unwrap();
            times += 1;
            // Status.
            {
                let status = format!(
                    "recluster: run recluster tasks:{} times, cost:{:?}",
                    times, elapsed_time
                );
                ctx.set_status_info(&status);
            }

            if !self.is_final {
                break;
            }

            if elapsed_time >= timeout {
                warn!(
                    "Recluster stopped because the runtime was over {:?}",
                    timeout
                );
                break;
            }

            self.ctx.clear_selected_segment_locations();
            self.ctx
                .evict_table_from_cache(&plan.catalog, &plan.database, &plan.table)?;
        }

        Ok(PipelineBuildResult::create())
    }
}

impl ReclusterTableInterpreter {
    async fn execute_recluster(&self, op: Recluster) -> Result<bool> {
        let start = SystemTime::now();

        // try to add lock table.
        let lock_guard = self
            .ctx
            .clone()
            .acquire_table_lock(&op.catalog, &op.database, &op.table, &self.lock_opt)
            .await?;

        let tbl = self
            .ctx
            .get_table(&op.catalog, &op.database, &op.table)
            .await?;
        let (s_expr, metadata, required) = if let Some(hilbert) = &self.hilbert_query {
            let mut planner = Planner::new(self.ctx.clone());
            let plan = planner
                .plan_stmt(&Statement::Query(hilbert.clone()), false)
                .await?;
            let Plan::Query {
                mut s_expr,
                metadata,
                bind_context,
                ..
            } = plan
            else {
                unreachable!()
            };
            if tbl.change_tracking_enabled() {
                *s_expr = set_update_stream_columns(&s_expr)?;
            }
            let s_expr = self.s_expr.replace_children(vec![Arc::new(*s_expr)]);
            (s_expr, metadata, bind_context.column_set())
        } else {
            (self.s_expr.clone(), MetadataRef::default(), HashSet::new())
        };

        let mut builder = PhysicalPlanBuilder::new(metadata, self.ctx.clone(), false);
        let physical_plan = match builder.build(&s_expr, required).await {
            Ok(res) => res,
            Err(e) => {
                return if e.code() == ErrorCode::NO_NEED_TO_RECLUSTER {
                    Ok(true)
                } else {
                    Err(e)
                };
            }
        };

        let mut build_res =
            build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan).await?;
        debug_assert!(build_res.main_pipeline.is_complete_pipeline()?);

        let max_threads = self.ctx.get_settings().get_max_threads()? as usize;
        build_res.set_max_threads(max_threads);

        let executor_settings = ExecutorSettings::try_create(self.ctx.clone())?;

        let mut pipelines = build_res.sources_pipelines;
        pipelines.push(build_res.main_pipeline);

        let complete_executor =
            PipelineCompleteExecutor::from_pipelines(pipelines, executor_settings)?;
        self.ctx.clear_inserted_segment_locations()?;
        self.ctx.set_executor(complete_executor.get_inner())?;
        complete_executor.execute()?;
        // make sure the executor is dropped before the next loop.
        drop(complete_executor);
        // make sure the lock guard is dropped before the next loop.
        drop(lock_guard);

        // vacuum temp files.
        hook_vacuum_temp_files(&self.ctx)?;
        hook_disk_temp_dir(&self.ctx)?;

        InterpreterClusteringHistory::write_log(&self.ctx, start, &op.database, &op.table)?;
        Ok(false)
    }
}
