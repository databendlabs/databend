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

use databend_common_base::runtime::GlobalIORuntime;
use databend_common_catalog::lock::LockTableOption;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_pipeline_core::ExecutionInfo;
use databend_common_sql::executor::PhysicalPlanBuilder;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::OptimizeCompactBlock;
use databend_common_sql::ColumnSet;
use databend_common_sql::MetadataRef;

use crate::interpreters::interpreter_optimize_purge::purge;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct OptimizeCompactBlockInterpreter {
    ctx: Arc<QueryContext>,
    s_expr: SExpr,
    lock_opt: LockTableOption,
    need_purge: bool,
}

impl OptimizeCompactBlockInterpreter {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        s_expr: SExpr,
        lock_opt: LockTableOption,
        need_purge: bool,
    ) -> Result<Self> {
        Ok(OptimizeCompactBlockInterpreter {
            ctx,
            s_expr,
            lock_opt,
            need_purge,
        })
    }
}

#[async_trait::async_trait]
impl Interpreter for OptimizeCompactBlockInterpreter {
    fn name(&self) -> &str {
        "OptimizeCompactBlockInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let OptimizeCompactBlock {
            catalog,
            database,
            table,
            limit,
        } = self.s_expr.plan().clone().try_into()?;

        // try add lock table.
        let lock_guard = self
            .ctx
            .clone()
            .acquire_table_lock(&catalog, &database, &table, &self.lock_opt)
            .await?;

        let mut build_res = PipelineBuildResult::create();
        let mut builder = PhysicalPlanBuilder::new(MetadataRef::default(), self.ctx.clone(), false);
        match builder.build(&self.s_expr, ColumnSet::new()).await {
            Ok(physical_plan) => {
                build_res =
                    build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan)
                        .await?;
                build_res.main_pipeline.add_lock_guard(lock_guard);
            }
            Err(e) => {
                if e.code() != ErrorCode::NO_NEED_TO_COMPACT {
                    return Err(e);
                }
            }
        }

        if self.need_purge {
            let ctx = self.ctx.clone();
            let num_snapshot_limit = limit.segment_limit;
            if build_res.main_pipeline.is_empty() {
                purge(ctx, &catalog, &database, &table, num_snapshot_limit, None).await?;
            } else {
                build_res
                    .main_pipeline
                    .set_on_finished(move |info: &ExecutionInfo| match &info.res {
                        Ok(_) => GlobalIORuntime::instance().block_on(async move {
                            purge(ctx, &catalog, &database, &table, num_snapshot_limit, None).await
                        }),
                        Err(error_code) => Err(error_code.clone()),
                    });
            }
        }
        Ok(build_res)
    }
}
