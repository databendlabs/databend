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

use databend_common_base::runtime::GlobalIORuntime;
use databend_common_catalog::lock::LockTableOption;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_pipeline_core::ExecutionInfo;
use databend_common_sql::executor::PhysicalPlanBuilder;
use databend_common_sql::optimizer::SExpr;
use databend_common_sql::plans::OptimizeCompactBlock;
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
        let plan: OptimizeCompactBlock = self.s_expr.plan().clone().try_into()?;
        // try add lock table.
        let lock_guard = self
            .ctx
            .clone()
            .acquire_table_lock(&plan.catalog, &plan.database, &plan.table, &self.lock_opt)
            .await?;

        let mut builder = PhysicalPlanBuilder::new(MetadataRef::default(), self.ctx.clone(), false);
        match builder.build(&self.s_expr, HashSet::new()).await {
            Ok(physical_plan) => {
                let mut build_res =
                    build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan)
                        .await?;
                if self.need_purge {
                    let ctx = self.ctx.clone();
                    build_res.main_pipeline.set_on_finished(
                        move |info: &ExecutionInfo| match &info.res {
                            Ok(_) => GlobalIORuntime::instance().block_on(async move {
                                purge(
                                    ctx,
                                    &plan.catalog,
                                    &plan.database,
                                    &plan.table,
                                    plan.limit.segment_limit,
                                    None,
                                )
                                .await
                            }),
                            Err(error_code) => Err(error_code.clone()),
                        },
                    );
                }
                build_res.main_pipeline.add_lock_guard(lock_guard);
                Ok(build_res)
            }
            Err(e) if e.code() == ErrorCode::NO_NEED_TO_COMPACT => {
                if self.need_purge {
                    purge(
                        self.ctx.clone(),
                        &plan.catalog,
                        &plan.database,
                        &plan.table,
                        plan.limit.segment_limit,
                        None,
                    )
                    .await?;
                }
                Ok(PipelineBuildResult::create())
            }
            Err(e) => Err(e),
        }
    }
}
