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

use databend_common_catalog::lock::LockTableOption;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_sql::ColumnSet;
use databend_common_sql::MetadataRef;
use databend_common_sql::optimizer::ir::DefaultPhysicalPlanner;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::plans::OptimizeCompactBlock;
use databend_storages_common_table_meta::table::is_fuse_backed_engine;

use crate::interpreters::Interpreter;
use crate::physical_plans::PhysicalPlanBuilder;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::sessions::TableContextTableAccess;
use crate::sessions::TableContextTableManagement;

pub struct OptimizeCompactBlockInterpreter {
    ctx: Arc<QueryContext>,
    s_expr: SExpr,
    lock_opt: LockTableOption,
}

impl OptimizeCompactBlockInterpreter {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        s_expr: SExpr,
        lock_opt: LockTableOption,
    ) -> Result<Self> {
        Ok(OptimizeCompactBlockInterpreter {
            ctx,
            s_expr,
            lock_opt,
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
    fn execute2(&self) -> futures::future::BoxFuture<'_, Result<PipelineBuildResult>> {
        Box::pin(async move {
            let OptimizeCompactBlock {
                catalog,
                database,
                table,
                ..
            } = self.s_expr.plan().clone().try_into()?;

            // MV tables reject ordinary writes but allow maintenance. Resolve the current
            // table directly and use the maintenance read-only check, rather than the generic
            // write-lock helper which deliberately skips these engines.
            let target = self
                .ctx
                .get_catalog(&catalog)
                .await?
                .get_table(&self.ctx.get_tenant(), &database, &table)
                .await?;
            let lock_guard = if is_fuse_backed_engine(target.engine())
                && !target.is_read_only_for_maintenance()
                && !target.is_temp()
            {
                self.ctx
                    .clone()
                    .acquire_table_lock_by_id(&catalog, target.get_id(), &self.lock_opt)
                    .await?
            } else {
                None
            };
            if lock_guard.is_some() {
                self.ctx
                    .evict_table_from_cache(&catalog, &database, &table)?;
            }

            let mut build_res = PipelineBuildResult::create();
            let mut builder =
                PhysicalPlanBuilder::new(MetadataRef::default(), self.ctx.clone(), false);
            match builder
                .build(
                    &DefaultPhysicalPlanner::plan(&self.s_expr)?,
                    ColumnSet::new(),
                )
                .await
            {
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

            Ok(build_res)
        })
    }
}
