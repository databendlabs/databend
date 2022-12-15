// Copyright 2021 Datafuse Labs.
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

use common_catalog::table::CompactTarget;
use common_exception::Result;
use common_sql::plans::OptimizeTableAction;
use common_sql::plans::OptimizeTablePlan;

use crate::interpreters::Interpreter;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::pipelines::Pipeline;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct OptimizeTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: OptimizeTablePlan,
}

impl OptimizeTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: OptimizeTablePlan) -> Result<Self> {
        Ok(OptimizeTableInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for OptimizeTableInterpreter {
    fn name(&self) -> &str {
        "OptimizeTableInterpreter"
    }

    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let plan = &self.plan;
        let ctx = self.ctx.clone();
        let mut table = self
            .ctx
            .get_table(&plan.catalog, &plan.database, &plan.table)
            .await?;

        let action = &plan.action;
        let do_purge = matches!(
            action,
            OptimizeTableAction::Purge | OptimizeTableAction::All
        );
        let do_compact_blocks = matches!(
            action,
            OptimizeTableAction::CompactBlocks(_) | OptimizeTableAction::All
        );

        let do_compact_segments_only = matches!(action, OptimizeTableAction::CompactSegments(_));

        let limit_opt = match action {
            OptimizeTableAction::CompactBlocks(limit_opt) => *limit_opt,
            OptimizeTableAction::CompactSegments(limit_opt) => *limit_opt,
            _ => None,
        };

        if do_compact_segments_only {
            let mut pipeline = Pipeline::create();
            table
                .compact(
                    ctx.clone(),
                    CompactTarget::Segments,
                    limit_opt,
                    &mut pipeline,
                )
                .await?;

            return Ok(PipelineBuildResult::create());
        }

        if do_compact_blocks {
            let mut pipeline = Pipeline::create();

            if table
                .compact(ctx.clone(), CompactTarget::Blocks, limit_opt, &mut pipeline)
                .await?
            {
                let settings = ctx.get_settings();
                pipeline.set_max_threads(settings.get_max_threads()? as usize);
                let query_id = ctx.get_id();
                let executor_settings = ExecutorSettings::try_create(&settings, query_id)?;
                let executor = PipelineCompleteExecutor::try_create(pipeline, executor_settings)?;

                ctx.set_executor(Arc::downgrade(&executor.get_inner()));
                executor.execute()?;
                drop(executor);
            }

            if do_purge {
                // currently, context caches the table, we have to "refresh"
                // the table by using the catalog API directly
                table = self
                    .ctx
                    .get_catalog(&plan.catalog)?
                    .get_table(ctx.get_tenant().as_str(), &plan.database, &plan.table)
                    .await?;
            }
        }

        if do_purge {
            table.purge(self.ctx.clone(), true).await?;
        }

        Ok(PipelineBuildResult::create())
    }
}
