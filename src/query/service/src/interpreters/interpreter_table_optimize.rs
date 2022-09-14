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

use common_exception::Result;
use common_legacy_planners::OptimizeTableAction;
use common_legacy_planners::OptimizeTablePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::Interpreter;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::pipelines::Pipeline;
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

    async fn execute(&self) -> Result<SendableDataBlockStream> {
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
        let do_compact = matches!(
            action,
            OptimizeTableAction::Compact | OptimizeTableAction::All
        );

        if do_compact {
            let mut pipeline = Pipeline::create();
            let mutator = table
                .compact(ctx.clone(), plan.catalog.clone(), &mut pipeline)
                .await?;

            if let Some(mutator) = mutator {
                let settings = ctx.get_settings();
                pipeline.set_max_threads(settings.get_max_threads()? as usize);
                let query_need_abort = ctx.query_need_abort();
                let executor_settings = ExecutorSettings::try_create(&settings)?;
                let executor = PipelineCompleteExecutor::try_create(
                    query_need_abort,
                    pipeline,
                    executor_settings,
                )?;
                executor.execute()?;
                drop(executor);

                let catalog_name = ctx.get_current_catalog();
                mutator
                    .try_commit(&catalog_name, table.get_table_info())
                    .await?;
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
            table.optimize(self.ctx.clone(), true).await?;
        }

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
    }
}
