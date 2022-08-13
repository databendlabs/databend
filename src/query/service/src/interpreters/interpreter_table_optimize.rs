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
use common_planners::OptimizeTableAction;
use common_planners::OptimizeTablePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::interpreters::Interpreter;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::pipelines::Pipeline;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::storages::fuse::FuseTable;
pub struct OptimizeTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: OptimizeTablePlan,
}

impl OptimizeTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: OptimizeTablePlan) -> Result<Self> {
        Ok(OptimizeTableInterpreter { ctx, plan })
    }

    #[tracing::instrument(level = "debug", name = "execute_recluster", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute_recluster(
        &self,
        catalog_name: &String,
        db_name: &String,
        tbl_name: &String,
    ) -> Result<()> {
        let ctx = self.ctx.clone();
        let settings = ctx.get_settings();

        let mut pipeline = Pipeline::create();

        let table = ctx.get_table(catalog_name, db_name, tbl_name).await?;
        let tbl = FuseTable::try_from_table(table.as_ref())?;

        let mutator = tbl.try_get_recluster_mutator(ctx.clone()).await?;
        let mut mutator = if let Some(mutator) = mutator {
            mutator
        } else {
            return Ok(());
        };

        let need_recluster = mutator.blocks_select().await?;

        if !need_recluster {
            return Ok(());
        }

        tbl.do_recluster(
            ctx.clone(),
            catalog_name.clone(),
            mutator.clone(),
            &mut pipeline,
        )?;

        pipeline.set_max_threads(settings.get_max_threads()? as usize);

        let async_runtime = ctx.get_storage_runtime();
        let query_need_abort = ctx.query_need_abort();
        let executor =
            PipelineCompleteExecutor::try_create(async_runtime, query_need_abort, pipeline)?;
        executor.execute()?;
        drop(executor);

        let catalog_name = ctx.get_current_catalog();
        mutator.commit_recluster(&catalog_name).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Interpreter for OptimizeTableInterpreter {
    fn name(&self) -> &str {
        "OptimizeTableInterpreter"
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        let plan = &self.plan;

        let action = &plan.action;
        let do_purge = matches!(
            action,
            OptimizeTableAction::Purge | OptimizeTableAction::All
        );
        let do_compact = matches!(
            action,
            OptimizeTableAction::Compact | OptimizeTableAction::All
        );
        let do_recluster = matches!(
            action,
            OptimizeTableAction::Recluster | OptimizeTableAction::All
        );

        if do_recluster {
            self.execute_recluster(&plan.catalog, &plan.database, &plan.table)
                .await?;
        }

        let mut table = self
            .ctx
            .get_table(&plan.catalog, &plan.database, &plan.table)
            .await?;

        if do_compact {
            // TODO(zhyass): clustering key.
            table.compact(self.ctx.clone(), self.plan.clone()).await?;
            if do_purge {
                // currently, context caches the table, we have to "refresh"
                // the table by using the catalog API directly
                let tenant = self.ctx.get_tenant();
                table = self
                    .ctx
                    .get_catalog(&plan.catalog)?
                    .get_table(tenant.as_str(), &plan.database, &plan.table)
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
