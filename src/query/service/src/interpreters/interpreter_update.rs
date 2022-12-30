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

use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_pipeline_core::Pipeline;

use crate::interpreters::Interpreter;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::executor::ExpressionBuilderWithoutRenaming;
use crate::sql::plans::ScalarExpr;
use crate::sql::plans::UpdatePlan;

/// interprets UpdatePlan
pub struct UpdateInterpreter {
    ctx: Arc<QueryContext>,
    plan: UpdatePlan,
}

impl UpdateInterpreter {
    /// Create the UpdateInterpreter from UpdatePlan
    pub fn try_create(ctx: Arc<QueryContext>, plan: UpdatePlan) -> Result<Self> {
        Ok(UpdateInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for UpdateInterpreter {
    /// Get the name of current interpreter
    fn name(&self) -> &str {
        "UpdateInterpreter"
    }

    /// Get the schema of UpdatePlan
    fn schema(&self) -> DataSchemaRef {
        self.plan.schema()
    }

    #[tracing::instrument(level = "debug", name = "update_interpreter_execute", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        // TODO check privilege
        let catalog_name = self.plan.catalog.as_str();
        let db_name = self.plan.database.as_str();
        let tbl_name = self.plan.table.as_str();
        let tbl = self.ctx.get_table(catalog_name, db_name, tbl_name).await?;

        let eb = ExpressionBuilderWithoutRenaming::create(self.plan.metadata.clone());
        let (filter, col_indices) = if let Some(scalar) = &self.plan.selection {
            (
                Some(eb.build(scalar)?),
                scalar.used_columns().into_iter().collect(),
            )
        } else {
            (None, vec![])
        };

        let update_list = self.plan.update_list.iter().try_fold(
            Vec::with_capacity(self.plan.update_list.len()),
            |mut acc, (id, scalar)| {
                let expr = eb.build(scalar)?;
                acc.push((*id, expr));
                Ok::<_, ErrorCode>(acc)
            },
        )?;

        let mut pipeline = Pipeline::create();
        tbl.update(
            self.ctx.clone(),
            filter,
            col_indices,
            update_list,
            &mut pipeline,
        )
        .await?;
        if !pipeline.pipes.is_empty() {
            let settings = self.ctx.get_settings();
            pipeline.set_max_threads(settings.get_max_threads()? as usize);
            let query_id = self.ctx.get_id();
            let executor_settings = ExecutorSettings::try_create(&settings, query_id)?;
            let executor = PipelineCompleteExecutor::try_create(pipeline, executor_settings)?;

            self.ctx.set_executor(Arc::downgrade(&executor.get_inner()));
            executor.execute()?;
            drop(executor);
        }

        Ok(PipelineBuildResult::create())
    }
}
