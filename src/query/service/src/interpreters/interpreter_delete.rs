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

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataSchemaRef;
use common_functions::scalars::BUILTIN_FUNCTIONS;
use common_pipeline_core::Pipeline;
use common_sql::executor::cast_expr_to_non_null_boolean;

use crate::interpreters::Interpreter;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::plans::DeletePlan;

/// interprets DeletePlan
pub struct DeleteInterpreter {
    ctx: Arc<QueryContext>,
    plan: DeletePlan,
}

impl DeleteInterpreter {
    /// Create the DeleteInterpreter from DeletePlan
    pub fn try_create(ctx: Arc<QueryContext>, plan: DeletePlan) -> Result<Self> {
        Ok(DeleteInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DeleteInterpreter {
    /// Get the name of current interpreter
    fn name(&self) -> &str {
        "DeleteInterpreter"
    }

    /// Get the schema of DeletePlan
    fn schema(&self) -> DataSchemaRef {
        self.plan.schema()
    }

    #[tracing::instrument(level = "debug", name = "delete_interpreter_execute", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let catalog_name = self.plan.catalog_name.as_str();
        let db_name = self.plan.database_name.as_str();
        let tbl_name = self.plan.table_name.as_str();
        let tbl = self.ctx.get_table(catalog_name, db_name, tbl_name).await?;

        let (filter, col_indices) = if let Some(scalar) = &self.plan.selection {
            let filter =
                cast_expr_to_non_null_boolean(scalar.as_expr_with_col_name()?)?.as_remote_expr();

            let expr = filter.as_expr(&BUILTIN_FUNCTIONS);
            if !expr.is_deterministic() {
                return Err(ErrorCode::Unimplemented(
                    "Delete must have deterministic predicate",
                ));
            }

            let col_indices = scalar.used_columns().into_iter().collect();
            (Some(filter), col_indices)
        } else {
            if self.plan.input_expr.is_some() {
                return Err(ErrorCode::Unimplemented(
                    "Delete with subquery isn't supported",
                ));
            }
            (None, vec![])
        };

        let mut pipeline = Pipeline::create();
        tbl.delete(self.ctx.clone(), filter, col_indices, &mut pipeline)
            .await?;
        if !pipeline.is_empty() {
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
