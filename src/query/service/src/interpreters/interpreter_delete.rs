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

use common_base::runtime::GlobalIORuntime;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::DataBlock;
use common_expression::ROW_ID_COL_NAME;
use common_functions::BUILTIN_FUNCTIONS;
use common_sql::executor::cast_expr_to_non_null_boolean;
use common_sql::optimizer::SExpr;
use common_sql::plans::BoundColumnRef;
use common_sql::plans::EvalScalar;
use common_sql::plans::RelOperator;
use common_sql::plans::ScalarItem;
use common_sql::BindContext;
use common_sql::ColumnBinding;
use common_sql::ScalarExpr;
use common_sql::Visibility;
use futures_util::TryStreamExt;
use table_lock::TableLockHandlerWrapper;

use crate::interpreters::Interpreter;
use crate::interpreters::SelectInterpreter;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelinePullingExecutor;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::plans::DeletePlan;
use crate::stream::PullingExecutorStream;

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

    #[tracing::instrument(level = "debug", name = "delete_interpreter_execute", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let catalog_name = self.plan.catalog_name.as_str();
        let db_name = self.plan.database_name.as_str();
        let tbl_name = self.plan.table_name.as_str();

        let tbl = self.ctx.get_table(catalog_name, db_name, tbl_name).await?;
        let table_info = tbl.get_table_info().clone();

        // Add table lock heartbeat.
        let handler = TableLockHandlerWrapper::instance(self.ctx.clone());
        let mut heartbeat = handler
            .try_lock(self.ctx.clone(), table_info.clone())
            .await?;

        // refresh table.
        let tbl = self
            .ctx
            .get_catalog(catalog_name)?
            .get_table(self.ctx.get_tenant().as_str(), db_name, tbl_name)
            .await?;

        let (filter, col_indices) = if let Some(scalar) = &self.plan.selection {
            let filter = cast_expr_to_non_null_boolean(
                scalar
                    .as_expr()?
                    .project_column_ref(|col| col.column_name.clone()),
            )?
            .as_remote_expr();

            let expr = filter.as_expr(&BUILTIN_FUNCTIONS);
            if !expr.is_deterministic(&BUILTIN_FUNCTIONS) {
                return Err(ErrorCode::Unimplemented(
                    "Delete must have deterministic predicate",
                ));
            }

            let col_indices = scalar.used_columns().into_iter().collect();
            (Some(filter), col_indices)
        } else {
            if let Some(input_expr) = &self.plan.input_expr {
                // Select `_row_id` column
                let table_index = self.plan.metadata.read().get_table_index(
                    Some(self.plan.database_name.as_str()),
                    self.plan.table_name.as_str(),
                );
                let row_id_col = ColumnBinding {
                    database_name: Some(self.plan.database_name.clone()),
                    table_name: Some(self.plan.table_name.clone()),
                    column_position: None,
                    table_index,
                    column_name: ROW_ID_COL_NAME.to_string(),
                    index: self.plan.metadata.read().columns().len(),
                    data_type: Box::new(DataType::Number(NumberDataType::UInt64)),
                    visibility: Visibility::InVisible,
                };
                let expr = SExpr::create_unary(
                    Arc::new(RelOperator::EvalScalar(EvalScalar {
                        items: vec![ScalarItem {
                            scalar: ScalarExpr::BoundColumnRef(BoundColumnRef {
                                span: None,
                                column: row_id_col.clone(),
                            }),
                            index: 0,
                        }],
                    })),
                    Arc::new(input_expr.clone()),
                );
                // Create `input_expr` pipeline and execute it to get `_row_id` data block.
                let select_interpreter = SelectInterpreter::try_create(
                    self.ctx.clone(),
                    BindContext::new(),
                    expr,
                    self.plan.metadata.clone(),
                    None,
                    false,
                )?;
                // Build physical plan
                let physical_plan = select_interpreter.build_physical_plan().await?;
                // Create pipeline for physical plan
                let pipeline =
                    build_query_pipeline(&self.ctx, &[row_id_col], &physical_plan, false, false)
                        .await?;
                let settings = self.ctx.get_settings();
                let query_id = self.ctx.get_id();
                let settings = ExecutorSettings::try_create(&settings, query_id)?;
                let pulling_executor = PipelinePullingExecutor::from_pipelines(pipeline, settings)?;
                self.ctx.set_executor(pulling_executor.get_inner())?;
                let stream = PullingExecutorStream::create(pulling_executor)?;
                let blocks = stream.try_collect::<Vec<DataBlock>>().await?;
                dbg!(blocks);
            }
            (None, vec![])
        };

        let mut build_res = PipelineBuildResult::create();
        tbl.delete(
            self.ctx.clone(),
            filter,
            col_indices,
            &mut build_res.main_pipeline,
        )
        .await?;

        if build_res.main_pipeline.is_empty() {
            heartbeat.shutdown().await?;
        } else {
            build_res.main_pipeline.set_on_finished(move |may_error| {
                // shutdown table lock heartbeat.
                GlobalIORuntime::instance().block_on(async move { heartbeat.shutdown().await })?;
                match may_error {
                    None => Ok(()),
                    Some(error_code) => Err(error_code.clone()),
                }
            });
        }

        Ok(build_res)
    }
}
