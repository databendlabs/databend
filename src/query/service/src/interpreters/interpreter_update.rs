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
use std::collections::VecDeque;
use std::sync::Arc;

use common_base::runtime::GlobalIORuntime;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::DataBlock;
use common_expression::ROW_ID_COL_NAME;
use common_license::license_manager::get_license_manager;
use common_sql::executor::cast_expr_to_non_null_boolean;
use common_sql::optimizer::CascadesOptimizer;
use common_sql::optimizer::HeuristicOptimizer;
use common_sql::optimizer::SExpr;
use common_sql::optimizer::SubqueryRewriter;
use common_sql::optimizer::DEFAULT_REWRITE_RULES;
use common_sql::plans::EvalScalar as PlanEvalScalar;
use common_sql::plans::RelOperator::EvalScalar;
use common_sql::plans::ScalarItem;
use common_sql::ColumnBinding;
use common_sql::ScalarExpr;
use common_sql::Visibility;
use futures_util::TryStreamExt;
use table_lock::TableLockHandlerWrapper;

use crate::interpreters::common::check_deduplicate_label;
use crate::interpreters::interpreter_delete::replace_subquery;
use crate::interpreters::interpreter_delete::subquery_filter;
use crate::interpreters::Interpreter;
use crate::interpreters::SelectInterpreter;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelinePullingExecutor;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::plans::UpdatePlan;
use crate::stream::PullingExecutorStream;

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

    #[tracing::instrument(level = "debug", name = "update_interpreter_execute", skip(self), fields(ctx.id = self.ctx.get_id().as_str()))]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        if check_deduplicate_label(self.ctx.clone()).await? {
            return Ok(PipelineBuildResult::create());
        }

        let catalog_name = self.plan.catalog.as_str();
        let db_name = self.plan.database.as_str();
        let tbl_name = self.plan.table.as_str();

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

        let selection = if !self.plan.subquery_desc.is_empty() {
            let support_row_id = tbl.support_row_id_column();
            if !support_row_id {
                return Err(ErrorCode::from_string(
                    "table doesn't support row_id, so it can't use delete with subquery"
                        .to_string(),
                ));
            }
            let table_index = self
                .plan
                .metadata
                .read()
                .get_table_index(Some(self.plan.database.as_str()), self.plan.table.as_str());
            let row_id_column_binding = ColumnBinding {
                database_name: Some(self.plan.database.clone()),
                table_name: Some(self.plan.table.clone()),
                column_position: None,
                table_index,
                column_name: ROW_ID_COL_NAME.to_string(),
                index: self.plan.subquery_desc[0].index,
                data_type: Box::new(DataType::Number(NumberDataType::UInt64)),
                visibility: Visibility::InVisible,
                virtual_computed_expr: None,
            };
            let mut filters = VecDeque::new();
            for subquery_desc in &self.plan.subquery_desc {
                let filter = subquery_filter(
                    self.ctx.clone(),
                    self.plan.metadata.clone(),
                    &row_id_column_binding,
                    subquery_desc,
                )
                .await?;
                filters.push_front(filter);
            }
            // Traverse `selection` and put `filters` into `selection`.
            let mut selection = self.plan.selection.clone().unwrap();
            replace_subquery(&mut filters, &mut selection)?;
            Some(selection)
        } else {
            self.plan.selection.clone()
        };

        let (filter, col_indices) = if let Some(scalar) = selection {
            let filter = cast_expr_to_non_null_boolean(
                scalar
                    .as_expr()?
                    .project_column_ref(|col| col.column_name.clone()),
            )?
            .as_remote_expr();
            let col_indices: Vec<usize> = if !self.plan.subquery_desc.is_empty() {
                let mut col_indices = HashSet::new();
                for subquery_desc in &self.plan.subquery_desc {
                    col_indices.extend(subquery_desc.outer_columns.iter());
                }
                col_indices.into_iter().collect()
            } else {
                scalar.used_columns().into_iter().collect()
            };
            (Some(filter), col_indices)
        } else {
            (None, vec![])
        };

        let mut subquery_scalars = vec![];
        let mut update_list = self.plan.generate_update_list(
            self.ctx.clone(),
            tbl.schema().into(),
            col_indices.clone(),
            &mut subquery_scalars,
        )?;

        for scalar in subquery_scalars.iter() {
            if let ScalarExpr::SubqueryExpr(subquery_expr) = scalar {
                if subquery_expr.data_type() == DataType::Nullable(Box::new(DataType::Boolean)) {
                    return Err(ErrorCode::from_string(
                        "
                        subquery type in update list should be scalar subquery"
                            .to_string(),
                    ));
                }
                let mut rewriter = SubqueryRewriter::new(self.plan.metadata.clone());
                let expr = if subquery_expr.outer_columns.is_empty() {
                    // Uncorrelated subquery
                    rewriter.rewrite(subquery_expr.subquery.as_ref())?
                } else {
                    // Correlated subquery
                    let eval_scalar = EvalScalar(PlanEvalScalar {
                        items: vec![ScalarItem {
                            scalar: scalar.clone(),
                            index: 0,
                        }],
                    });
                    let subquery_expr = SExpr::create_unary(
                        Arc::new(eval_scalar),
                        Arc::new(self.plan.table_expr.clone()),
                    );
                    rewriter.rewrite(&subquery_expr)?
                };
                // Optimize expression
                // BindContext is only used by pre_optimize and post_optimize, so we can use a mock one.
                let heuristic_optimizer = HeuristicOptimizer::new(
                    self.ctx.get_function_context()?,
                    self.plan.bind_context.clone(),
                    self.plan.metadata.clone(),
                );
                let mut expr =
                    heuristic_optimizer.optimize_expression(&expr, &DEFAULT_REWRITE_RULES)?;
                let mut cascades =
                    CascadesOptimizer::create(self.ctx.clone(), self.plan.metadata.clone(), false)?;
                expr = cascades.optimize(expr)?;

                // Create `input_expr` pipeline and execute it to get `` data block.
                let select_interpreter = SelectInterpr_row_ideter::try_create(
                    self.ctx.clone(),
                    self.plan.bind_context.as_ref().clone(),
                    expr,
                    self.plan.metadata.clone(),
                    None,
                    false,
                )?;
                // Build physical plan
                let physical_plan = select_interpreter.build_physical_plan().await?;
                // Create pipeline for physical plan
                let pipeline = build_query_pipeline(
                    &self.ctx,
                    &[subquery_expr.output_column.clone()],
                    &physical_plan,
                    false,
                    false,
                )
                .await?;

                // Execute pipeline
                let settings = self.ctx.get_settings();
                let query_id = self.ctx.get_id();
                let settings = ExecutorSettings::try_create(&settings, query_id)?;
                let pulling_executor = PipelinePullingExecutor::from_pipelines(pipeline, settings)?;
                self.ctx.set_executor(pulling_executor.get_inner())?;
                let stream_blocks = PullingExecutorStream::create(pulling_executor)?
                    .try_collect::<Vec<DataBlock>>()
                    .await?;
                dbg!(stream_blocks[0].columns()[0].value.as_ref().len());
            }
        }

        let computed_list = self
            .plan
            .generate_stored_computed_list(self.ctx.clone(), Arc::new(tbl.schema().into()))?;

        if !computed_list.is_empty() {
            let license_manager = get_license_manager();
            license_manager.manager.check_enterprise_enabled(
                &self.ctx.get_settings(),
                self.ctx.get_tenant(),
                "update_computed_column".to_string(),
            )?;
        }

        let mut build_res = PipelineBuildResult::create();
        tbl.update(
            self.ctx.clone(),
            filter,
            col_indices,
            update_list,
            computed_list,
            !self.plan.subquery_desc.is_empty(),
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
