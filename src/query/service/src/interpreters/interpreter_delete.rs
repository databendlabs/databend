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
use common_catalog::table::DeletionFilters;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::DataBlock;
use common_expression::ROW_ID_COL_NAME;
use common_functions::BUILTIN_FUNCTIONS;
use common_sql::executor::cast_expr_to_non_null_boolean;
use common_sql::optimizer::CascadesOptimizer;
use common_sql::optimizer::HeuristicOptimizer;
use common_sql::optimizer::SExpr;
use common_sql::optimizer::DEFAULT_REWRITE_RULES;
use common_sql::plans::BoundColumnRef;
use common_sql::plans::ConstantExpr;
use common_sql::plans::EvalScalar;
use common_sql::plans::FunctionCall;
use common_sql::plans::RelOperator;
use common_sql::plans::ScalarItem;
use common_sql::plans::SubqueryDesc;
use common_sql::BindContext;
use common_sql::ColumnBinding;
use common_sql::MetadataRef;
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

        let selection = if !self.plan.subquery_desc.is_empty() {
            let support_row_id = tbl.support_row_id_column();
            if !support_row_id {
                return Err(ErrorCode::from_string(
                    "table doesn't support row_id, so it can't use delete with subquery"
                        .to_string(),
                ));
            }
            let table_index = self.plan.metadata.read().get_table_index(
                Some(self.plan.database_name.as_str()),
                self.plan.table_name.as_str(),
            );
            let row_id_column_binding = ColumnBinding {
                database_name: Some(self.plan.database_name.clone()),
                table_name: Some(self.plan.table_name.clone()),
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

        let (filters, col_indices) = if let Some(scalar) = selection {
            // prepare the filter expression
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

            // prepare the inverse filter expression
            let inverted_filter = {
                let inverse = ScalarExpr::FunctionCall(common_sql::planner::plans::FunctionCall {
                    span: None,
                    func_name: "not".to_string(),
                    params: vec![],
                    arguments: vec![scalar.clone()],
                });
                cast_expr_to_non_null_boolean(
                    inverse
                        .as_expr()?
                        .project_column_ref(|col| col.column_name.clone()),
                )?
                .as_remote_expr()
            };

            let col_indices: Vec<usize> = if !self.plan.subquery_desc.is_empty() {
                let mut col_indices = HashSet::new();
                for subquery_desc in &self.plan.subquery_desc {
                    col_indices.extend(subquery_desc.outer_columns.iter());
                }
                col_indices.into_iter().collect()
            } else {
                scalar.used_columns().into_iter().collect()
            };
            (
                Some(DeletionFilters {
                    filter,
                    inverted_filter,
                }),
                col_indices,
            )
        } else {
            (None, vec![])
        };

        let mut build_res = PipelineBuildResult::create();
        tbl.delete(
            self.ctx.clone(),
            filters,
            col_indices,
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

pub async fn subquery_filter(
    ctx: Arc<QueryContext>,
    metadata: MetadataRef,
    row_id_column_binding: &ColumnBinding,
    subquery_desc: &SubqueryDesc,
) -> Result<ScalarExpr> {
    // Select `_row_id` column
    let input_expr = subquery_desc.input_expr.clone();

    let expr = SExpr::create_unary(
        Arc::new(RelOperator::EvalScalar(EvalScalar {
            items: vec![ScalarItem {
                scalar: ScalarExpr::BoundColumnRef(BoundColumnRef {
                    span: None,
                    column: row_id_column_binding.clone(),
                }),
                index: 0,
            }],
        })),
        Arc::new(input_expr),
    );
    // Optimize expression
    // BindContext is only used by pre_optimize and post_optimize, so we can use a mock one.
    let mock_bind_context = Box::new(BindContext::new());
    let heuristic_optimizer = HeuristicOptimizer::new(
        ctx.get_function_context()?,
        &mock_bind_context,
        metadata.clone(),
    );
    let mut expr = heuristic_optimizer.optimize_expression(&expr, &DEFAULT_REWRITE_RULES)?;
    let mut cascades = CascadesOptimizer::create(ctx.clone(), metadata.clone(), false)?;
    expr = cascades.optimize(expr)?;

    // Create `input_expr` pipeline and execute it to get `_row_id` data block.
    let select_interpreter = SelectInterpreter::try_create(
        ctx.clone(),
        BindContext::new(),
        expr,
        metadata.clone(),
        None,
        false,
    )?;
    // Build physical plan
    let physical_plan = select_interpreter.build_physical_plan().await?;
    // Create pipeline for physical plan
    let pipeline = build_query_pipeline(
        &ctx,
        &[row_id_column_binding.clone()],
        &physical_plan,
        false,
        false,
    )
    .await?;

    // Execute pipeline
    let settings = ctx.get_settings();
    let query_id = ctx.get_id();
    let settings = ExecutorSettings::try_create(&settings, query_id)?;
    let pulling_executor = PipelinePullingExecutor::from_pipelines(pipeline, settings)?;
    ctx.set_executor(pulling_executor.get_inner())?;
    let stream_blocks = PullingExecutorStream::create(pulling_executor)?
        .try_collect::<Vec<DataBlock>>()
        .await?;
    let row_id_array = if !stream_blocks.is_empty() {
        let block = DataBlock::concat(&stream_blocks)?;
        let row_id_col = block.columns()[0]
            .value
            .convert_to_full_column(&DataType::Number(NumberDataType::UInt64), block.num_rows());
        // Make a selection: `_row_id` IN (row_id_col)
        // Construct array function for `row_id_col`
        let mut row_id_array = Vec::with_capacity(row_id_col.len());
        for row_id in row_id_col.iter() {
            let scalar = row_id.to_owned();
            let constant_scalar_expr = ScalarExpr::ConstantExpr(ConstantExpr {
                span: None,
                value: scalar,
            });
            row_id_array.push(constant_scalar_expr);
        }
        row_id_array
    } else {
        vec![]
    };

    let array_raw_expr = ScalarExpr::FunctionCall(FunctionCall {
        span: None,
        func_name: "array".to_string(),
        params: vec![],
        arguments: row_id_array,
    });

    let row_id_expr = ScalarExpr::BoundColumnRef(BoundColumnRef {
        span: None,
        column: row_id_column_binding.clone(),
    });

    Ok(ScalarExpr::FunctionCall(FunctionCall {
        span: None,
        func_name: "contains".to_string(),
        params: vec![],
        arguments: vec![array_raw_expr, row_id_expr],
    }))
}

pub fn replace_subquery(
    filters: &mut VecDeque<ScalarExpr>,
    selection: &mut ScalarExpr,
) -> Result<()> {
    match selection {
        ScalarExpr::FunctionCall(func) => {
            for arg in &mut func.arguments {
                replace_subquery(filters, arg)?;
            }
        }
        ScalarExpr::SubqueryExpr { .. } => {
            let filter = filters.pop_back().unwrap();
            *selection = filter;
        }
        _ => {}
    }
    Ok(())
}
