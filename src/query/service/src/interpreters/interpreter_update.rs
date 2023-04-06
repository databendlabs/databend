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
use common_expression::types::DataType;
use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_sql::executor::cast_expr_to_non_null_boolean;
use common_sql::plans::BoundColumnRef;
use common_sql::plans::CastExpr;
use common_sql::plans::FunctionCall;
use common_sql::BindContext;
use common_sql::ColumnBinding;
use common_sql::ScalarExpr;
use common_sql::Visibility;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
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
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let catalog_name = self.plan.catalog.as_str();
        let db_name = self.plan.database.as_str();
        let tbl_name = self.plan.table.as_str();
        let tbl = self.ctx.get_table(catalog_name, db_name, tbl_name).await?;

        let (filter, col_indices) = if let Some(scalar) = &self.plan.selection {
            let filter =
                cast_expr_to_non_null_boolean(scalar.as_expr_with_col_name()?)?.as_remote_expr();
            let col_indices = scalar.used_columns().into_iter().collect();
            (Some(filter), col_indices)
        } else {
            (None, vec![])
        };

        let predicate = ScalarExpr::BoundColumnRef(BoundColumnRef {
            span: None,
            column: ColumnBinding {
                database_name: None,
                table_name: None,
                table_index: None,
                column_name: "_predicate".to_string(),
                index: tbl.schema().num_fields(),
                data_type: Box::new(DataType::Boolean),
                visibility: Visibility::Visible,
            },
        });

        let schema: DataSchema = tbl.schema().into();
        let update_list = self.plan.update_list.iter().try_fold(
            Vec::with_capacity(self.plan.update_list.len()),
            |mut acc, (index, scalar)| {
                let field = schema.field(*index);
                let left = ScalarExpr::CastExpr(CastExpr {
                    span: scalar.span(),
                    is_try: false,
                    argument: Box::new(scalar.clone()),
                    target_type: Box::new(field.data_type().clone()),
                });
                let scalar = if col_indices.is_empty() {
                    // The condition is always true.
                    // Replace column to the result of the following expression:
                    // CAST(expression, type)
                    left
                } else {
                    // Replace column to the result of the following expression:
                    // if(condition, CAST(expression, type), column)
                    let mut right = None;
                    for column_binding in self.plan.bind_context.columns.iter() {
                        if BindContext::match_column_binding(
                            Some(db_name),
                            Some(tbl_name),
                            field.name(),
                            column_binding,
                        ) {
                            right = Some(ScalarExpr::BoundColumnRef(BoundColumnRef {
                                span: None,
                                column: column_binding.clone(),
                            }));
                            break;
                        }
                    }
                    let right = right.ok_or_else(|| ErrorCode::Internal("It's a bug"))?;
                    ScalarExpr::FunctionCall(FunctionCall {
                        span: None,
                        func_name: "if".to_string(),
                        params: vec![],
                        arguments: vec![predicate.clone(), left, right],
                    })
                };
                acc.push((*index, scalar.as_expr_with_col_name()?.as_remote_expr()));
                Ok::<_, ErrorCode>(acc)
            },
        )?;

        let mut build_res = PipelineBuildResult::create();
        tbl.update(
            self.ctx.clone(),
            filter,
            col_indices,
            update_list,
            &mut build_res.main_pipeline,
        )
        .await?;
        Ok(build_res)
    }
}
