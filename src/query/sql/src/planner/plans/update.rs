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

use std::collections::HashMap;
use std::sync::Arc;

use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::ComputedExpr;
use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_expression::Expr;
use common_expression::FieldIndex;
use common_expression::RemoteExpr;

use crate::parse_computed_exprs;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::FunctionCall;
use crate::plans::ScalarExpr;
use crate::BindContext;
use crate::ColumnBinding;
use crate::Visibility;

#[derive(Clone, Debug)]
pub struct UpdatePlan {
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub update_list: HashMap<FieldIndex, ScalarExpr>,
    pub selection: Option<ScalarExpr>,
    pub bind_context: Box<BindContext>,
}

impl UpdatePlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }

    pub fn generate_update_list(
        &self,
        schema: DataSchema,
        col_indices: Vec<usize>,
    ) -> Result<Vec<(FieldIndex, RemoteExpr<String>)>> {
        let predicate = ScalarExpr::BoundColumnRef(BoundColumnRef {
            span: None,
            column: ColumnBinding {
                database_name: None,
                table_name: None,
                table_index: None,
                column_name: "_predicate".to_string(),
                index: schema.num_fields(),
                data_type: Box::new(DataType::Boolean),
                visibility: Visibility::Visible,
                virtual_computed_expr: None,
            },
        });

        self.update_list.iter().try_fold(
            Vec::with_capacity(self.update_list.len()),
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
                    for column_binding in self.bind_context.columns.iter() {
                        if BindContext::match_column_binding(
                            Some(&self.database),
                            Some(&self.table),
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
                acc.push((
                    *index,
                    scalar
                        .as_expr()?
                        .project_column_ref(|col| col.column_name.clone())
                        .as_remote_expr(),
                ));
                Ok::<_, ErrorCode>(acc)
            },
        )
    }

    pub fn generate_stored_computed_list(
        &self,
        ctx: Arc<dyn TableContext>,
        schema: DataSchemaRef,
    ) -> Result<Vec<(FieldIndex, RemoteExpr<String>)>> {
        let mut remote_exprs = Vec::new();
        for (i, f) in schema.fields().iter().enumerate() {
            if let Some(ComputedExpr::Stored(stored_expr)) = f.computed_expr() {
                let mut expr = parse_computed_exprs(ctx.clone(), schema.clone(), stored_expr)?;
                let mut expr = expr.remove(0);
                if expr.data_type() != f.data_type() {
                    expr = Expr::Cast {
                        span: None,
                        is_try: f.data_type().is_nullable(),
                        expr: Box::new(expr),
                        dest_type: f.data_type().clone(),
                    };
                }

                // If related column has updated, the stored computed column need to regenerate.
                let mut need_update = false;
                let column_ids = expr.column_refs();
                for (column_id, _) in column_ids.iter() {
                    if self.update_list.contains_key(&column_id) {
                        need_update = true;
                        break;
                    }
                }
                if need_update {
                    let remote_expr = expr
                        .project_column_ref(|index| schema.field(*index).name().to_string())
                        .as_remote_expr();

                    remote_exprs.push((i, remote_expr));
                }
            }
        }
        Ok(remote_exprs)
    }
}
