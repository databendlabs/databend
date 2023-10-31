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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::ComputedExpr;
use common_expression::ConstantFolder;
use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_expression::Expr;
use common_expression::FieldIndex;
use common_expression::RemoteExpr;
use common_functions::BUILTIN_FUNCTIONS;

use crate::binder::wrap_cast_scalar;
use crate::binder::ColumnBindingBuilder;
use crate::parse_computed_expr;
use crate::plans::BoundColumnRef;
use crate::plans::FunctionCall;
use crate::plans::ScalarExpr;
use crate::plans::SubqueryDesc;
use crate::BindContext;
use crate::MetadataRef;
use crate::Visibility;

pub const PREDICATE_COLUMN_NAME: &str = "_predicate";

#[derive(Clone, Debug)]
pub struct UpdatePlan {
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub update_list: HashMap<FieldIndex, ScalarExpr>,
    pub selection: Option<ScalarExpr>,
    pub bind_context: Box<BindContext>,
    pub metadata: MetadataRef,
    pub subquery_desc: Vec<SubqueryDesc>,
}

impl UpdatePlan {
    pub fn schema(&self) -> DataSchemaRef {
        Arc::new(DataSchema::empty())
    }

    pub fn generate_update_list(
        &self,
        ctx: Arc<dyn TableContext>,
        schema: DataSchema,
        col_indices: Vec<usize>,
        use_column_name_index: Option<usize>,
        has_alias: bool,
    ) -> Result<Vec<(FieldIndex, RemoteExpr<String>)>> {
        let column = ColumnBindingBuilder::new(
            PREDICATE_COLUMN_NAME.to_string(),
            use_column_name_index.unwrap_or_else(|| schema.num_fields()),
            Box::new(DataType::Boolean),
            Visibility::Visible,
        )
        .build();
        let predicate = ScalarExpr::BoundColumnRef(BoundColumnRef { span: None, column });

        self.update_list.iter().try_fold(
            Vec::with_capacity(self.update_list.len()),
            |mut acc, (index, scalar)| {
                let field = schema.field(*index);
                let data_type = scalar.data_type()?;
                let target_type = field.data_type();
                let left = wrap_cast_scalar(scalar, &data_type, target_type)?;

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
                            if has_alias {
                                None
                            } else {
                                Some(&self.database)
                            },
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

                    let mut right = right.ok_or_else(|| ErrorCode::Internal("It's a bug"))?;
                    let right_data_type = right.data_type()?;

                    // corner case: for merge into, if target_table's fields are not null, when after bind_join, it will
                    // change into nullable, so we need to cast this.
                    right = wrap_cast_scalar(&right, &right_data_type, target_type)?;

                    ScalarExpr::FunctionCall(FunctionCall {
                        span: None,
                        func_name: "if".to_string(),
                        params: vec![],
                        arguments: vec![predicate.clone(), left, right],
                    })
                };
                let expr = scalar.as_expr()?.project_column_ref(|col| {
                    if use_column_name_index.is_none() {
                        col.column_name.clone()
                    } else {
                        col.index.to_string()
                    }
                });
                let (expr, _) =
                    ConstantFolder::fold(&expr, &ctx.get_function_context()?, &BUILTIN_FUNCTIONS);
                acc.push((*index, expr.as_remote_expr()));
                Ok::<_, ErrorCode>(acc)
            },
        )
    }

    pub fn generate_stored_computed_list(
        &self,
        ctx: Arc<dyn TableContext>,
        schema: DataSchemaRef,
    ) -> Result<BTreeMap<FieldIndex, RemoteExpr<String>>> {
        let mut remote_exprs = BTreeMap::new();
        for (i, f) in schema.fields().iter().enumerate() {
            if let Some(ComputedExpr::Stored(stored_expr)) = f.computed_expr() {
                let mut expr = parse_computed_expr(ctx.clone(), schema.clone(), stored_expr)?;
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
                let field_indices = expr.column_refs();
                for (field_index, _) in field_indices.iter() {
                    if self.update_list.contains_key(field_index) {
                        need_update = true;
                        break;
                    }
                }
                if need_update {
                    let expr =
                        expr.project_column_ref(|index| schema.field(*index).name().to_string());
                    let (expr, _) = ConstantFolder::fold(
                        &expr,
                        &ctx.get_function_context()?,
                        &BUILTIN_FUNCTIONS,
                    );
                    remote_exprs.insert(i, expr.as_remote_expr());
                }
            }
        }
        Ok(remote_exprs)
    }
}
