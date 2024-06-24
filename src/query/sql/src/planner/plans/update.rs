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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::type_check::check_cast;
use databend_common_expression::types::DataType;
use databend_common_expression::ComputedExpr;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::FieldIndex;
use databend_common_expression::RemoteExpr;
use databend_common_expression::PREDICATE_COLUMN_NAME;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_core::LockGuard;

use crate::binder::wrap_cast;
use crate::binder::ColumnBindingBuilder;
use crate::parse_computed_expr;
use crate::plans::BoundColumnRef;
use crate::plans::FunctionCall;
use crate::plans::ScalarExpr;
use crate::plans::SubqueryDesc;
use crate::BindContext;
use crate::MetadataRef;
use crate::Visibility;

#[derive(Clone)]
pub struct UpdatePlan {
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub update_list: HashMap<FieldIndex, ScalarExpr>,
    pub selection: Option<ScalarExpr>,
    pub bind_context: Box<BindContext>,
    pub metadata: MetadataRef,
    pub subquery_desc: Vec<SubqueryDesc>,
    pub lock_guard: Option<LockGuard>,
}

impl std::fmt::Debug for UpdatePlan {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("Update")
            .field("catalog", &self.catalog)
            .field("database", &self.database)
            .field("table", &self.table)
            .field("update_list", &self.update_list)
            .field("selection", &self.selection)
            .field("subquery_desc", &self.subquery_desc)
            .finish()
    }
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
    ) -> Result<Vec<(FieldIndex, RemoteExpr<String>)>> {
        generate_update_list(
            ctx,
            &self.bind_context,
            &self.update_list,
            schema,
            col_indices,
            None,
            Some(&self.database),
            &self.table,
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
                let expr = parse_computed_expr(ctx.clone(), schema.clone(), stored_expr)?;
                let expr = check_cast(None, false, expr, f.data_type(), &BUILTIN_FUNCTIONS)?;

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

#[allow(clippy::too_many_arguments)]
pub fn generate_update_list(
    ctx: Arc<dyn TableContext>,
    bind_context: &BindContext,
    update_list: &HashMap<FieldIndex, ScalarExpr>,
    schema: DataSchema,
    col_indices: Vec<usize>,
    use_column_name_index: Option<usize>,
    database: Option<&str>,
    table: &str,
) -> Result<Vec<(FieldIndex, RemoteExpr<String>)>> {
    let column = ColumnBindingBuilder::new(
        PREDICATE_COLUMN_NAME.to_string(),
        use_column_name_index.unwrap_or_else(|| schema.num_fields()),
        Box::new(DataType::Boolean),
        Visibility::Visible,
    )
    .build();
    let predicate = ScalarExpr::BoundColumnRef(BoundColumnRef { span: None, column });

    update_list.iter().try_fold(
        Vec::with_capacity(update_list.len()),
        |mut acc, (index, scalar)| {
            let field = schema.field(*index);
            let data_type = scalar.data_type()?;
            let target_type = field.data_type();
            let left = if data_type != *target_type {
                wrap_cast(scalar, target_type)
            } else {
                scalar.clone()
            };

            let scalar = if col_indices.is_empty() {
                // The condition is always true.
                // Replace column to the result of the following expression:
                // CAST(expression, type)
                left
            } else {
                // Replace column to the result of the following expression:
                // if(condition, CAST(expression, type), column)
                let mut right = None;
                for column_binding in bind_context.columns.iter() {
                    if BindContext::match_column_binding(
                        database,
                        Some(table),
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

                // corner case: for merge into, if target_table's fields are not null, when after bind_join, it will
                // change into nullable, so we need to cast this. but we will do cast after all matched clauses,please
                // see `cast_data_type_for_merge()`.

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
