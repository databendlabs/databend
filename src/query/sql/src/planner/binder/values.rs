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

use databend_common_ast::ast::Expr as AExpr;
use databend_common_ast::Span;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::type_check::common_super_type;
use databend_common_expression::types::DataType;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::Evaluator;
use databend_common_functions::BUILTIN_FUNCTIONS;
use indexmap::IndexMap;

use crate::binder::wrap_cast;
use crate::optimizer::ColumnSet;
use crate::optimizer::SExpr;
use crate::plans::BoundColumnRef;
use crate::plans::ConstantTableScan;
use crate::BindContext;
use crate::Binder;
use crate::ColumnBindingBuilder;
use crate::MetadataRef;
use crate::NameResolutionContext;
use crate::ScalarBinder;
use crate::ScalarExpr;
use crate::Visibility;

impl Binder {
    #[async_backtrace::framed]
    pub(crate) async fn bind_values(
        &mut self,
        bind_context: &mut BindContext,
        span: Span,
        values: &[Vec<AExpr>],
    ) -> Result<(SExpr, BindContext)> {
        bind_values(
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            bind_context,
            span,
            values,
        )
        .await
    }
}

pub async fn bind_values(
    ctx: Arc<dyn TableContext>,
    name_resolution_ctx: &NameResolutionContext,
    metadata: MetadataRef,
    bind_context: &mut BindContext,
    span: Span,
    values: &[Vec<AExpr>],
) -> Result<(SExpr, BindContext)> {
    if values.is_empty() {
        return Err(ErrorCode::SemanticError(
            "Values lists must have at least one row".to_string(),
        )
        .set_span(span));
    }
    let same_length = values.windows(2).all(|v| v[0].len() == v[1].len());
    if !same_length {
        return Err(ErrorCode::SemanticError(
            "Values lists must all be the same length".to_string(),
        )
        .set_span(span));
    }

    let num_rows = values.len();
    let num_cols = values[0].len();

    // assigns default column names col0, col1, etc.
    let names = (0..num_cols)
        .map(|i| format!("col{}", i))
        .collect::<Vec<_>>();

    let mut scalar_binder = ScalarBinder::new(
        bind_context,
        ctx.clone(),
        name_resolution_ctx,
        metadata.clone(),
        &[],
        HashMap::new(),
        Box::new(IndexMap::new()),
    );

    let mut col_scalars = vec![Vec::with_capacity(values.len()); num_cols];
    let mut common_types: Vec<Option<DataType>> = vec![None; num_cols];

    for row_values in values.iter() {
        for (i, value) in row_values.iter().enumerate() {
            let (scalar, data_type) = scalar_binder.bind(value).await?;
            col_scalars[i].push((scalar, data_type.clone()));

            // Get the common data type for each columns.
            match &common_types[i] {
                Some(common_type) => {
                    if common_type != &data_type {
                        let new_common_type = common_super_type(
                            common_type.clone(),
                            data_type.clone(),
                            &BUILTIN_FUNCTIONS.default_cast_rules,
                        );
                        if new_common_type.is_none() {
                            return Err(ErrorCode::SemanticError(format!(
                                "{} and {} don't have common data type",
                                common_type, data_type
                            ))
                            .set_span(span));
                        }
                        common_types[i] = new_common_type;
                    }
                }
                None => {
                    common_types[i] = Some(data_type);
                }
            }
        }
    }

    let mut value_fields = Vec::with_capacity(names.len());
    for (name, common_type) in names.into_iter().zip(common_types.into_iter()) {
        let value_field = DataField::new(&name, common_type.unwrap());
        value_fields.push(value_field);
    }
    let value_schema = DataSchema::new(value_fields);

    let input = DataBlock::empty();
    let func_ctx = ctx.get_function_context()?;
    let evaluator = Evaluator::new(&input, &func_ctx, &BUILTIN_FUNCTIONS);

    // use values to build columns
    let mut value_columns = Vec::with_capacity(col_scalars.len());
    for (scalars, value_field) in col_scalars.iter().zip(value_schema.fields().iter()) {
        let mut builder = ColumnBuilder::with_capacity(value_field.data_type(), col_scalars.len());
        for (scalar, value_type) in scalars {
            let scalar = if value_type != value_field.data_type() {
                wrap_cast(scalar, value_field.data_type())
            } else {
                scalar.clone()
            };
            let expr = scalar
                .as_expr()?
                .project_column_ref(|col| value_schema.index_of(&col.index.to_string()).unwrap());
            let result = evaluator.run(&expr)?;

            match result.as_scalar() {
                Some(val) => {
                    builder.push(val.as_ref());
                }
                None => {
                    return Err(ErrorCode::SemanticError(format!(
                        "Value must be a scalar, but get {}",
                        result
                    ))
                    .set_span(span));
                }
            }
        }
        value_columns.push(builder.build());
    }

    // add column bindings
    let mut columns = ColumnSet::new();
    let mut fields = Vec::with_capacity(values.len());
    for value_field in value_schema.fields() {
        let index = metadata.read().columns().len();
        columns.insert(index);

        let column_binding = ColumnBindingBuilder::new(
            value_field.name().clone(),
            index,
            Box::new(value_field.data_type().clone()),
            Visibility::Visible,
        )
        .build();
        let _ = metadata.write().add_derived_column(
            value_field.name().clone(),
            value_field.data_type().clone(),
            Some(ScalarExpr::BoundColumnRef(BoundColumnRef {
                span,
                column: column_binding.clone(),
            })),
        );
        bind_context.add_column_binding(column_binding);

        let field = DataField::new(&index.to_string(), value_field.data_type().clone());
        fields.push(field);
    }
    let schema = DataSchemaRefExt::create(fields);

    let s_expr = SExpr::create_leaf(Arc::new(
        ConstantTableScan {
            values: value_columns,
            num_rows,
            schema,
            columns,
        }
        .into(),
    ));

    Ok((s_expr, bind_context.clone()))
}
