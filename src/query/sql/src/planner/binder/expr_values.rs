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
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_pipeline_transforms::processors::Transform;
use indexmap::IndexMap;

use crate::binder::wrap_cast;
use crate::evaluator::BlockOperator;
use crate::evaluator::CompoundBlockOperator;
use crate::plans::walk_expr_mut;
use crate::plans::ConstantExpr;
use crate::plans::VisitorMut;
use crate::BindContext;
use crate::MetadataRef;
use crate::ScalarBinder;
use crate::ScalarExpr;

struct ExprValuesRewriter {
    ctx: Arc<dyn TableContext>,
    scalars: Vec<ScalarExpr>,
}

impl ExprValuesRewriter {
    pub fn new(ctx: Arc<dyn TableContext>) -> Self {
        Self {
            ctx,
            scalars: vec![],
        }
    }

    pub fn reset_scalars(&mut self) {
        self.scalars.clear();
    }

    pub fn scalars(&self) -> &[ScalarExpr] {
        &self.scalars
    }
}

impl<'a> VisitorMut<'a> for ExprValuesRewriter {
    fn visit(&mut self, expr: &'a mut ScalarExpr) -> Result<()> {
        if let ScalarExpr::AsyncFunctionCall(async_func) = &expr {
            let catalog = self.ctx.get_default_catalog()?;
            let value = databend_common_base::runtime::block_on(async move {
                async_func
                    .function
                    .generate(catalog.clone(), async_func)
                    .await
            })?;

            *expr = ScalarExpr::ConstantExpr(ConstantExpr {
                span: async_func.span,
                value,
            });
        }

        if matches!(
            expr,
            ScalarExpr::WindowFunction(_)
                | ScalarExpr::AggregateFunction(_)
                | ScalarExpr::UDFCall(_)
        ) {
            self.scalars.push(expr.clone());
            return Ok(());
        }

        walk_expr_mut(self, expr)
    }
}

impl BindContext {
    pub async fn exprs_to_scalar(
        &mut self,
        exprs: &[AExpr],
        schema: &DataSchemaRef,
        ctx: Arc<dyn TableContext>,
        deny_column_reference: bool,
        metadata: MetadataRef,
    ) -> Result<Vec<Scalar>> {
        let schema_fields_len = schema.fields().len();
        if exprs.len() != schema_fields_len {
            return Err(ErrorCode::TableSchemaMismatch(format!(
                "Table columns count is not match, expect {schema_fields_len}, input: {}, expr: {:?}",
                exprs.len(),
                exprs
            )));
        }
        let mut scalar_binder = ScalarBinder::new(
            self,
            ctx.clone(),
            metadata.clone(),
            &[],
            HashMap::new(),
            Box::new(IndexMap::new()),
        )
        .with_deny_column_reference(deny_column_reference);

        let mut map_exprs = Vec::with_capacity(exprs.len());

        // check invalid ScalarExpr
        let mut rewriter = ExprValuesRewriter::new(ctx.clone());
        for (i, expr) in exprs.iter().enumerate() {
            // `DEFAULT` in insert values will be parsed as `Expr::ColumnRef`.
            if let AExpr::ColumnRef { column, .. } = expr {
                if column.column.name().eq_ignore_ascii_case("default") {
                    let field = schema.field(i);
                    map_exprs.push(scalar_binder.get_default_value(field, schema).await?);
                    continue;
                }
            }

            let (mut scalar, data_type) = scalar_binder.bind(expr)?;

            rewriter.reset_scalars();
            rewriter.visit(&mut scalar)?;

            if !rewriter.scalars().is_empty() {
                return Err(ErrorCode::SemanticError(
                    "Aggregate, external udf and window functions are not allowed in value expressions"
                        .to_string(),
                ));
            }

            let target_type = schema.field(i).data_type();
            if data_type != *target_type {
                scalar = wrap_cast(&scalar, target_type);
            }
            let expr = scalar
                .as_expr()?
                .project_column_ref(|col| schema.index_of(&col.index.to_string()).unwrap());
            map_exprs.push(expr);
        }

        let operators = vec![BlockOperator::Map {
            exprs: map_exprs,
            projections: None,
        }];

        let one_row_chunk = DataBlock::new(
            vec![BlockEntry::new(
                DataType::Number(NumberDataType::UInt8),
                Value::Scalar(Scalar::Number(NumberScalar::UInt8(1))),
            )],
            1,
        );
        let func_ctx = ctx.get_function_context()?;
        let mut expression_transform = CompoundBlockOperator {
            operators,
            ctx: func_ctx,
        };
        let res = expression_transform.transform(one_row_chunk)?;
        let scalars: Vec<Scalar> = res
            .columns()
            .iter()
            .skip(1)
            .map(|col| unsafe { col.value.as_ref().index_unchecked(0).to_owned() })
            .collect();
        Ok(scalars)
    }
}
