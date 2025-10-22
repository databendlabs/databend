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

use databend_common_ast::ast::Expr as AExpr;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::UInt8Type;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::FunctionKind;
use databend_common_expression::Scalar;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_users::Object;

use crate::binder::wrap_cast;
use crate::evaluator::BlockOperator;
use crate::plans::walk_expr_mut;
use crate::plans::ConstantExpr;
use crate::plans::VisitorMut;
use crate::BindContext;
use crate::DefaultExprBinder;
use crate::MetadataRef;
use crate::NameResolutionContext;
use crate::ScalarBinder;
use crate::ScalarExpr;

pub(crate) struct ExprValuesRewriter {
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
        match &expr {
            ScalarExpr::AsyncFunctionCall(async_func) => {
                let tenant = self.ctx.get_tenant();
                let catalog = self.ctx.get_default_catalog()?;
                let visibility_checker = if self
                    .ctx
                    .get_settings()
                    .get_enable_experimental_sequence_privilege_check()?
                {
                    let ctx = self.ctx.clone();
                    Some(databend_common_base::runtime::block_on(async move {
                        ctx.get_visibility_checker(false, Object::Sequence).await
                    })?)
                } else {
                    None
                };
                let value = databend_common_base::runtime::block_on(async move {
                    async_func
                        .generate(tenant.clone(), catalog.clone(), visibility_checker)
                        .await
                })?;

                *expr = ScalarExpr::ConstantExpr(ConstantExpr {
                    span: async_func.span,
                    value,
                });
            }
            ScalarExpr::FunctionCall(func) => {
                if BUILTIN_FUNCTIONS
                    .get_property(&func.func_name)
                    .map(|property| property.kind == FunctionKind::SRF)
                    .unwrap_or(false)
                {
                    self.scalars.push(expr.clone());
                    return Ok(());
                }
            }
            ScalarExpr::WindowFunction(_)
            | ScalarExpr::AggregateFunction(_)
            | ScalarExpr::SubqueryExpr(_)
            | ScalarExpr::UDFCall(_)
            | ScalarExpr::UDAFCall(_) => {
                self.scalars.push(expr.clone());
                return Ok(());
            }
            ScalarExpr::BoundColumnRef(_)
            | ScalarExpr::ConstantExpr(_)
            | ScalarExpr::TypedConstantExpr(_, _)
            | ScalarExpr::LambdaFunction(_)
            | ScalarExpr::CastExpr(_) => {}
            ScalarExpr::UDFLambdaCall(_) => {}
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
        name_resolution_ctx: &NameResolutionContext,
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
        let mut scalar_binder =
            ScalarBinder::new(self, ctx.clone(), name_resolution_ctx, metadata.clone(), &[
            ]);

        let mut map_exprs = Vec::with_capacity(exprs.len());

        // check invalid ScalarExpr
        let mut rewriter = ExprValuesRewriter::new(ctx.clone());
        let mut default_values_binder = DefaultExprBinder::try_new(ctx.clone())?;
        for (i, expr) in exprs.iter().enumerate() {
            // `DEFAULT` in insert values will be parsed as `Expr::ColumnRef`.
            if let AExpr::ColumnRef { column, .. } = expr {
                if column.column.name().eq_ignore_ascii_case("default") {
                    let field = schema.field(i);
                    map_exprs.push(default_values_binder.get_expr(field, schema)?);
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
                .project_column_ref(|col| schema.index_of(&col.index.to_string()))?;
            map_exprs.push(expr);
        }

        let op = BlockOperator::Map {
            exprs: map_exprs,
            projections: None,
        };
        let one_row_chunk =
            DataBlock::new(vec![BlockEntry::new_const_column_arg::<UInt8Type>(1, 1)], 1);
        let func_ctx = ctx.get_function_context()?;
        let scalars = op
            .execute(&func_ctx, one_row_chunk)?
            .columns()
            .iter()
            .skip(1)
            .map(|col| unsafe { col.index_unchecked(0).to_owned() })
            .collect();
        Ok(scalars)
    }
}
