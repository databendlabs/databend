// Copyright 2022 Datafuse Labs.
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

use common_ast::ast::Expr;
use common_ast::parser::error::DisplayError;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::sql::binder::scalar::ScalarBinder;
use crate::sql::binder::scalar_common::find_aggregate_scalars;
use crate::sql::binder::scalar_common::find_aggregate_scalars_from_bind_context;
use crate::sql::binder::Binder;
use crate::sql::binder::ColumnBinding;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::AggregatePlan;
use crate::sql::plans::Scalar;
use crate::sql::plans::ScalarExpr;
use crate::sql::BindContext;

#[derive(Clone, PartialEq, Debug, Default)]
pub struct AggregateInfo {
    /// Aggregation functions
    pub aggregate_functions: Vec<Scalar>,

    /// Available aliases can be used as group item
    pub available_aliases: Vec<ColumnBinding>,
}

impl<'a> Binder {
    /// We have supported two kinds of `group by` items:
    ///
    ///   - Alias, the aliased expressions specified in `SELECT` clause, e.g. column `b` in
    ///     `SELECT a as b, COUNT(a) FROM t GROUP BY b`.
    ///   - Scalar expressions that can be evaluated in current scope(doesn't contain aliases), e.g.
    ///     column `a` and expression `a+1` in `SELECT a as b, COUNT(a) FROM t GROUP BY a, a+1`.
    pub(crate) fn analyze_aggregate(&self, output_context: &BindContext) -> Result<AggregateInfo> {
        let mut available_aliases: Vec<ColumnBinding> = vec![];
        for column_binding in output_context.all_column_bindings() {
            if let Some(scalar) = &column_binding.scalar {
                // If a column is `BoundColumnRef`, we will treat it as an available alias anyway.
                // Otherwise we will check visibility and if the scalar contains aggregate functions.
                if matches!(scalar.as_ref(), Scalar::BoundColumnRef(_))
                    || (column_binding.visible
                        && find_aggregate_scalars(&[*scalar.clone()]).is_empty())
                {
                    available_aliases.push(column_binding.clone());
                }
            }
        }

        let mut aggregate_functions: Vec<Scalar> = Vec::new();
        for agg_scalar in find_aggregate_scalars_from_bind_context(output_context)? {
            match agg_scalar {
                agg @ Scalar::AggregateFunction(_) => aggregate_functions.push(agg),
                _ => {
                    return Err(ErrorCode::LogicalError(
                        "scalar expr must be Aggregation scalar expr",
                    ));
                }
            }
        }

        Ok(AggregateInfo {
            aggregate_functions,
            available_aliases,
        })
    }

    pub(super) async fn bind_group_by(
        &mut self,
        input_context: &BindContext,
        output_context: BindContext,
        child: SExpr,
        group_by_expr: &[Expr<'a>],
        agg_info: &AggregateInfo,
    ) -> Result<(SExpr, BindContext)> {
        let scalar_binder = ScalarBinder::new(input_context, self.ctx.clone());
        let mut group_expr = Vec::with_capacity(group_by_expr.len());
        for expr in group_by_expr.iter() {
            let (scalar_expr, _) = scalar_binder.bind_expr(expr).await.or_else(|e| {
                let mut result = vec![];
                // If cannot resolve group item, then try to find an available alias
                for column_binding in agg_info.available_aliases.iter() {
                    let col_name = column_binding.column_name.as_str();
                    if let Some(scalar) = &column_binding.scalar {
                        // TODO(leiysky): check if expr is a qualified name
                        if col_name == expr.to_string().to_lowercase().as_str() {
                            result.push(scalar);
                        }
                    }
                }

                if result.is_empty() {
                    Err(e)
                } else if result.len() > 1 {
                    Err(ErrorCode::SemanticError(expr.span().display_error(
                        format!("GROUP BY \"{}\" is ambiguous", expr),
                    )))
                } else {
                    Ok((*result[0].clone(), result[0].data_type()))
                }
            })?;

            group_expr.push(scalar_expr);
        }

        let aggregate_plan = AggregatePlan {
            group_items: group_expr,
            aggregate_functions: agg_info.aggregate_functions.clone(),
        };
        let new_expr = SExpr::create_unary(aggregate_plan.into(), child);
        Ok((new_expr, output_context))
    }
}
