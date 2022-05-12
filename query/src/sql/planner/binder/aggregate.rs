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

use std::collections::HashMap;

use common_ast::ast::Expr;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::sql::binder::scalar::ScalarBinder;
use crate::sql::binder::scalar_common::find_aggregate_scalars_from_bind_context;
use crate::sql::binder::Binder;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::AggregateFunction;
use crate::sql::plans::AggregatePlan;
use crate::sql::plans::Scalar;
use crate::sql::plans::Scalar::BoundColumnRef;
use crate::sql::BindContext;

#[derive(Clone, PartialEq, Debug)]
pub struct AggregateInfo {
    /// Aggregation scalar expression
    pub agg_scalar_exprs: Option<Vec<Scalar>>,

    /// The origin scalar expression of Group by
    /// For the sql: `SELECT a%3 as a1, count(1) as ct from t GROUP BY a1`;
    /// The origin scalar expression is `a%3`.
    pub origin_group_by: Option<HashMap<String, Scalar>>,
}

impl AggregateInfo {
    pub fn new() -> Self {
        AggregateInfo {
            agg_scalar_exprs: None,
            origin_group_by: None,
        }
    }
}

impl<'a> Binder {
    pub(crate) fn analyze_aggregate(
        &self,
        output_context: &BindContext,
        agg_info: &mut AggregateInfo,
    ) -> Result<()> {
        let mut agg_expr: Vec<Scalar> = Vec::new();
        for agg_scalar in find_aggregate_scalars_from_bind_context(output_context)? {
            match agg_scalar {
                Scalar::AggregateFunction(AggregateFunction {
                    func_name,
                    distinct,
                    params,
                    args,
                    return_type,
                }) => agg_expr.push(
                    AggregateFunction {
                        func_name,
                        distinct,
                        params,
                        args,
                        return_type,
                    }
                    .into(),
                ),
                _ => {
                    return Err(ErrorCode::LogicalError(
                        "scalar expr must be Aggregation scalar expr",
                    ));
                }
            }
        }
        agg_info.agg_scalar_exprs = Some(agg_expr);
        Ok(())
    }

    pub(super) async fn bind_group_by(
        &mut self,
        group_by_expr: &[Expr<'a>],
        input_context: &mut BindContext,
        agg_info: &AggregateInfo,
    ) -> Result<()> {
        let scalar_binder = ScalarBinder::new(input_context, self.ctx.clone());
        let mut group_expr = Vec::with_capacity(group_by_expr.len());
        for expr in group_by_expr.iter() {
            let (scalar_expr, _) = scalar_binder.bind_expr(expr).await?;
            if let BoundColumnRef(bound_column) = scalar_expr {
                let col_name = bound_column.column.column_name.as_str();
                if let Some(origin_group_by) = agg_info.origin_group_by.as_ref() {
                    if origin_group_by.contains_key(bound_column.column.column_name.as_str()) {
                        // Use the origin group by expression
                        group_expr.push(
                            origin_group_by
                                .get(col_name)
                                .ok_or_else(|| {
                                    ErrorCode::SemanticError({
                                        format!("Not exist alias name {}", col_name)
                                    })
                                })?
                                .clone(),
                        );
                        continue;
                    }
                }
            }
            group_expr.push(scalar_binder.bind_expr(expr).await?.0);
        }
        let aggregate_plan = AggregatePlan {
            group_expr,
            agg_expr: agg_info.agg_scalar_exprs.clone().unwrap(),
        };
        let new_expr = SExpr::create_unary(
            aggregate_plan.into(),
            input_context.expression.clone().unwrap(),
        );
        input_context.expression = Some(new_expr);
        Ok(())
    }
}
