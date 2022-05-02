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

use std::sync::Arc;

use common_ast::ast::Expr;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::sql::binder::scalar::ScalarBinder;
use crate::sql::binder::scalar_common::find_aggregate_scalars;
use crate::sql::binder::scalar_common::find_aggregate_scalars_from_bind_context;
use crate::sql::binder::Binder;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::AggregatePlan;
use crate::sql::plans::Scalar;
use crate::sql::BindContext;
use crate::sql::ScalarExprRef;

impl Binder {
    pub(crate) fn analyze_aggregate(
        &self,
        expr: &Option<Expr>,
        output_context: &BindContext,
        input_context: &mut BindContext,
    ) -> Result<()> {
        let mut agg_expr: Vec<ScalarExprRef> = Vec::new();
        let mut agg_scalars = find_aggregate_scalars_from_bind_context(output_context)?;
        if let Some(e) = expr {
            let scalar_binder = ScalarBinder::new();
            let scalar = scalar_binder.bind_expr(e, input_context)?;
            agg_scalars.extend_from_slice(
                find_aggregate_scalars(&[scalar
                    .as_any()
                    .downcast_ref::<Scalar>()
                    .ok_or_else(|| ErrorCode::UnImplement("Can't downcast to Scalar"))?])
                .as_slice(),
            );
        }
        for agg_scalar in find_aggregate_scalars_from_bind_context(output_context)? {
            match agg_scalar {
                Scalar::AggregateFunction {
                    func_name,
                    distinct,
                    params,
                    args,
                    data_type,
                    nullable,
                } => agg_expr.push(Arc::new(Scalar::AggregateFunction {
                    func_name,
                    distinct,
                    params,
                    args,
                    data_type,
                    nullable,
                })),
                _ => {
                    return Err(ErrorCode::LogicalError(
                        "scalar expr must be Aggregation scalar expr",
                    ))
                }
            }
        }
        input_context.agg_scalar_exprs = Some(agg_expr);
        Ok(())
    }

    pub(super) fn bind_group_by(
        &mut self,
        group_by_expr: &[Expr],
        input_context: &mut BindContext,
    ) -> Result<()> {
        let scalar_binder = ScalarBinder::new();
        let group_expr = group_by_expr
            .iter()
            .map(|expr| scalar_binder.bind_expr(expr, input_context))
            .collect::<Result<Vec<ScalarExprRef>>>();

        let aggregate_plan = AggregatePlan {
            group_expr: group_expr?,
            agg_expr: input_context.agg_scalar_exprs.as_ref().unwrap().clone(),
        };
        let new_expr = SExpr::create_unary(
            aggregate_plan.into(),
            input_context.expression.clone().unwrap(),
        );
        input_context.expression = Some(new_expr);
        Ok(())
    }
}
