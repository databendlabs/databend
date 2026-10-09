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

use databend_common_exception::Result;
use databend_common_expression::Scalar;

use super::bind_join::RightColumnReplacements;
use crate::binder::BindContext;
use crate::optimizer::OptimizerContext;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::operator::ScalarAggregateMatch;
use crate::optimizer::optimizers::operator::SubqueryDecorrelatorOptimizer;
use crate::optimizer::optimizers::operator::function_call;
use crate::optimizer::optimizers::operator::match_scalar_aggregate;
use crate::planner::binder::Binder;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::ConstantExpr;
use crate::plans::EvalScalar;
use crate::plans::Filter;
use crate::plans::JoinType;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::SubqueryComparisonOp;

impl Binder {
    /// Binds a correlated lateral subquery whose result is a scalar aggregate (no GROUP BY),
    /// optionally followed by row-preserving operators such as projections and HAVING. See
    /// `decorrelate/scalar_aggregate.rs` for the rewrite.
    ///
    /// For INNER and CROSS lateral joins, `HAVING` and the user join conditions are applied
    /// as filters above the rebuilt plan. For a LEFT lateral join the outer row must be kept
    /// when they fail, so they are evaluated into a match flag instead, and the right side
    /// columns are replaced by new columns that are NULL when the flag is false. The
    /// replacements are returned, not applied to `right_context`; callers must apply them to
    /// the bind contexts built from the right side bindings. Returns `None` if the plan shape
    /// isn't supported.
    pub(crate) fn try_bind_lateral_scalar_aggregate(
        &mut self,
        join_type: JoinType,
        left_child: &SExpr,
        right_child: &SExpr,
        right_context: &BindContext,
        (left_conditions, right_conditions): (&[ScalarExpr], &[ScalarExpr]),
        non_equi_conditions: &[ScalarExpr],
    ) -> Result<Option<(SExpr, RightColumnReplacements)>> {
        if !matches!(
            join_type,
            JoinType::Inner | JoinType::Cross | JoinType::Left
        ) {
            return Ok(None);
        }
        let ScalarAggregateMatch::Shape(shape) = match_scalar_aggregate(left_child, right_child)?
        else {
            return Ok(None);
        };
        // The NULL-extended replacements are matched by column index in the join output
        // context, so a right side column must not share its index with a left column.
        // `bind_join` separates them; bail out if a caller didn't.
        if join_type == JoinType::Left {
            let left_prop = RelExpr::with_s_expr(left_child).derive_relational_prop()?;
            if right_context
                .columns
                .iter()
                .any(|column| left_prop.output_columns.contains(&column.index))
            {
                return Ok(None);
            }
        }

        let opt_ctx = OptimizerContext::new(
            self.ctx.clone(),
            self.metadata.clone(),
            self.ctx.get_function_context()?,
        );
        let mut decorrelator = SubqueryDecorrelatorOptimizer::new(opt_ctx, Some(self.clone()));
        let keep_rows = join_type == JoinType::Left;
        let Some(rewrite) =
            decorrelator.rewrite_scalar_aggregate(left_child, &shape, keep_rows, true)?
        else {
            return Ok(None);
        };

        // The user join conditions, applied above the rebuilt plan.
        let mut predicates = left_conditions
            .iter()
            .zip(right_conditions.iter())
            .map(|(left, right)| {
                SubqueryComparisonOp::Equal
                    .to_func_call(None, left.clone(), right.clone())
                    .map(ScalarExpr::FunctionCall)
            })
            .collect::<Result<Vec<_>>>()?;
        predicates.extend(non_equi_conditions.iter().cloned());

        let mut non_lazy_columns = rewrite.key_columns;
        for scalar in predicates.iter() {
            scalar.collect_used_columns(&mut non_lazy_columns);
        }
        self.metadata.write().add_non_lazy_columns(non_lazy_columns);

        let mut s_expr = rewrite.s_expr;
        if !keep_rows {
            if !predicates.is_empty() {
                s_expr =
                    SExpr::create_unary(Arc::new(Filter { predicates }.into()), Arc::new(s_expr));
            }
            return Ok(Some((s_expr, RightColumnReplacements::default())));
        }

        // LEFT lateral join: the ON condition is combined into the match flag of HAVING.
        let mut matched = rewrite.matched;
        if !predicates.is_empty() {
            let (flag, op) = decorrelator.add_match_flag(matched.as_ref(), predicates, true)?;
            matched = Some(flag);
            s_expr = SExpr::create_unary(Arc::new(op), Arc::new(s_expr));
        }
        let mut replacements = RightColumnReplacements::default();
        if let Some(matched) = matched {
            (s_expr, replacements) = self.mask_right_columns(s_expr, right_context, &matched)?;
        }
        Ok(Some((s_expr, replacements)))
    }

    /// Adds columns replacing the right side columns of a LEFT lateral join, NULL when
    /// `matched` is false. `right_context` is left unchanged; callers apply the returned
    /// replacements to the bind contexts built from the right side bindings.
    fn mask_right_columns(
        &mut self,
        s_expr: SExpr,
        right_context: &BindContext,
        matched: &ScalarExpr,
    ) -> Result<(SExpr, RightColumnReplacements)> {
        let mut items = Vec::with_capacity(right_context.columns.len());
        let mut replacements = Vec::with_capacity(right_context.columns.len());
        for column in right_context.columns.iter() {
            let value = ScalarExpr::BoundColumnRef(BoundColumnRef {
                span: None,
                column: column.clone(),
            });
            let null = ScalarExpr::CastExpr(CastExpr {
                span: None,
                is_try: false,
                argument: Box::new(ScalarExpr::ConstantExpr(ConstantExpr {
                    span: None,
                    value: Scalar::Null,
                })),
                target_type: Box::new(column.data_type.wrap_nullable()),
            });
            let scalar = function_call("if", vec![matched.clone(), value, null])?;
            let data_type = scalar.data_type().into_owned();
            let index = self
                .metadata
                .write()
                .add_derived_column(column.column_name.clone(), data_type.clone());
            items.push(ScalarItem { scalar, index });
            let mut replaced = column.clone();
            replaced.index = index;
            replaced.data_type = Box::new(data_type);
            replacements.push((column.index, replaced));
        }
        let s_expr = SExpr::create_unary(Arc::new(EvalScalar { items }.into()), Arc::new(s_expr));
        Ok((s_expr, RightColumnReplacements(replacements)))
    }
}
