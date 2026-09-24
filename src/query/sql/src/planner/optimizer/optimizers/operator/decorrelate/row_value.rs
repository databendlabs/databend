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

//! Row-value `IN` against a multi-column subquery: `(a, b) IN (SELECT x, y ...)`.
//!
//! SQL compares the rows field by field with three-valued logic: TRUE when some
//! subquery row equals on every field, FALSE when every subquery row differs on
//! at least one non-NULL field, and NULL otherwise.
//!
//! A hash join only pairs rows whose keys are equal, so a NULL field can never
//! match through a key. The rewrite builds one `RightMark` join per set of
//! compared fields: the strict branch keys on every field; each wildcard branch
//! drops a subset of the nullable fields from the key and checks on the matched
//! pairs that every dropped field is NULL on one side. All branches are plain
//! hash joins with non-NULL keys, and an `EvalScalar` combines their markers.

use std::sync::Arc;

use databend_common_ast::Span;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_expression::Symbol;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberScalar;

use super::DerivedColumnScope;
use super::subquery_decorrelator::SubqueryDecorrelatorOptimizer;
use super::subquery_decorrelator::UnnestResult;
use crate::ColumnBinding;
use crate::ColumnBindingBuilder;
use crate::Visibility;
use crate::optimizer::ir::SExpr;
use crate::plans::Aggregate;
use crate::plans::BoundColumnRef;
use crate::plans::ConstantExpr;
use crate::plans::EvalScalar;
use crate::plans::Filter;
use crate::plans::FunctionCall;
use crate::plans::Join;
use crate::plans::JoinEquiCondition;
use crate::plans::JoinType;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::SubqueryExpr;

/// Each nullable field doubles the number of wildcard branches, and every
/// branch evaluates the subquery again.
const MAX_NULLABLE_ROW_FIELDS: usize = 3;

fn call(span: Span, name: &str, args: Vec<ScalarExpr>) -> Result<ScalarExpr> {
    let mut func = FunctionCall {
        span,
        func_name: name.to_string(),
        params: vec![],
        arguments: args,
        return_type: Box::new(DataType::Boolean),
    };
    func.refresh_return_type()?;
    Ok(func.into())
}

fn fold(span: Span, op: &str, preds: Vec<ScalarExpr>) -> Result<ScalarExpr> {
    let mut iter = preds.into_iter();
    let first = iter.next().expect("at least one predicate");
    iter.try_fold(first, |acc, pred| call(span, op, vec![acc, pred]))
}

/// `x IS NULL`; only `is_not_null` is registered as a function.
fn is_null(span: Span, expr: ScalarExpr) -> Result<ScalarExpr> {
    call(span, "not", vec![call(span, "is_not_null", vec![expr])?])
}

fn constant(span: Span, value: Scalar) -> ScalarExpr {
    ScalarExpr::ConstantExpr(ConstantExpr { span, value })
}

fn column_ref(span: Span, name: String, index: Symbol, data_type: DataType) -> ScalarExpr {
    let column = ColumnBindingBuilder::new(name, index, Box::new(data_type), Visibility::Visible);
    ScalarExpr::BoundColumnRef(BoundColumnRef {
        span,
        column: column.build(),
    })
}

fn right_ref(span: Span, column: &ColumnBinding, index: Symbol) -> ScalarExpr {
    let name = format!("subquery_{index}");
    column_ref(span, name, index, (*column.data_type).clone())
}

/// Left-hand fields and the matching subquery output columns of a row-value subquery.
pub(crate) fn row_value_fields(
    subquery: &SubqueryExpr,
) -> Result<(Vec<ScalarExpr>, Vec<ScalarExpr>)> {
    let span = subquery.span;
    let arity = subquery.row_columns.len();
    let child = subquery
        .child_expr
        .as_deref()
        .ok_or_else(|| ErrorCode::Internal("row-value subquery requires a left-hand expression"))?;
    let left = match child {
        ScalarExpr::FunctionCall(func)
            if func.func_name == "tuple" && func.arguments.len() == arity =>
        {
            func.arguments.clone()
        }
        _ => (1..=arity)
            .map(|i| {
                let mut get = FunctionCall {
                    span,
                    func_name: "get".to_string(),
                    params: vec![Scalar::Number(NumberScalar::Int64(i as i64))],
                    arguments: vec![child.clone()],
                    return_type: Box::new(DataType::Null),
                };
                get.refresh_return_type()?;
                Ok(get.into())
            })
            .collect::<Result<_>>()?,
    };
    let right = subquery
        .row_columns
        .iter()
        .map(|column| right_ref(span, column, column.index))
        .collect();
    Ok((left, right))
}

/// `l1 = r1 AND l2 = r2 AND ...` with SQL three-valued semantics.
pub(crate) fn row_value_equality(
    subquery: &SubqueryExpr,
    left: &[ScalarExpr],
    right: &[ScalarExpr],
) -> Result<ScalarExpr> {
    let span = subquery.span;
    let preds = left
        .iter()
        .zip(right)
        .map(|(l, r)| call(span, "eq", vec![l.clone(), r.clone()]))
        .collect::<Result<_>>()?;
    fold(span, "and", preds)
}

impl SubqueryDecorrelatorOptimizer {
    fn new_marker(&self) -> Symbol {
        self.ctx.get_metadata().write().add_derived_column(
            "marker".to_string(),
            DataType::Nullable(Box::new(DataType::Boolean)),
        )
    }

    /// A fresh copy of the subquery plan. New column indexes and scan ids keep
    /// the copies apart in the memo and in runtime filter targets.
    fn subquery_copy(&mut self, subquery: &SubqueryExpr) -> Result<(SExpr, Vec<ScalarExpr>)> {
        let mut scope = DerivedColumnScope::default();
        let plan = self.clone_outer_recursive(&subquery.subquery, &mut scope)?;
        let right = subquery
            .row_columns
            .iter()
            .map(|column| {
                Ok(right_ref(
                    subquery.span,
                    column,
                    scope.must_resolve(column.index)?,
                ))
            })
            .collect::<Result<_>>()?;
        Ok((plan, right))
    }

    /// Mark `outer` rows that match `build` on `keys` and satisfy `predicate`.
    fn mark_join(
        &self,
        outer: SExpr,
        build: SExpr,
        keys: Vec<(ScalarExpr, ScalarExpr)>,
        predicate: Option<ScalarExpr>,
        marker_index: Symbol,
    ) -> SExpr {
        let (left, right) = keys.into_iter().unzip();
        let join = Join {
            equi_conditions: JoinEquiCondition::new_conditions(left, right, vec![]),
            non_equi_conditions: predicate.into_iter().collect(),
            join_type: JoinType::RightMark,
            marker_index: Some(marker_index),
            ..Join::default()
        };
        SExpr::create_binary(Arc::new(join.into()), Arc::new(outer), Arc::new(build))
    }

    /// Rewrite an uncorrelated `(l1, ..., ln) IN (SELECT r1, ..., rn ...)`.
    ///
    /// The marker follows SQL row comparison: TRUE, FALSE, or NULL. When the
    /// subquery is a whole `WHERE` conjunct only TRUE rows survive, so a single
    /// strict hash join is enough.
    pub(crate) fn rewrite_uncorrelated_row_value(
        &mut self,
        outer: SExpr,
        subquery: &SubqueryExpr,
        is_conjunctive_predicate: bool,
    ) -> Result<(SExpr, UnnestResult)> {
        let span = subquery.span;
        let (left, right) = row_value_fields(subquery)?;
        let arity = left.len();
        let nullable: Vec<usize> = (0..arity)
            .filter(|&i| {
                left[i].data_type().is_nullable_or_null()
                    || right[i].data_type().is_nullable_or_null()
            })
            .collect();
        let keys_of = |right: &[ScalarExpr], keep: &dyn Fn(usize) -> bool| {
            (0..arity)
                .filter(|&i| keep(i))
                .map(|i| (left[i].clone(), right[i].clone()))
                .collect::<Vec<_>>()
        };

        if is_conjunctive_predicate || nullable.is_empty() {
            let marker_index = subquery
                .projection_index
                .unwrap_or_else(|| self.new_marker());
            let keys = keys_of(&right, &|_| true);
            let s_expr =
                self.mark_join(outer, *subquery.subquery.clone(), keys, None, marker_index);
            return Ok((s_expr, UnnestResult::MarkJoin { marker_index }));
        }
        if nullable.len() > MAX_NULLABLE_ROW_FIELDS {
            return Err(ErrorCode::Unimplemented(format!(
                "row-value IN with more than {MAX_NULLABLE_ROW_FIELDS} nullable fields is only supported as a WHERE condition"
            ))
            .set_span(span));
        }

        // Hash joins may report NULL markers on their own (NULL keys on either
        // side), so every branch marker is folded with `is_true` below.
        let strict_marker = self.new_marker();
        let keys = keys_of(&right, &|_| true);
        let mut s_expr =
            self.mark_join(outer, *subquery.subquery.clone(), keys, None, strict_marker);
        let mut wildcard_markers = vec![];
        // `mask` selects the nullable fields that stay in the hash key; the
        // dropped fields must be NULL on one of the two sides.
        for mask in 0..(1u32 << nullable.len()) - 1 {
            let compared = |i: usize| match nullable.iter().position(|&n| n == i) {
                Some(bit) => mask & (1 << bit) != 0,
                None => true,
            };
            let dropped: Vec<usize> = (0..arity).filter(|&i| !compared(i)).collect();
            let (copy, right) = self.subquery_copy(subquery)?;
            let marker = self.new_marker();
            let branch = if dropped.len() == arity {
                // Nothing to hash on: reduce the subquery to its distinct NULL
                // patterns so the key-less join stays tiny.
                let mut flags = vec![];
                let mut eval_items = vec![];
                for &i in &dropped {
                    let index = self
                        .ctx
                        .get_metadata()
                        .write()
                        .add_derived_column(format!("is_null_{i}"), DataType::Boolean);
                    flags.push(column_ref(
                        span,
                        format!("is_null_{i}"),
                        index,
                        DataType::Boolean,
                    ));
                    eval_items.push(ScalarItem {
                        scalar: is_null(span, right[i].clone())?,
                        index,
                    });
                }
                let group_items = flags
                    .iter()
                    .zip(&eval_items)
                    .map(|(flag, item)| ScalarItem {
                        scalar: flag.clone(),
                        index: item.index,
                    })
                    .collect();
                let patterns = copy
                    .build_unary(EvalScalar { items: eval_items })
                    .build_unary(Aggregate {
                        group_items,
                        ..Default::default()
                    });
                let preds = dropped
                    .iter()
                    .zip(flags)
                    .map(|(&i, flag)| call(span, "or", vec![is_null(span, left[i].clone())?, flag]))
                    .collect::<Result<_>>()?;
                self.mark_join(
                    s_expr,
                    patterns,
                    vec![],
                    Some(fold(span, "and", preds)?),
                    marker,
                )
            } else {
                // Keep NULL out of the hash keys so the join never reports
                // has_null, then check the dropped fields on matched pairs.
                let filters = nullable
                    .iter()
                    .filter(|&&i| compared(i))
                    .map(|&i| call(span, "is_not_null", vec![right[i].clone()]))
                    .collect::<Result<Vec<_>>>()?;
                let build = if filters.is_empty() {
                    copy
                } else {
                    copy.build_unary(Filter {
                        predicates: filters,
                    })
                };
                let preds = dropped
                    .iter()
                    .map(|&i| {
                        let left_null = is_null(span, left[i].clone())?;
                        call(span, "or", vec![
                            left_null,
                            is_null(span, right[i].clone())?,
                        ])
                    })
                    .collect::<Result<_>>()?;
                let keys = keys_of(&right, &compared);
                self.mark_join(s_expr, build, keys, Some(fold(span, "and", preds)?), marker)
            };
            s_expr = branch;
            wildcard_markers.push(marker);
        }

        // TRUE on a strict match, NULL on a possible match, FALSE otherwise.
        let is_true = |index| {
            let marker = column_ref(
                span,
                "marker".to_string(),
                index,
                DataType::Nullable(Box::new(DataType::Boolean)),
            );
            call(span, "is_true", vec![marker])
        };
        let possible = fold(
            span,
            "or",
            wildcard_markers
                .into_iter()
                .map(is_true)
                .collect::<Result<_>>()?,
        )?;
        let combined = call(span, "if", vec![
            is_true(strict_marker)?,
            constant(span, Scalar::Boolean(true)),
            possible,
            constant(span, Scalar::Null),
            constant(span, Scalar::Boolean(false)),
        ])?;
        let marker_index = subquery
            .projection_index
            .unwrap_or_else(|| self.new_marker());
        let s_expr = s_expr.build_unary(EvalScalar {
            items: vec![ScalarItem {
                scalar: combined,
                index: marker_index,
            }],
        });
        Ok((s_expr, UnnestResult::MarkJoin { marker_index }))
    }
}
