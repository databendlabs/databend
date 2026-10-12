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

//! `(a, b) IN (SELECT x, y ...)` with SQL row comparison semantics.
//!
//! A hash join cannot match a NULL field through its key, so the rewrite
//! uses one `RightMark` join per set of compared fields: the strict join keys
//! on every field (TRUE), and each wildcard join drops some nullable fields
//! from the key and requires them to be NULL on either side (UNKNOWN).

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

/// Wildcard joins grow as 2^n in the number of nullable fields.
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
    let mut acc = iter.next().expect("at least one predicate");
    for pred in iter {
        acc = call(span, op, vec![acc, pred])?;
    }
    Ok(acc)
}

fn is_null(span: Span, expr: ScalarExpr) -> Result<ScalarExpr> {
    let not_null = call(span, "is_not_null", vec![expr])?;
    call(span, "not", vec![not_null])
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

fn marker_ref(span: Span, index: Symbol) -> ScalarExpr {
    let data_type = DataType::Nullable(Box::new(DataType::Boolean));
    column_ref(span, "marker".to_string(), index, data_type)
}

fn right_refs(
    subquery: &SubqueryExpr,
    scope: Option<&DerivedColumnScope>,
) -> Result<Vec<ScalarExpr>> {
    let mut refs = Vec::with_capacity(subquery.row_columns.len());
    for column in &subquery.row_columns {
        let index = match scope {
            Some(scope) => scope.must_resolve(column.index)?,
            None => column.index,
        };
        let data_type = (*column.data_type).clone();
        refs.push(column_ref(
            subquery.span,
            format!("subquery_{index}"),
            index,
            data_type,
        ));
    }
    Ok(refs)
}

/// Left-hand fields and the matching subquery output columns.
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
        _ => {
            let mut fields = Vec::with_capacity(arity);
            for i in 1..=arity {
                let mut get = FunctionCall {
                    span,
                    func_name: "get".to_string(),
                    params: vec![Scalar::Number(NumberScalar::Int64(i as i64))],
                    arguments: vec![child.clone()],
                    return_type: Box::new(DataType::Null),
                };
                get.refresh_return_type()?;
                fields.push(get.into());
            }
            fields
        }
    };
    Ok((left, right_refs(subquery, None)?))
}

/// `l1 = r1 AND l2 = r2 AND ...`
pub(crate) fn row_value_equality(
    subquery: &SubqueryExpr,
    left: &[ScalarExpr],
    right: &[ScalarExpr],
) -> Result<ScalarExpr> {
    let span = subquery.span;
    let mut preds = Vec::with_capacity(left.len());
    for (l, r) in left.iter().zip(right) {
        preds.push(call(span, "eq", vec![l.clone(), r.clone()])?);
    }
    fold(span, "and", preds)
}

impl SubqueryDecorrelatorOptimizer {
    fn new_markers(&self, count: usize) -> Vec<Symbol> {
        let metadata = self.ctx.get_metadata();
        let mut metadata = metadata.write();
        let mut markers = Vec::with_capacity(count);
        for _ in 0..count {
            markers.push(metadata.add_derived_column(
                "marker".to_string(),
                DataType::Nullable(Box::new(DataType::Boolean)),
            ));
        }
        markers
    }

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

    /// Key-less join against the distinct NULL patterns of the subquery.
    fn null_pattern_join(
        &mut self,
        outer: SExpr,
        subquery: &SubqueryExpr,
        left: &[ScalarExpr],
        marker: Symbol,
    ) -> Result<SExpr> {
        let span = subquery.span;
        let mut scope = DerivedColumnScope::default();
        let copy = self.clone_outer_recursive(&subquery.subquery, &mut scope)?;
        let right = right_refs(subquery, Some(&scope))?;

        let mut indexes = Vec::with_capacity(left.len());
        {
            let metadata = self.ctx.get_metadata();
            let mut metadata = metadata.write();
            for i in 0..left.len() {
                indexes
                    .push(metadata.add_derived_column(format!("is_null_{i}"), DataType::Boolean));
            }
        }

        let mut eval_items = vec![];
        let mut group_items = vec![];
        let mut preds = vec![];
        for (i, index) in indexes.into_iter().enumerate() {
            let flag = column_ref(span, format!("is_null_{i}"), index, DataType::Boolean);
            eval_items.push(ScalarItem {
                scalar: is_null(span, right[i].clone())?,
                index,
            });
            group_items.push(ScalarItem {
                scalar: flag.clone(),
                index,
            });
            let left_null = is_null(span, left[i].clone())?;
            preds.push(call(span, "or", vec![left_null, flag])?);
        }
        let patterns = copy
            .build_unary(EvalScalar { items: eval_items })
            .build_unary(Aggregate {
                group_items,
                ..Default::default()
            });
        let predicate = fold(span, "and", preds)?;
        Ok(self.mark_join(outer, patterns, vec![], Some(predicate), marker))
    }

    /// Join keyed on the fields in `compared`; the other fields must be NULL
    /// on either side.
    fn wildcard_join(
        &mut self,
        outer: SExpr,
        subquery: &SubqueryExpr,
        left: &[ScalarExpr],
        nullable: &[usize],
        compared: &[bool],
        marker: Symbol,
    ) -> Result<SExpr> {
        let span = subquery.span;
        let mut scope = DerivedColumnScope::default();
        let copy = self.clone_outer_recursive(&subquery.subquery, &mut scope)?;
        let right = right_refs(subquery, Some(&scope))?;

        let mut keys = vec![];
        let mut filters = vec![];
        let mut preds = vec![];
        for i in 0..left.len() {
            if compared[i] {
                keys.push((left[i].clone(), right[i].clone()));
                if nullable.contains(&i) {
                    filters.push(call(span, "is_not_null", vec![right[i].clone()])?);
                }
            } else {
                let left_null = is_null(span, left[i].clone())?;
                let right_null = is_null(span, right[i].clone())?;
                preds.push(call(span, "or", vec![left_null, right_null])?);
            }
        }
        let build = copy.build_unary(Filter {
            predicates: filters,
        });
        let predicate = fold(span, "and", preds)?;
        Ok(self.mark_join(outer, build, keys, Some(predicate), marker))
    }

    /// Uncorrelated `(l1, ..., ln) IN (SELECT r1, ..., rn ...)`.
    ///
    /// A `WHERE` conjunct only keeps TRUE rows, so one strict hash join is
    /// enough there; otherwise UNKNOWN must be told apart from FALSE.
    pub(crate) fn rewrite_uncorrelated_row_value(
        &mut self,
        outer: SExpr,
        subquery: &SubqueryExpr,
        is_conjunctive_predicate: bool,
    ) -> Result<(SExpr, UnnestResult)> {
        let span = subquery.span;
        let (left, right) = row_value_fields(subquery)?;
        let arity = left.len();

        let mut nullable = vec![];
        for i in 0..arity {
            if left[i].data_type().is_nullable_or_null()
                || right[i].data_type().is_nullable_or_null()
            {
                nullable.push(i);
            }
        }

        let mut strict_keys = Vec::with_capacity(arity);
        for i in 0..arity {
            strict_keys.push((left[i].clone(), right[i].clone()));
        }
        let build = *subquery.subquery.clone();

        if is_conjunctive_predicate || nullable.is_empty() {
            let marker_index = match subquery.projection_index {
                Some(index) => index,
                None => self.new_markers(1)[0],
            };
            let s_expr = self.mark_join(outer, build, strict_keys, None, marker_index);
            return Ok((s_expr, UnnestResult::MarkJoin { marker_index }));
        }
        if nullable.len() > MAX_NULLABLE_ROW_FIELDS {
            return Err(ErrorCode::Unimplemented(format!(
                "row-value IN with more than {MAX_NULLABLE_ROW_FIELDS} nullable fields is only supported as a WHERE condition"
            ))
            .set_span(span));
        }

        // markers[0] is the strict join, markers[1..] one per wildcard branch.
        let branches = (1usize << nullable.len()) - 1;
        let markers = self.new_markers(branches + 1);
        let strict_marker = markers[0];
        let wildcard_markers = &markers[1..];
        let mut s_expr = self.mark_join(outer, build, strict_keys, None, strict_marker);

        // Bit `b` of `mask` keeps nullable field `nullable[b]` in the key.
        for (mask, marker) in wildcard_markers.iter().enumerate() {
            let mut compared = vec![true; arity];
            for (bit, field) in nullable.iter().enumerate() {
                compared[*field] = mask & (1 << bit) != 0;
            }
            s_expr = if compared.iter().any(|c| *c) {
                self.wildcard_join(s_expr, subquery, &left, &nullable, &compared, *marker)?
            } else {
                self.null_pattern_join(s_expr, subquery, &left, *marker)?
            };
        }

        // Joins may emit NULL markers themselves, hence `is_true`.
        let mut possible = vec![];
        for marker in wildcard_markers {
            possible.push(call(span, "is_true", vec![marker_ref(span, *marker)])?);
        }
        let strict = call(span, "is_true", vec![marker_ref(span, strict_marker)])?;
        let combined = call(span, "if", vec![
            strict,
            constant(span, Scalar::Boolean(true)),
            fold(span, "or", possible)?,
            constant(span, Scalar::Null),
            constant(span, Scalar::Boolean(false)),
        ])?;
        let marker_index = match subquery.projection_index {
            Some(index) => index,
            None => self.new_markers(1)[0],
        };
        let s_expr = s_expr.build_unary(EvalScalar {
            items: vec![ScalarItem {
                scalar: combined,
                index: marker_index,
            }],
        });
        Ok((s_expr, UnnestResult::MarkJoin { marker_index }))
    }
}
