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

//! Decorrelation of a correlated scalar aggregate (no GROUP BY), shared by `LATERAL` joins and
//! scalar subqueries.
//!
//! Such a subquery returns exactly one aggregate row for every outer row, even when its input
//! is empty. The generic flattening groups the aggregate by the correlated columns, so outer
//! values without any input row lose their group. Here only the aggregate is flattened and
//! LEFT joined back to the outer side; aggregates whose empty-input value isn't NULL (`count`,
//! `array_agg`, ...) are filled with that value; and the operators above the aggregate are
//! evaluated after the join, where both outer columns and the filled aggregate values are
//! available.

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ConstantFolder;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_functions::aggregates::eval_aggr;

use super::DerivedColumnScope;
use super::FlattenInfo;
use super::SubqueryDecorrelatorOptimizer;
use crate::ColumnSet;
use crate::Metadata;
use crate::Symbol;
use crate::binder::ColumnBindingBuilder;
use crate::binder::Visibility;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::plans::Aggregate;
use crate::plans::AggregateFunction;
use crate::plans::AggregateMode;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::ConstantExpr;
use crate::plans::EvalScalar;
use crate::plans::FunctionCall;
use crate::plans::Join;
use crate::plans::JoinEquiCondition;
use crate::plans::JoinType;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::SubqueryExpr;

/// The result of [`match_scalar_aggregate`].
pub(crate) enum ScalarAggregateMatch {
    /// Not a correlated scalar aggregate this rewrite supports.
    Unsupported,
    /// A correlated scalar aggregate under `LIMIT 0` or an `OFFSET`: the subquery never
    /// returns a row.
    AlwaysEmpty,
    Shape(Box<ScalarAggregateShape>),
}

/// A correlated scalar aggregate (no GROUP BY) at the top of a subquery, under operators that
/// keep it at one row per outer value.
pub(crate) struct ScalarAggregateShape {
    /// Projections and filters (`HAVING`) above the aggregate, top-down. Sorts, `LIMIT n` and
    /// `SELECT DISTINCT` are no-ops on a single row and dropped.
    kept: Vec<RelOperator>,
    aggregate: Aggregate,
    aggregate_input: SExpr,
    correlated_columns: ColumnSet,
    /// For each aggregate function, its value on empty input and whether a marker is needed
    /// to recognize unmatched rows.
    empty_values: Vec<(Scalar, bool)>,
}

/// The result of [`SubqueryDecorrelatorOptimizer::rewrite_scalar_aggregate`].
pub(crate) struct ScalarAggregateRewrite {
    pub(crate) s_expr: SExpr,
    /// With `keep_rows`, a boolean that is false for the rows a `HAVING` above the aggregate
    /// rejects. Those rows are kept and must be NULL-extended by the caller.
    pub(crate) matched: Option<ScalarExpr>,
    /// Columns used by the keys of the reconnecting join.
    pub(crate) key_columns: ColumnSet,
}

/// The result of [`SubqueryDecorrelatorOptimizer::try_decorrelate_scalar_aggregate_subquery`].
pub(crate) enum ScalarSubqueryRewrite {
    /// Not a supported shape; use the generic decorrelation unchanged.
    Unsupported,
    /// The generic LEFT join plan is already correct: the subquery yields NULL for an outer
    /// value without aggregate input, which is what NULL-extension produces. The caller must
    /// not apply the `count` fix-up to it.
    GenericPlan,
    Rewritten(Box<(SExpr, DerivedColumnScope)>),
}

/// An aggregate output whose value on empty input is not NULL, e.g. `count(*)` or `array_agg`.
struct AggregateFill {
    /// The column index referenced by the operators above the aggregate.
    output: Symbol,
    /// The column index now produced by the flattened aggregate.
    raw: Symbol,
    /// The aggregate result on empty input.
    empty_value: Scalar,
    data_type: DataType,
    /// Whether the aggregate may return NULL for non-empty input. If not, a NULL `raw` value
    /// can only come from the LEFT join, so it identifies unmatched rows without a marker.
    needs_marker: bool,
}

/// Matches a correlated scalar aggregate (no GROUP BY) under projections, `HAVING` filters and
/// operators that are no-ops on a single row, with every correlated column of `subquery`
/// produced by `outer`.
pub(crate) fn match_scalar_aggregate(
    outer: &SExpr,
    subquery: &SExpr,
) -> Result<ScalarAggregateMatch> {
    let mut kept = Vec::new();
    let mut always_empty = false;
    let mut current = subquery;
    let aggregate = loop {
        match current.plan() {
            RelOperator::EvalScalar(eval) => {
                if current.plan().has_subquery() {
                    return Ok(ScalarAggregateMatch::Unsupported);
                }
                kept.push(RelOperator::EvalScalar(eval.clone()));
            }
            RelOperator::Filter(filter) => {
                if current.plan().has_subquery() {
                    return Ok(ScalarAggregateMatch::Unsupported);
                }
                // `ON true` of a LEFT lateral join is pushed down as `Filter [true]`, which
                // can be dropped.
                if !filter.predicates.iter().all(is_true_constant) {
                    kept.push(RelOperator::Filter(filter.clone()));
                }
            }
            // Sorting a single row is a no-op.
            RelOperator::Sort(sort) if sort.limit != Some(0) => {}
            RelOperator::Limit(limit) => {
                // `LIMIT n` keeps the single row; `LIMIT 0` or an `OFFSET` removes it.
                if limit.offset != 0 || limit.limit == Some(0) {
                    always_empty = true;
                }
            }
            // `SELECT DISTINCT` over a single row is a no-op. The binder builds it as an
            // aggregate that groups by the projected columns, keeping their indexes.
            RelOperator::Aggregate(aggregate) if is_passthrough_distinct(aggregate) => {}
            RelOperator::Aggregate(aggregate) => break aggregate,
            _ => return Ok(ScalarAggregateMatch::Unsupported),
        }
        current = current.unary_child();
    };
    if aggregate.mode != AggregateMode::Initial
        || !aggregate.group_items.is_empty()
        || aggregate.grouping_sets.is_some()
        || aggregate.rank_limit.is_some()
    {
        return Ok(ScalarAggregateMatch::Unsupported);
    }
    let aggregate_input = current.unary_child();

    // If the aggregate input is uncorrelated, the aggregate already yields one row and the
    // generic flattening handles it.
    let outer_prop = RelExpr::with_s_expr(outer).derive_relational_prop()?;
    let subquery_prop = RelExpr::with_s_expr(subquery).derive_relational_prop()?;
    let correlated_columns = RelExpr::with_s_expr(aggregate_input)
        .derive_relational_prop()?
        .outer_columns
        .clone();
    if correlated_columns.is_empty()
        || !subquery_prop
            .outer_columns
            .is_subset(&outer_prop.output_columns)
        || has_correlated_scalar_aggregate(aggregate_input)?
    {
        return Ok(ScalarAggregateMatch::Unsupported);
    }
    if always_empty {
        return Ok(ScalarAggregateMatch::AlwaysEmpty);
    }

    let mut empty_values = Vec::with_capacity(aggregate.aggregate_functions.len());
    for item in aggregate.aggregate_functions.iter() {
        let ScalarExpr::AggregateFunction(func) = &item.scalar else {
            return Ok(ScalarAggregateMatch::Unsupported);
        };
        let Some((empty_value, needs_marker)) = empty_aggregate_value(func) else {
            return Ok(ScalarAggregateMatch::Unsupported);
        };
        if matches!(empty_value, Scalar::Null) && !func.return_type.is_nullable_or_null() {
            return Ok(ScalarAggregateMatch::Unsupported);
        }
        empty_values.push((empty_value, needs_marker));
    }

    Ok(ScalarAggregateMatch::Shape(Box::new(
        ScalarAggregateShape {
            kept,
            aggregate: aggregate.clone(),
            aggregate_input: aggregate_input.clone(),
            correlated_columns,
            empty_values,
        },
    )))
}

impl SubqueryDecorrelatorOptimizer {
    /// Rewrites a matched scalar aggregate: the flattened aggregate is LEFT joined to `outer`
    /// on the correlation keys, aggregates whose empty-input value isn't NULL are filled, and
    /// the operators above the aggregate are re-applied on top.
    ///
    /// With `keep_rows`, a `HAVING` above the aggregate doesn't remove the outer row: it is
    /// evaluated into the returned match flag, and the projections above it are guarded so
    /// they aren't evaluated on rejected rows. Without `keep_rows`, it is applied as a filter.
    /// Returns `None` if no reconnecting key can be built.
    pub(crate) fn rewrite_scalar_aggregate(
        &mut self,
        outer: &SExpr,
        shape: &ScalarAggregateShape,
        keep_rows: bool,
        is_lateral: bool,
    ) -> Result<Option<ScalarAggregateRewrite>> {
        let metadata = self.ctx.get_metadata();
        // Give aggregates that need a fill a new output column; the original column is
        // produced by the fill projection above the join.
        let mut aggregate = shape.aggregate.clone();
        let mut fills = Vec::new();
        for (item, (empty_value, needs_marker)) in aggregate
            .aggregate_functions
            .iter_mut()
            .zip(shape.empty_values.iter())
        {
            if matches!(empty_value, Scalar::Null) {
                // The NULL produced by the LEFT join is already the empty-input result.
                continue;
            }
            let ScalarExpr::AggregateFunction(func) = &item.scalar else {
                return Ok(None);
            };
            let data_type = func.return_type.as_ref().clone();
            let raw = metadata
                .write()
                .add_derived_column(func.display_name.clone(), data_type.clone());
            fills.push(AggregateFill {
                output: item.index,
                raw,
                empty_value: empty_value.clone(),
                data_type,
                needs_marker: *needs_marker,
            });
            item.index = raw;
        }

        let aggregate_expr = SExpr::create_unary(
            Arc::new(aggregate.into()),
            Arc::new(shape.aggregate_input.clone()),
        );
        let (mut flatten_plan, derived_columns) = self.flatten_plan(
            outer,
            &aggregate_expr,
            &shape.correlated_columns,
            &mut FlattenInfo {
                from_count_func: false,
            },
            false,
        )?;

        let mut outer_keys = Vec::new();
        let mut derived_keys = Vec::new();
        self.add_equi_conditions(
            None,
            &shape.correlated_columns,
            &derived_columns,
            &mut derived_keys,
            &mut outer_keys,
        )?;
        if outer_keys.is_empty() {
            return Ok(None);
        }
        // The keys identify correlation groups, so NULL groups must match each other.
        let is_null_equal = Self::nullable_condition_indexes(&outer_keys, &derived_keys);
        let mut key_columns = ColumnSet::new();
        for scalar in outer_keys.iter().chain(&derived_keys) {
            scalar.collect_used_columns(&mut key_columns);
        }

        let name_prefix = if is_lateral { "lateral" } else { "subquery" };
        // Unmatched rows are recognized by a NULL aggregate value when the aggregate never
        // returns NULL. Otherwise a constant marker on the aggregate side tells whether the
        // LEFT join found the aggregate row of an outer row.
        let marker = if fills.iter().any(|fill| fill.needs_marker) {
            let marker = metadata
                .write()
                .add_derived_column(format!("{name_prefix}_marker"), DataType::Boolean);
            flatten_plan = SExpr::create_unary(
                Arc::new(
                    EvalScalar {
                        items: vec![ScalarItem {
                            scalar: ScalarExpr::ConstantExpr(ConstantExpr {
                                span: None,
                                value: Scalar::Boolean(true),
                            }),
                            index: marker,
                        }],
                    }
                    .into(),
                ),
                Arc::new(flatten_plan),
            );
            Some(marker)
        } else {
            None
        };

        let join = Join {
            equi_conditions: JoinEquiCondition::new_conditions(
                outer_keys,
                derived_keys,
                is_null_equal,
            ),
            non_equi_conditions: vec![],
            join_type: JoinType::Left,
            marker_index: None,
            from_correlated_subquery: !is_lateral,
            need_hold_hash_table: false,
            is_lateral,
            single_to_inner: None,
            build_side_cache_info: None,
            spatial_join: None,
        };
        let mut s_expr = SExpr::create_binary(
            Arc::new(join.into()),
            Arc::new(outer.clone()),
            Arc::new(flatten_plan),
        );

        if !fills.is_empty() {
            let metadata = metadata.read();
            let marker = marker
                .map(|marker| column_ref(&metadata, marker, DataType::Boolean.wrap_nullable()));
            let items = fills
                .into_iter()
                .map(|fill| {
                    let nullable_type = fill.data_type.wrap_nullable();
                    let raw = column_ref(&metadata, fill.raw, nullable_type.clone());
                    let empty = cast(constant(fill.empty_value), nullable_type);
                    let matched_by = match (&marker, fill.needs_marker) {
                        (Some(marker), true) => marker.clone(),
                        _ => raw.clone(),
                    };
                    let matched = function_call("is_not_null", vec![matched_by])?;
                    let value = function_call("if", vec![matched, raw, empty])?;
                    // Matched rows carry the real aggregate value and unmatched rows the
                    // empty-input value, so the result is never NULL for non-nullable types.
                    let scalar = if fill.data_type.is_nullable_or_null() {
                        value
                    } else {
                        cast(value, fill.data_type)
                    };
                    Ok(ScalarItem {
                        scalar,
                        index: fill.output,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            drop(metadata);
            s_expr = SExpr::create_unary(Arc::new(EvalScalar { items }.into()), Arc::new(s_expr));
        }

        let mut matched: Option<ScalarExpr> = None;
        for op in shape.kept.iter().rev() {
            let op = match (op, keep_rows) {
                (op, false) => op.clone(),
                // Every outer row has exactly one candidate row here, which must be kept when
                // HAVING fails. Each filter is turned into a match flag, combined with the
                // flags below it.
                (RelOperator::Filter(filter), true) => {
                    let (flag, op) = self.add_match_flag(
                        matched.as_ref(),
                        filter.predicates.clone(),
                        is_lateral,
                    )?;
                    matched = Some(flag);
                    op
                }
                (RelOperator::EvalScalar(eval), true) => {
                    // Rows whose flag is already false are NULL-extended later, so don't
                    // evaluate expressions on them: they may fail where the original plan
                    // would never have evaluated them, e.g. behind `HAVING count(*) > 0`.
                    let mut eval = eval.clone();
                    if let Some(matched) = &matched {
                        for item in eval.items.iter_mut() {
                            guard_scalar(&mut item.scalar, matched)?;
                        }
                    }
                    RelOperator::EvalScalar(eval)
                }
                _ => unreachable!("only filters and projections are kept"),
            };
            s_expr = SExpr::create_unary(Arc::new(op), Arc::new(s_expr));
        }

        Ok(Some(ScalarAggregateRewrite {
            s_expr,
            matched,
            key_columns,
        }))
    }

    /// Adds a boolean column that is true when `matched` (if any) is true and all
    /// `predicates` are true. The predicates are only evaluated on rows that are still
    /// matched. Returns the column reference and the projection producing it.
    pub(crate) fn add_match_flag(
        &mut self,
        matched: Option<&ScalarExpr>,
        predicates: Vec<ScalarExpr>,
        is_lateral: bool,
    ) -> Result<(ScalarExpr, RelOperator)> {
        let mut flag = matched.cloned();
        for predicate in predicates {
            let predicate = function_call("is_true", vec![predicate])?;
            flag = Some(match flag {
                None => predicate,
                Some(flag) => function_call("if", vec![flag, predicate, boolean(false)])?,
            });
        }
        let flag = flag.unwrap_or_else(|| boolean(true));
        let data_type = flag.data_type().into_owned();
        let name_prefix = if is_lateral { "lateral" } else { "subquery" };
        let metadata = self.ctx.get_metadata();
        let index = metadata
            .write()
            .add_derived_column(format!("{name_prefix}_matched"), data_type.clone());
        let column = column_ref(&metadata.read(), index, data_type);
        let op = RelOperator::EvalScalar(EvalScalar {
            items: vec![ScalarItem {
                scalar: flag,
                index,
            }],
        });
        Ok((column, op))
    }

    /// Decorrelates a scalar subquery over a correlated scalar aggregate, so that an outer
    /// value without aggregate input gets the subquery's empty-input result, e.g. 1 for
    /// `count(*) + 1` instead of NULL.
    ///
    /// When that result is NULL anyway, the generic plan is already correct and is kept, so
    /// common shapes such as `x > (SELECT avg(..) ..)` don't change.
    pub(crate) fn try_decorrelate_scalar_aggregate_subquery(
        &mut self,
        outer: &SExpr,
        subquery: &SubqueryExpr,
    ) -> Result<ScalarSubqueryRewrite> {
        let shape = match match_scalar_aggregate(outer, &subquery.subquery)? {
            ScalarAggregateMatch::Unsupported => return Ok(ScalarSubqueryRewrite::Unsupported),
            // The subquery yields NULL for every outer row, like the NULL-extended generic
            // plan.
            ScalarAggregateMatch::AlwaysEmpty => return Ok(ScalarSubqueryRewrite::GenericPlan),
            ScalarAggregateMatch::Shape(shape) => shape,
        };
        let func_ctx = self.ctx.get_table_ctx().get_function_context()?;
        if empty_input_result_is_null(&shape, subquery.output_column.index, &func_ctx) {
            return Ok(ScalarSubqueryRewrite::GenericPlan);
        }
        let Some(rewrite) = self.rewrite_scalar_aggregate(outer, &shape, true, false)? else {
            return Ok(ScalarSubqueryRewrite::Unsupported);
        };

        let ScalarAggregateRewrite {
            mut s_expr,
            matched,
            ..
        } = rewrite;
        // The caller reads the subquery result as a nullable column. Produce one, NULL when
        // HAVING rejects the row. `subquery.data_type` is already wrapped by the binder, so
        // check the type the rebuilt plan actually produces for the output column.
        let output = subquery.output_column.index;
        let output_type = subquery.output_column.data_type.as_ref();
        let nullable_type = output_type.wrap_nullable();
        let mut derived_columns = DerivedColumnScope::default();
        if matched.is_some() || !output_type.is_nullable_or_null() {
            let metadata = self.ctx.get_metadata();
            let value = column_ref(
                &metadata.read(),
                output,
                subquery.output_column.data_type.as_ref().clone(),
            );
            let scalar = match matched {
                Some(matched) => function_call("if", vec![
                    matched,
                    value,
                    cast(constant(Scalar::Null), nullable_type),
                ])?,
                None => cast(value, nullable_type),
            };
            let index = metadata.write().add_derived_column(
                subquery.output_column.column_name.clone(),
                scalar.data_type().into_owned(),
            );
            s_expr = SExpr::create_unary(
                Arc::new(
                    EvalScalar {
                        items: vec![ScalarItem { scalar, index }],
                    }
                    .into(),
                ),
                Arc::new(s_expr),
            );
            derived_columns.record(output, index);
        }
        Ok(ScalarSubqueryRewrite::Rewritten(Box::new((
            s_expr,
            derived_columns,
        ))))
    }
}

/// Whether the scalar subquery yields NULL when its aggregate input is empty. Then the generic
/// plan, which NULL-extends outer values without an aggregate row, is already correct.
///
/// Evaluated by constant folding with the aggregates replaced by their empty-input values.
/// Anything that doesn't fold, e.g. an expression over an outer column, counts as not NULL.
fn empty_input_result_is_null(
    shape: &ScalarAggregateShape,
    output: Symbol,
    func_ctx: &FunctionContext,
) -> bool {
    let mut values = HashMap::new();
    for (item, (empty_value, _)) in shape
        .aggregate
        .aggregate_functions
        .iter()
        .zip(shape.empty_values.iter())
    {
        let ScalarExpr::AggregateFunction(func) = &item.scalar else {
            return false;
        };
        values.insert(
            item.index,
            (empty_value.clone(), func.return_type.as_ref().clone()),
        );
    }
    for op in shape.kept.iter().rev() {
        match op {
            RelOperator::EvalScalar(eval) => {
                for item in eval.items.iter() {
                    match fold_with_values(&item.scalar, &values, func_ctx) {
                        Some(value) => {
                            values.insert(item.index, value);
                        }
                        None => {
                            values.remove(&item.index);
                        }
                    }
                }
            }
            RelOperator::Filter(filter) => {
                for predicate in filter.predicates.iter() {
                    match fold_with_values(predicate, &values, func_ctx) {
                        Some((Scalar::Boolean(true), _)) => {}
                        // HAVING removes the single row, so the subquery yields NULL.
                        Some((Scalar::Boolean(false) | Scalar::Null, _)) => return true,
                        _ => return false,
                    }
                }
            }
            _ => return false,
        }
    }
    matches!(values.get(&output), Some((Scalar::Null, _)))
}

/// Folds `scalar` with its columns replaced by the constants in `values`. Returns `None` if a
/// column has no known value or the result isn't a constant.
fn fold_with_values(
    scalar: &ScalarExpr,
    values: &HashMap<Symbol, (Scalar, DataType)>,
    func_ctx: &FunctionContext,
) -> Option<(Scalar, DataType)> {
    let mut scalar = scalar.clone();
    for column in scalar.used_columns() {
        let (value, data_type) = values.get(&column)?;
        let replacement = ScalarExpr::TypedConstantExpr(
            ConstantExpr {
                span: None,
                value: value.clone(),
            },
            data_type.clone(),
        );
        scalar
            .replace_column_with_scalar(column, &replacement)
            .ok()?;
    }
    let expr = scalar.as_expr().ok()?;
    let (expr, _) = ConstantFolder::fold(Cow::Owned(expr), func_ctx, &BUILTIN_FUNCTIONS);
    let constant = expr.into_owned().into_constant().ok()?;
    Some((constant.scalar, constant.data_type))
}

/// Makes `scalar` evaluate only on rows where `matched` is true; other rows get the
/// default value of its type, which is never observed because those rows are masked.
fn guard_scalar(scalar: &mut ScalarExpr, matched: &ScalarExpr) -> Result<()> {
    if matches!(
        scalar,
        ScalarExpr::BoundColumnRef(_) | ScalarExpr::ConstantExpr(_)
    ) {
        return Ok(());
    }
    let data_type = scalar.data_type().into_owned();
    let default = cast(constant(Scalar::default_value(&data_type)), data_type);
    let value = std::mem::replace(scalar, boolean(false));
    *scalar = function_call("if", vec![matched.clone(), value, default])?;
    Ok(())
}

/// Builds a function call whose return type is resolved from the function signature, like
/// the type checker does, instead of being written by hand.
pub(crate) fn function_call(name: &str, arguments: Vec<ScalarExpr>) -> Result<ScalarExpr> {
    let mut call = FunctionCall {
        span: None,
        func_name: name.to_string(),
        params: vec![],
        arguments,
        return_type: Box::new(DataType::Null),
    };
    call.refresh_return_type()?;
    Ok(ScalarExpr::FunctionCall(call))
}

fn cast(argument: ScalarExpr, target_type: DataType) -> ScalarExpr {
    ScalarExpr::CastExpr(CastExpr {
        span: None,
        is_try: false,
        argument: Box::new(argument),
        target_type: Box::new(target_type),
    })
}

fn constant(value: Scalar) -> ScalarExpr {
    ScalarExpr::ConstantExpr(ConstantExpr { span: None, value })
}

fn boolean(value: bool) -> ScalarExpr {
    constant(Scalar::Boolean(value))
}

fn column_ref(metadata: &Metadata, index: Symbol, data_type: DataType) -> ScalarExpr {
    ScalarExpr::BoundColumnRef(BoundColumnRef {
        span: None,
        column: ColumnBindingBuilder::new(
            metadata.column(index).name(),
            index,
            Box::new(data_type),
            Visibility::Visible,
        )
        .build(),
    })
}

/// Whether `s_expr` contains a correlated scalar aggregate (no GROUP BY).
///
/// The fill above the reconnecting join assumes that an outer value without a flattened
/// aggregate row has empty aggregate input. A nested correlated scalar aggregate breaks
/// this: its flattened groups also disappear for such values, although it returns one row
/// for each of them, so the outer aggregate's empty-input value would be wrong.
fn has_correlated_scalar_aggregate(s_expr: &SExpr) -> Result<bool> {
    if let RelOperator::Aggregate(aggregate) = s_expr.plan()
        && aggregate.group_items.is_empty()
        && !RelExpr::with_s_expr(s_expr)
            .derive_relational_prop()?
            .outer_columns
            .is_empty()
    {
        return Ok(true);
    }
    for child in s_expr.children() {
        if has_correlated_scalar_aggregate(child)? {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Whether `aggregate` is a `SELECT DISTINCT` that only groups by input columns under their
/// own indexes, so dropping it doesn't change the columns seen above it.
fn is_passthrough_distinct(aggregate: &Aggregate) -> bool {
    aggregate.from_distinct
        && aggregate.mode == AggregateMode::Initial
        && aggregate.aggregate_functions.is_empty()
        && aggregate.grouping_sets.is_none()
        && aggregate.rank_limit.is_none()
        && aggregate.group_items.iter().all(|item| {
            matches!(
                &item.scalar,
                ScalarExpr::BoundColumnRef(column) if column.column.index == item.index
            )
        })
}

fn is_true_constant(scalar: &ScalarExpr) -> bool {
    matches!(
        scalar,
        ScalarExpr::ConstantExpr(ConstantExpr {
            value: Scalar::Boolean(true),
            ..
        })
    )
}

/// Evaluates an aggregate function on empty input, e.g. `0` for `count` and `[]` for
/// `array_agg`, and tells whether a marker is needed to recognize unmatched rows.
/// Returns `None` if the function can't be evaluated here.
///
/// The function is resolved with all arguments nullable, the worst case for its return type,
/// so the result doesn't depend on the argument types recorded by the binder. If even then
/// the return type isn't nullable, the aggregate never returns NULL and no marker is needed.
///
/// No built-in aggregate currently needs the marker: those with a non-NULL empty-input value
/// (`count`, `uniq`, `approx_count_distinct`, `array_agg`, `list`, `json_agg`,
/// `json_array_agg`, `json_object_agg`, `group_array_moving_*`, `group_bitmap`,
/// `bitmap_construct_agg`, `sum_zero`) all return a non-nullable type, and the others return
/// NULL on empty input, which the LEFT join produces without a fill. The marker path is kept
/// so that a future aggregate with a nullable result and a non-NULL empty-input value is
/// still filled correctly instead of falling back to a plan that loses empty groups.
fn empty_aggregate_value(func: &AggregateFunction) -> Option<(Scalar, bool)> {
    let eval = |nullable: bool| {
        let entries = func
            .args
            .iter()
            .map(|arg| {
                let data_type = if nullable {
                    arg.data_type().wrap_nullable()
                } else {
                    arg.data_type().into_owned()
                };
                BlockEntry::from(ColumnBuilder::with_capacity(&data_type, 0).build())
            })
            .collect::<Vec<_>>();
        let (column, data_type) =
            eval_aggr(&func.func_name, func.params.clone(), &entries, 0, vec![]).ok()?;
        let value = column.index(0)?.to_owned();
        Some((value, data_type))
    };
    match eval(true) {
        Some((value, data_type)) => Some((value, data_type.is_nullable_or_null())),
        None => eval(false).map(|(value, _)| (value, true)),
    }
}
