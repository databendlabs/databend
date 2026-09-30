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
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_functions::aggregates::eval_aggr;

use super::bind_join::RightColumnReplacements;
use crate::ColumnSet;
use crate::Metadata;
use crate::Symbol;
use crate::binder::BindContext;
use crate::binder::ColumnBindingBuilder;
use crate::binder::Visibility;
use crate::optimizer::OptimizerContext;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::operator::FlattenInfo;
use crate::optimizer::optimizers::operator::SubqueryDecorrelatorOptimizer;
use crate::planner::binder::Binder;
use crate::plans::AggregateFunction;
use crate::plans::AggregateMode;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::ConstantExpr;
use crate::plans::EvalScalar;
use crate::plans::Filter;
use crate::plans::FunctionCall;
use crate::plans::Join;
use crate::plans::JoinEquiCondition;
use crate::plans::JoinType;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::SubqueryComparisonOp;

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

impl Binder {
    /// Binds a correlated lateral subquery whose result is a scalar aggregate (no GROUP BY),
    /// optionally followed by row-preserving operators such as projections and HAVING.
    ///
    /// Such a subquery returns exactly one aggregate row for every outer row, even when its
    /// input is empty. The generic flattening groups the aggregate by the correlated columns,
    /// so outer values without any input row lose their group. Here the flattened aggregate is
    /// LEFT joined back to the outer side, aggregates whose empty-input value isn't NULL are
    /// filled with that value, and the operators above the aggregate are evaluated after the
    /// join, where both outer columns and the filled aggregate values are available.
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

        // Collect the operators above the scalar aggregate, top-down.
        let mut kept = Vec::new();
        let mut current = right_child;
        let aggregate = loop {
            match current.plan() {
                RelOperator::EvalScalar(eval) => {
                    if current.plan().has_subquery() {
                        return Ok(None);
                    }
                    kept.push(RelOperator::EvalScalar(eval.clone()));
                }
                RelOperator::Filter(filter) => {
                    if current.plan().has_subquery() {
                        return Ok(None);
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
                    if limit.offset != 0 || limit.limit == Some(0) {
                        return Ok(None);
                    }
                }
                RelOperator::Aggregate(aggregate) => break aggregate,
                _ => return Ok(None),
            }
            current = current.unary_child();
        };
        if aggregate.mode != AggregateMode::Initial
            || !aggregate.group_items.is_empty()
            || aggregate.grouping_sets.is_some()
            || aggregate.rank_limit.is_some()
        {
            return Ok(None);
        }
        let aggregate_input = current.unary_child();

        // If the aggregate input is uncorrelated, the aggregate already yields one row and
        // the generic flattening handles it.
        let left_prop = RelExpr::with_s_expr(left_child).derive_relational_prop()?;
        let right_prop = RelExpr::with_s_expr(right_child).derive_relational_prop()?;
        let correlated_columns = RelExpr::with_s_expr(aggregate_input)
            .derive_relational_prop()?
            .outer_columns
            .clone();
        if correlated_columns.is_empty()
            || !right_prop
                .outer_columns
                .is_subset(&left_prop.output_columns)
            || has_correlated_scalar_aggregate(aggregate_input)?
        {
            return Ok(None);
        }

        // Give aggregates that need a fill a new output column; the original column is
        // produced by the fill projection above the join.
        let mut aggregate = aggregate.clone();
        let mut fills = Vec::new();
        for item in aggregate.aggregate_functions.iter_mut() {
            let ScalarExpr::AggregateFunction(func) = &item.scalar else {
                return Ok(None);
            };
            let Some((empty_value, needs_marker)) = empty_aggregate_value(func) else {
                return Ok(None);
            };
            let data_type = func.return_type.as_ref().clone();
            if matches!(empty_value, Scalar::Null) {
                if data_type.is_nullable_or_null() {
                    // The NULL produced by the LEFT join is already the empty-input result.
                    continue;
                }
                return Ok(None);
            }
            let raw = self
                .metadata
                .write()
                .add_derived_column(func.display_name.clone(), data_type.clone());
            fills.push(AggregateFill {
                output: item.index,
                raw,
                empty_value,
                data_type,
                needs_marker,
            });
            item.index = raw;
        }

        let opt_ctx = OptimizerContext::new(
            self.ctx.clone(),
            self.metadata.clone(),
            self.ctx.get_function_context()?,
        );
        let mut decorrelator = SubqueryDecorrelatorOptimizer::new(opt_ctx, Some(self.clone()));
        let aggregate_expr = SExpr::create_unary(
            Arc::new(aggregate.into()),
            Arc::new(aggregate_input.clone()),
        );
        let (mut flatten_plan, derived_columns) = decorrelator.flatten_plan(
            left_child,
            &aggregate_expr,
            &correlated_columns,
            &mut FlattenInfo {
                from_count_func: false,
            },
            false,
        )?;

        let mut outer_keys = Vec::new();
        let mut derived_keys = Vec::new();
        decorrelator.add_equi_conditions(
            None,
            &correlated_columns,
            &derived_columns,
            &mut derived_keys,
            &mut outer_keys,
        )?;
        if outer_keys.is_empty() {
            return Ok(None);
        }
        // The keys identify correlation groups, so NULL groups must match each other.
        let is_null_equal =
            SubqueryDecorrelatorOptimizer::nullable_condition_indexes(&outer_keys, &derived_keys);

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

        let mut non_lazy_columns = ColumnSet::new();
        for scalar in outer_keys.iter().chain(&derived_keys).chain(&predicates) {
            scalar.collect_used_columns(&mut non_lazy_columns);
        }
        self.metadata.write().add_non_lazy_columns(non_lazy_columns);

        // Unmatched rows are recognized by a NULL aggregate value when the aggregate never
        // returns NULL. Otherwise a constant marker on the aggregate side tells whether the
        // LEFT join found the aggregate row of an outer row.
        let marker = if fills.iter().any(|fill| fill.needs_marker) {
            let marker = self
                .metadata
                .write()
                .add_derived_column("lateral_marker".to_string(), DataType::Boolean);
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
            from_correlated_subquery: false,
            need_hold_hash_table: false,
            is_lateral: true,
            single_to_inner: None,
            build_side_cache_info: None,
            spatial_join: None,
        };
        let mut s_expr = SExpr::create_binary(
            Arc::new(join.into()),
            Arc::new(left_child.clone()),
            Arc::new(flatten_plan),
        );

        if !fills.is_empty() {
            let metadata = self.metadata.read();
            let marker = marker
                .map(|marker| column_ref(&metadata, marker, DataType::Boolean.wrap_nullable()));
            let items = fills
                .into_iter()
                .map(|fill| {
                    let nullable_type = fill.data_type.wrap_nullable();
                    let raw = column_ref(&metadata, fill.raw, nullable_type.clone());
                    let empty = ScalarExpr::CastExpr(CastExpr {
                        span: None,
                        is_try: false,
                        argument: Box::new(ScalarExpr::ConstantExpr(ConstantExpr {
                            span: None,
                            value: fill.empty_value,
                        })),
                        target_type: Box::new(nullable_type.clone()),
                    });
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
                        ScalarExpr::CastExpr(CastExpr {
                            span: None,
                            is_try: false,
                            argument: Box::new(value),
                            target_type: Box::new(fill.data_type),
                        })
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

        if join_type != JoinType::Left {
            for op in kept.into_iter().rev() {
                s_expr = SExpr::create_unary(Arc::new(op), Arc::new(s_expr));
            }
            if !predicates.is_empty() {
                s_expr =
                    SExpr::create_unary(Arc::new(Filter { predicates }.into()), Arc::new(s_expr));
            }
            return Ok(Some((s_expr, RightColumnReplacements::default())));
        }

        // LEFT lateral join: every outer row has exactly one candidate right row here, which
        // must be NULL-extended instead of removed when HAVING or the ON condition fails.
        // Each filter is turned into a match flag, combined with the flags below it.
        let mut matched: Option<ScalarExpr> = None;
        for op in kept.into_iter().rev() {
            let op = match op {
                RelOperator::Filter(filter) => {
                    let flag = self.add_match_flag(matched.as_ref(), filter.predicates)?;
                    matched = Some(flag.0);
                    flag.1
                }
                RelOperator::EvalScalar(mut eval) => {
                    // Rows whose flag is already false are NULL-extended later, so don't
                    // evaluate expressions on them: they may fail where the original plan
                    // would never have evaluated them, e.g. behind `HAVING count(*) > 0`.
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
        if !predicates.is_empty() {
            let flag = self.add_match_flag(matched.as_ref(), predicates)?;
            matched = Some(flag.0);
            s_expr = SExpr::create_unary(Arc::new(flag.1), Arc::new(s_expr));
        }
        let mut replacements = RightColumnReplacements::default();
        if let Some(matched) = matched {
            (s_expr, replacements) = self.mask_right_columns(s_expr, right_context, &matched)?;
        }
        Ok(Some((s_expr, replacements)))
    }

    /// Adds a boolean column that is true when `matched` (if any) is true and all
    /// `predicates` are true. The predicates are only evaluated on rows that are still
    /// matched. Returns the column reference and the projection producing it.
    fn add_match_flag(
        &mut self,
        matched: Option<&ScalarExpr>,
        predicates: Vec<ScalarExpr>,
    ) -> Result<(ScalarExpr, RelOperator)> {
        let mut flag = matched.cloned();
        for predicate in predicates {
            let predicate = function_call("is_true", vec![predicate])?;
            flag = Some(match flag {
                None => predicate,
                Some(flag) => function_call("if", vec![flag, predicate, false_constant()])?,
            });
        }
        let flag = flag.unwrap_or_else(|| {
            ScalarExpr::ConstantExpr(ConstantExpr {
                span: None,
                value: Scalar::Boolean(true),
            })
        });
        let data_type = flag.data_type().into_owned();
        let index = self
            .metadata
            .write()
            .add_derived_column("lateral_matched".to_string(), data_type.clone());
        let column = column_ref(&self.metadata.read(), index, data_type);
        let op = RelOperator::EvalScalar(EvalScalar {
            items: vec![ScalarItem {
                scalar: flag,
                index,
            }],
        });
        Ok((column, op))
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
    let default = ScalarExpr::CastExpr(CastExpr {
        span: None,
        is_try: false,
        argument: Box::new(ScalarExpr::ConstantExpr(ConstantExpr {
            span: None,
            value: Scalar::default_value(&data_type),
        })),
        target_type: Box::new(data_type),
    });
    let value = std::mem::replace(scalar, false_constant());
    *scalar = function_call("if", vec![matched.clone(), value, default])?;
    Ok(())
}

/// Builds a function call whose return type is resolved from the function signature, like
/// the type checker does, instead of being written by hand.
fn function_call(name: &str, arguments: Vec<ScalarExpr>) -> Result<ScalarExpr> {
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

fn false_constant() -> ScalarExpr {
    ScalarExpr::ConstantExpr(ConstantExpr {
        span: None,
        value: Scalar::Boolean(false),
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

fn is_true_constant(scalar: &ScalarExpr) -> bool {
    matches!(
        scalar,
        ScalarExpr::ConstantExpr(ConstantExpr {
            value: Scalar::Boolean(true),
            ..
        })
    )
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
