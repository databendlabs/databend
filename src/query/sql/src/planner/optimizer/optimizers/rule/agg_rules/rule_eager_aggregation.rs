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

use databend_common_exception::ErrorCode;
use databend_common_expression::types::number::NumberDataType;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::UInt64Type;
use databend_common_functions::aggregates::AggregateFunctionFactory;

use crate::binder::wrap_cast;
use crate::binder::ColumnBindingBuilder;
use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::SExpr;
use crate::optimizer::ir::Side;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::Aggregate;
use crate::plans::AggregateFunction;
use crate::plans::AggregateMode;
use crate::plans::BoundColumnRef;
use crate::plans::EvalScalar;
use crate::plans::FunctionCall;
use crate::plans::JoinType;
use crate::plans::RelOp;
use crate::plans::ScalarItem;
use crate::ColumnSet;
use crate::IndexType;
use crate::MetadataRef;
use crate::ScalarExpr;
use crate::Visibility;

/// Rule to push aggregation past a join to reduces the number of input rows to the join.
/// Read the paper "Eager aggregation and lazy aggregation" for more details.
///
/// Input:
///                 expression
///                     |
///              aggregate: SUM(x)
///                     |
///                    join
///                     | \
///                     |  *
///                     |
///                    (x)
///
/// Output:
/// (1) Eager Group-By:
///                 expression
///                     |
///      final aggregate: SUM(eager SUM(x))
///                     |
///                    join
///                     | \
///                     |  *
///                     |
///                     eager group-by: eager SUM(x)
///
/// (2) Eager Count:
///                 expression
///                     |
///        final aggregate: SUM(x * cnt)
///                     |
///                    join
///                     | \
///                     |  eager count: cnt
///                     |
///                    (x)
///
/// (3) Double Eager:
///                 expression
///                     |
///    final aggregate: SUM(eager SUM(x) * cnt)
///                     |
///                     join
///                     | \
///                     |   eager count: cnt
///                     |
///                     eager group-by: eager SUM(x)
///
/// Input:
///                 expression
///                     |
///          aggregate: SUM(x), SUM(y)
///                     |
///                    join
///                     | \
///                     | (y)
///                     |
///                    (x)
///
/// (1) Eager Groupby-Count:
///                 expression
///                     |
///      final aggregate: SUM(eager SUM(x)), SUM(y * cnt)
///                     |
///                     join
///                     | \
///                     | (y)
///                     |
///                     eager group-by: eager SUM(x), eager count: cnt
///
/// (2) Eager Split:
///                 expression
///                     |
///      final aggregate: SUM(eager SUM(x) * cnt2), SUM(eager SUM(y) * cnt1)
///                     |
///                     join
///                     | \
///                     | eager group-by: eager SUM(y), eager count: cnt2
///                     |
///                     eager group-by: eager SUM(x), eager count: cnt1
pub struct RuleEagerAggregation {
    matchers: Vec<Matcher>,
    metadata: MetadataRef,
}

impl RuleEagerAggregation {
    pub fn new(metadata: MetadataRef) -> Self {
        Self {
            // There are multiple patterns for EagerAggregation, we will match them
            // in the `apply` function, so the pattern here only contains Expression.
            // Expression
            //     |
            //     *
            matchers: vec![
                //     Expression
                //         |
                //  Aggregate(final)
                //         |
                // Aggregate(partial)
                //         |
                //        Join
                //       /    \
                //      *      *
                Matcher::MatchOp {
                    op_type: RelOp::EvalScalar,
                    children: vec![Matcher::MatchOp {
                        op_type: RelOp::Aggregate,
                        children: vec![Matcher::MatchOp {
                            op_type: RelOp::Aggregate,
                            children: vec![Matcher::MatchOp {
                                op_type: RelOp::Join,
                                children: vec![Matcher::Leaf, Matcher::Leaf],
                            }],
                        }],
                    }],
                },
                //     Expression
                //         |
                //  Aggregate(final)
                //         |
                // Aggregate(partial)
                //         |
                //     Expression
                //         |
                //        Join
                //       /    \
                //      *      *
                Matcher::MatchOp {
                    op_type: RelOp::EvalScalar,
                    children: vec![Matcher::MatchOp {
                        op_type: RelOp::Aggregate,
                        children: vec![Matcher::MatchOp {
                            op_type: RelOp::Aggregate,
                            children: vec![Matcher::MatchOp {
                                op_type: RelOp::EvalScalar,
                                children: vec![Matcher::MatchOp {
                                    op_type: RelOp::Join,
                                    children: vec![Matcher::Leaf, Matcher::Leaf],
                                }],
                            }],
                        }],
                    }],
                },
                //     Expression
                //         |
                //        Sort
                //         |
                //  Aggregate(final)
                //         |
                // Aggregate(partial)
                //         |
                //        Join
                //       /    \
                //      *      *
                Matcher::MatchOp {
                    op_type: RelOp::EvalScalar,
                    children: vec![Matcher::MatchOp {
                        op_type: RelOp::Sort,
                        children: vec![Matcher::MatchOp {
                            op_type: RelOp::Aggregate,
                            children: vec![Matcher::MatchOp {
                                op_type: RelOp::Aggregate,
                                children: vec![Matcher::MatchOp {
                                    op_type: RelOp::Join,
                                    children: vec![Matcher::Leaf, Matcher::Leaf],
                                }],
                            }],
                        }],
                    }],
                },
                //     Expression
                //         |
                //        Sort
                //         |
                //  Aggregate(final)
                //         |
                // Aggregate(partial)
                //         |
                //     Expression
                //         |
                //        Join
                //       /    \
                //      *      *
                Matcher::MatchOp {
                    op_type: RelOp::EvalScalar,
                    children: vec![Matcher::MatchOp {
                        op_type: RelOp::Sort,
                        children: vec![Matcher::MatchOp {
                            op_type: RelOp::Aggregate,
                            children: vec![Matcher::MatchOp {
                                op_type: RelOp::Aggregate,
                                children: vec![Matcher::MatchOp {
                                    op_type: RelOp::EvalScalar,
                                    children: vec![Matcher::MatchOp {
                                        op_type: RelOp::Join,
                                        children: vec![Matcher::Leaf, Matcher::Leaf],
                                    }],
                                }],
                            }],
                        }],
                    }],
                },
            ],
            metadata,
        }
    }
}

impl Rule for RuleEagerAggregation {
    fn id(&self) -> RuleID {
        RuleID::EagerAggregation
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<(), ErrorCode> {
        let i = self
            .matchers
            .iter()
            .position(|matcher| matcher.matches(s_expr))
            .unwrap();
        self.apply_matcher(i, s_expr, state)
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }

    fn transformation(&self) -> bool {
        false
    }

    fn apply_matcher(
        &self,
        i: usize,
        s_expr: &SExpr,
        state: &mut TransformResult,
    ) -> Result<(), ErrorCode> {
        let eval_scalar_expr = s_expr;
        let (sort_expr, final_agg_expr, has_extra_eval) = {
            let next = s_expr.unary_child();
            match i {
                0 => (None, next, false),
                1 => (None, next, true),
                2 => (Some(next), next.unary_child(), false),
                3 => (Some(next), next.unary_child(), true),
                _ => unreachable!(),
            }
        };

        let final_agg_partial_expr = final_agg_expr.unary_child();
        let (extra_eval_scalar_expr, join_expr) = if has_extra_eval {
            let next = final_agg_partial_expr.unary_child();
            (Some(next), next.unary_child())
        } else {
            (None, final_agg_partial_expr.unary_child())
        };

        let join = join_expr.plan().as_join().unwrap();
        // Only supports inner/cross join and equal conditions.
        if !matches!(join.join_type, JoinType::Inner | JoinType::Cross)
            | !join.non_equi_conditions.is_empty()
        {
            return Ok(());
        }

        let eval_scalar = eval_scalar_expr.plan().as_eval_scalar().unwrap();
        let final_agg = final_agg_expr.plan().as_aggregate().unwrap();

        // Get the original column set from the left child and right child of join.
        let mut join_columns = Pair::try_new_with(|side| {
            Ok::<_, ErrorCode>(
                side.child(join_expr)
                    .derive_relational_prop()?
                    .output_columns
                    .clone(),
            )
        })?;

        let extra_eval_scalar = match extra_eval_scalar_expr {
            Some(expr) => expr.plan().clone().into_eval_scalar().unwrap(),
            None => EvalScalar { items: vec![] },
        };
        // Check if all extra eval scalars can be solved by one of the children.
        let mut eager_extra_eval_scalar_expr = Pair {
            left: EvalScalar { items: vec![] },
            right: EvalScalar { items: vec![] },
        };
        if extra_eval_scalar_expr.is_some() {
            for eval_item in &extra_eval_scalar.items {
                let eval_used_columns = eval_item.scalar.used_columns();
                let mut resolved_by_one_child = false;
                join_columns.for_each_mut(|side, columns_set| {
                    if eval_used_columns.is_subset(columns_set) {
                        eager_extra_eval_scalar_expr[side]
                            .items
                            .push(eval_item.clone());
                        columns_set.insert(eval_item.index);
                        resolved_by_one_child = true;
                    }
                });
                if !resolved_by_one_child {
                    return Ok(());
                }
            }
        }

        let eval_scalar_columns = eval_scalar
            .items
            .iter()
            .filter_map(|item| {
                if let ScalarExpr::BoundColumnRef(column) = &item.scalar {
                    Some(column.column.index)
                } else {
                    None
                }
            })
            .collect::<ColumnSet>();

        // Get all eager aggregate functions of the left child and right child.
        let function_factory = AggregateFunctionFactory::instance();
        let eager_aggregations = Pair::new_with(|side| {
            get_eager_aggregation_functions(
                final_agg,
                &join_columns[side],
                &eval_scalar_columns,
                function_factory,
            )
        });

        // There are some aggregate functions that cannot be pushed down, if a func(x)
        // cannot be pushed down, it means that we need to add x to the group by items
        // of eager aggregation, it will change the semantics of the sql, So we should
        // stop eager aggregation.
        if eager_aggregations[Side::Left].len() + eager_aggregations[Side::Right].len()
            != final_agg.aggregate_functions.len()
        {
            return Ok(());
        }

        // Divide group by columns into two parts, where the left set comes from the left
        // child and the right set comes from the right child.
        let mut eager_group_columns = Pair {
            left: ColumnSet::new(),
            right: ColumnSet::new(),
        };
        for group_item in &final_agg.group_items {
            let ScalarExpr::BoundColumnRef(_) = &group_item.scalar else {
                return Ok(());
            };
            join_columns.for_each(|side, join_columns| {
                if join_columns.contains(&group_item.index) {
                    eager_group_columns[side].insert(group_item.index);
                }
            });
        }

        // Using join conditions to propagate group item to another child.
        let (left_conditions, right_conditions): (Vec<_>, _) = join
            .equi_conditions
            .iter()
            .map(|condition| (&condition.left, &condition.right))
            .unzip();
        let join_conditions = Pair {
            left: left_conditions,
            right: right_conditions,
        };
        let original_group_items_len = final_agg.group_items.len();
        let mut final_agg = final_agg.clone();

        for condition in &join.equi_conditions {
            let ScalarExpr::BoundColumnRef(left_column) = &condition.left else {
                return Ok(());
            };
            let ScalarExpr::BoundColumnRef(right_column) = &condition.right else {
                return Ok(());
            };
            let cond_column = Pair {
                left: left_column.column.index,
                right: right_column.column.index,
            };
            cond_column.for_each(|side, index| {
                let opposite = side.opposite();
                if eager_group_columns[side].contains(index)
                    && !eager_group_columns[opposite].contains(&cond_column[opposite])
                {
                    final_agg.group_items.push(ScalarItem {
                        scalar: opposite.join_condition(condition).clone(),
                        index: cond_column[opposite],
                    });
                    eager_group_columns[opposite].insert(cond_column[opposite]);
                }
            });
        }

        // If a child's `can_eager` is true, its group_columns_set should include all
        // join conditions related to the child.
        let can_eager = Pair::new_with(|side| {
            join_conditions[side]
                .iter()
                .all(|cond| cond.used_columns().is_subset(&eager_group_columns[side]))
        });

        if !can_eager[Side::Left] && !can_eager[Side::Right] {
            return Ok(());
        }

        let mut ctx = EagerContext {
            final_agg,
            eval_scalar,
            join_expr,
            sort_expr,
            extra_eval_scalar,
            metadata: &self.metadata,
            can_eager,
            eager_aggregations,
            eager_extra_eval_scalar_expr,
            join_columns,
            original_group_items_len,
        };

        for mut result in ctx.apply()? {
            result.set_applied_rule(&self.id());
            state.add_result(result);
        }

        Ok(())
    }
}

struct EagerContext<'a> {
    final_agg: Aggregate,
    eval_scalar: &'a EvalScalar,
    join_expr: &'a SExpr,
    sort_expr: Option<&'a SExpr>,
    extra_eval_scalar: EvalScalar,
    metadata: &'a MetadataRef,
    can_eager: Pair<bool>,
    eager_aggregations: Pair<Vec<(usize, IndexType)>>,
    eager_extra_eval_scalar_expr: Pair<EvalScalar>,
    join_columns: Pair<ColumnSet>,
    original_group_items_len: usize,
}

impl<'a> EagerContext<'a> {
    fn apply(&mut self) -> Result<Vec<SExpr>, ErrorCode> {
        let can_push_down = Pair {
            left: !self.eager_aggregations[Side::Left].is_empty() && self.can_eager[Side::Left],
            right: !self.eager_aggregations[Side::Right].is_empty() && self.can_eager[Side::Right],
        };

        let mut results = Vec::new();

        if can_push_down[Side::Left]
            && can_push_down[Side::Right]
            && !self.apply_eager_split_and_groupby_count(Side::Left, &mut results)?
        {
            return Ok(vec![]);
        }

        let d = if can_push_down[Side::Left] {
            Side::Left
        } else {
            Side::Right
        };
        if can_push_down[d]
            && self.eager_aggregations[d.opposite()].is_empty()
            && !self.apply_single_side_push(d, &mut results)?
        {
            return Ok(vec![]);
        }

        Ok(results)
    }

    fn apply_eager_split_and_groupby_count(
        &mut self,
        d: Side,
        results: &mut Vec<SExpr>,
    ) -> Result<bool, ErrorCode> {
        let original_final_agg = self.final_agg.clone();
        let mut final_eager_split = self.final_agg.clone();

        let mut count_to_be_removed = Vec::new();
        let mut func_to_be_restored = Vec::new();

        let eager_count_indexes = Pair::new_with(|side| {
            let (index, vec_index) = add_eager_count(&mut final_eager_split, self.metadata);
            self.eager_aggregations[side].push((vec_index, index));
            (index, vec_index)
        });

        let mut eager_group_by_and_eager_count = Pair {
            left: final_eager_split.clone(),
            right: final_eager_split.clone(),
        };

        let mut eager_groupby_count_scalar = self.eval_scalar.clone();
        let mut eager_split_eval_scalar = self.eval_scalar.clone();

        let mut old_to_new = HashMap::new();
        let mut func_from: HashMap<usize, Side> = HashMap::new();
        let mut success = false;

        eager_count_indexes.try_for_each(|side, (_, idx)| {
            for (index, _) in &self.eager_aggregations[side] {
                if idx == index {
                    continue;
                }

                let aggr_function = &mut final_eager_split.aggregate_functions[*index];
                let eval_scalars_items =
                    eager_split_eval_scalar
                        .items
                        .iter_mut()
                        .chain(if side == d {
                            eager_groupby_count_scalar.items.iter_mut()
                        } else {
                            [].iter_mut()
                        });
                let (cur_success, old_index, new_index) =
                    update_aggregate_and_eval(aggr_function, self.metadata, eval_scalars_items)?;
                success |= cur_success;

                old_to_new.insert(old_index, new_index);
                func_from.insert(old_index, side);
                func_from.insert(new_index, side);

                if side == d.opposite() {
                    func_to_be_restored.push((new_index, *index));
                }
            }
            Ok::<_, ErrorCode>(())
        })?;

        eager_count_indexes.for_each(|_, (eager_count_index, _)| {
            final_eager_split
                .aggregate_functions
                .retain(|aggr| aggr.index != *eager_count_index);
        });

        let mut final_eager_groupby_count = final_eager_split.clone();
        for i in &mut count_to_be_removed {
            *i = old_to_new[i];
        }
        final_eager_groupby_count
            .aggregate_functions
            .retain(|agg| !count_to_be_removed.contains(&agg.index));

        for (index, idx) in func_to_be_restored {
            if let Some(aggr) = final_eager_groupby_count
                .aggregate_functions
                .iter_mut()
                .find(|aggr| {
                    aggr.index == index && idx < original_final_agg.aggregate_functions.len()
                })
            {
                *aggr = original_final_agg.aggregate_functions[idx].clone();
            }
        }

        let mut eager_split_count_sum = EvalScalar { items: vec![] };
        let mut eager_groupby_count_count_sum = EvalScalar { items: vec![] };
        {
            let mut plans_and_counts = Pair {
                left: (&mut final_eager_split, &mut eager_split_count_sum),
                right: (
                    &mut final_eager_groupby_count,
                    &mut eager_groupby_count_count_sum,
                ),
            };
            plans_and_counts.try_for_each_mut(|side, (plan, count_sum)| {
                for agg in plan.aggregate_functions.iter_mut() {
                    if let ScalarExpr::AggregateFunction(aggregate_function) = &mut agg.scalar {
                        if aggregate_function.func_name != "sum"
                            || (side == Side::Right && func_from[&agg.index] == d)
                        {
                            continue;
                        }
                        count_sum
                            .items
                            .push(create_eager_count_multiply_scalar_item(
                                aggregate_function,
                                eager_count_indexes[func_from[&agg.index].opposite()].0,
                                &self.extra_eval_scalar,
                                self.metadata,
                            )?);
                    }
                }
                Ok::<_, ErrorCode>(())
            })?;
        }

        if !success {
            return Ok(false);
        }

        eager_group_by_and_eager_count.for_each_mut(|side, plan| {
            remove_group_by_items_and_aggregate_functions(
                plan,
                &self.join_columns[side],
                &self.eager_aggregations[side.opposite()],
            );
        });

        let groupby_count_join = self.join_expr.replace_side_child(
            d,
            d.build_join_child(
                self.join_expr,
                &self.eager_extra_eval_scalar_expr[d],
                eager_group_by_and_eager_count[d].clone(),
            ),
        );

        self.push_result(
            results,
            groupby_count_join
                .clone()
                .build_unary(eager_groupby_count_count_sum),
            final_eager_groupby_count,
            eager_groupby_count_scalar,
        );

        let opposite = d.opposite();
        let eager_split_join = groupby_count_join
            .replace_side_child(
                opposite,
                opposite.build_join_child(
                    self.join_expr,
                    &self.eager_extra_eval_scalar_expr[opposite],
                    eager_group_by_and_eager_count[opposite].clone(),
                ),
            )
            .build_unary(eager_split_count_sum);
        self.push_result(
            results,
            eager_split_join,
            final_eager_split,
            eager_split_eval_scalar,
        );

        Ok(true)
    }

    fn apply_single_side_push(
        &mut self,
        d: Side,
        results: &mut Vec<SExpr>,
    ) -> Result<bool, ErrorCode> {
        let mut final_double_eager = self.final_agg.clone();
        let mut final_eager_count = self.final_agg.clone();

        let mut eager_count_index = 0;
        let mut eager_count_vec_idx = 0;
        if self.can_eager[d.opposite()] {
            (eager_count_index, eager_count_vec_idx) =
                add_eager_count(&mut final_double_eager, self.metadata);
            self.eager_aggregations[d.opposite()].push((eager_count_vec_idx, eager_count_index));
        }

        let mut eager_group_by = final_double_eager.clone();
        let mut eager_count = final_double_eager.clone();
        let mut double_eager_eval_scalar = self.eval_scalar.clone();
        let mut old_to_new = HashMap::new();
        let mut success = false;

        for (index, _) in &self.eager_aggregations[d] {
            if self.can_eager[d.opposite()] && eager_count_vec_idx == *index {
                continue;
            }
            let aggr_function = &mut final_double_eager.aggregate_functions[*index];
            let (cur_success, old_index, new_index) = update_aggregate_and_eval(
                aggr_function,
                self.metadata,
                double_eager_eval_scalar.items.iter_mut(),
            )?;
            success |= cur_success;
            old_to_new.insert(old_index, new_index);
        }

        if self.can_eager[d.opposite()] {
            final_double_eager
                .aggregate_functions
                .remove(eager_count_vec_idx);
        }

        let final_eager_group_by = final_double_eager.clone();

        let mut need_eager_count = false;
        let mut double_eager_count_sum = EvalScalar { items: vec![] };
        let mut eager_count_sum = EvalScalar { items: vec![] };
        if self.can_eager[d.opposite()] {
            let mut plans_and_counts = Pair {
                left: (&mut final_double_eager, &mut double_eager_count_sum),
                right: (&mut final_eager_count, &mut eager_count_sum),
            };
            plans_and_counts.try_for_each_mut(|_, (plan, sum)| {
                for agg in plan.aggregate_functions.iter_mut() {
                    if let ScalarExpr::AggregateFunction(aggregate_function) = &mut agg.scalar {
                        if aggregate_function.func_name == "sum" {
                            sum.items.push(create_eager_count_multiply_scalar_item(
                                aggregate_function,
                                eager_count_index,
                                &self.extra_eval_scalar,
                                self.metadata,
                            )?);
                            need_eager_count = true;
                        }
                    }
                }
                Ok::<_, ErrorCode>(())
            })?;
        }

        if !success {
            return Ok(false);
        }

        let eager_group_by_eval_scalar = double_eager_eval_scalar.clone();

        remove_group_by_items_and_aggregate_functions(
            &mut eager_group_by,
            &self.join_columns[d],
            &self.eager_aggregations[d.opposite()],
        );
        if self.can_eager[d.opposite()] && need_eager_count {
            remove_group_by_items_and_aggregate_functions(
                &mut eager_count,
                &self.join_columns[d.opposite()],
                &self.eager_aggregations[d],
            );
        }

        let group_by_child = Arc::new(d.build_join_child(
            self.join_expr,
            &self.eager_extra_eval_scalar_expr[d],
            eager_group_by.clone(),
        ));

        self.push_result(
            results,
            self.join_expr.replace_side_child(d, group_by_child.clone()),
            final_eager_group_by,
            eager_group_by_eval_scalar,
        );

        if self.can_eager[d.opposite()] && need_eager_count {
            let new_join = self.join_expr.replace_side_child(
                d.opposite(),
                d.opposite().build_join_child(
                    self.join_expr,
                    &EvalScalar { items: vec![] },
                    eager_count,
                ),
            );

            self.push_result(
                results,
                new_join.clone().build_unary(eager_count_sum),
                final_eager_count,
                self.eval_scalar.clone(),
            );

            self.push_result(
                results,
                new_join
                    .replace_side_child(d, group_by_child)
                    .build_unary(double_eager_count_sum),
                final_double_eager,
                double_eager_eval_scalar,
            );
        }

        Ok(true)
    }

    fn push_result(
        &self,
        results: &mut Vec<SExpr>,
        s_expr: SExpr,
        mut final_aggr: Aggregate,
        eval_scalar: EvalScalar,
    ) {
        final_aggr
            .group_items
            .truncate(self.original_group_items_len);
        let plan = s_expr
            .build_unary(Aggregate {
                mode: AggregateMode::Partial,
                ..final_aggr.clone()
            })
            .build_unary(final_aggr);
        let plan = match self.sort_expr {
            Some(sort_expr) => plan.build_unary(sort_expr.plan.clone()),
            None => plan,
        };
        results.push(plan.build_unary(eval_scalar));
    }
}

// In the current implementation, if an aggregation function can be eager,
// then it needs to satisfy the following constraints:
// (1) The args.len() must equal to 1 if func_name is not "count".
// (2) The aggregate function can be decomposed.
// (3) The index of the aggregate function can be found in the column source of the eval scalar.
// (4) The data type of the aggregate column is either Number or Nullable(Number).
// Return the Vec(index, func index) for each eager aggregation function.
fn get_eager_aggregation_functions(
    agg_final: &Aggregate,
    join_columns: &ColumnSet,
    eval_scalar_columns: &ColumnSet,
    function_factory: &AggregateFunctionFactory,
) -> Vec<(usize, IndexType)> {
    agg_final
        .aggregate_functions
        .iter()
        .enumerate()
        .filter_map(|(index, func)| {
            let ScalarExpr::AggregateFunction(aggregate_function) = &func.scalar else {
                return None;
            };
            let [ScalarExpr::BoundColumnRef(column)] = &aggregate_function.args[..] else {
                return None;
            };
            match &*column.column.data_type {
                DataType::Number(_) | DataType::Decimal(_) => (),
                DataType::Nullable(box DataType::Number(_) | box DataType::Decimal(_)) => {}
                _ => return None,
            }
            if !function_factory.is_decomposable(&aggregate_function.func_name)
                || !eval_scalar_columns.contains(&func.index)
                || !join_columns.contains(&column.column.index)
            {
                return None;
            }
            Some((index, func.index))
        })
        .collect()
}

// Final aggregate functions's data type = eager aggregate functions's return_type
// For COUNT, func_name: count => sum, return_type: Nullable(UInt64)
fn modify_final_aggregate_function(agg: &mut AggregateFunction, old_index: usize) {
    if agg.func_name.as_str() == "count" {
        agg.func_name = "sum".to_string();
        agg.return_type = Box::new(UInt64Type::data_type().wrap_nullable());
    }
    let agg_func = ScalarExpr::BoundColumnRef(BoundColumnRef {
        span: None,
        column: ColumnBindingBuilder::new(
            "_eager".to_string(),
            old_index,
            agg.return_type.clone(),
            Visibility::Visible,
        )
        .build(),
    });
    if agg.args.is_empty() {
        // eager count
        agg.args.push(agg_func);
    } else {
        agg.args[0] = agg_func;
    }
}

fn remove_group_by_items_and_aggregate_functions(
    agg_final: &mut Aggregate,
    columns_set: &ColumnSet,
    eager_aggregations: &[(usize, usize)],
) {
    // Remove group by items.
    agg_final
        .group_items
        .retain(|item| columns_set.contains(&item.index));

    // Remove aggregate functions.
    agg_final
        .aggregate_functions
        .retain(|item| eager_aggregations.iter().all(|(_, i)| *i != item.index));
}

fn add_eager_count(final_agg: &mut Aggregate, metadata: &MetadataRef) -> (usize, usize) {
    let eager_count_vec_index = final_agg.aggregate_functions.len();
    let eager_count_column = metadata.write().add_derived_column(
        "count(*)".to_string(),
        DataType::Number(NumberDataType::UInt64),
    );
    final_agg.aggregate_functions.push(ScalarItem {
        index: eager_count_column,
        scalar: ScalarExpr::AggregateFunction(AggregateFunction {
            span: None,
            func_name: "count".to_string(),
            distinct: false,
            params: vec![],
            args: vec![],
            return_type: Box::new(DataType::Number(NumberDataType::UInt64)),
            sort_descs: vec![],
            display_name: "count(*)".to_string(),
        }),
    });
    (eager_count_column, eager_count_vec_index)
}

fn update_aggregate_and_eval<'a>(
    aggr_function: &mut ScalarItem,
    metadata: &MetadataRef,
    eval_scalar_items: impl Iterator<Item = &'a mut ScalarItem>,
) -> Result<(bool, usize, usize), ErrorCode> {
    let ScalarExpr::AggregateFunction(agg) = &mut aggr_function.scalar else {
        unreachable!()
    };

    let old_index = aggr_function.index;
    let new_index = metadata.write().add_derived_column(
        format!("_eager_final_{}", agg.func_name),
        *(agg.return_type).clone(),
    );

    // Modify final aggregate functions.
    // final_aggregate_functions is currently a clone of eager aggregation.
    modify_final_aggregate_function(agg, old_index);
    // final_aggregate_functions is a final aggregation now.
    aggr_function.index = new_index;

    let mut success = false;

    // Modify the eval scalars of all aggregate functions.
    for scalar_item in eval_scalar_items {
        if let ScalarExpr::BoundColumnRef(column) = &mut scalar_item.scalar {
            if column.column.index != old_index {
                continue;
            }
            // If it's already a column, we can just update the column_binding index
            let column_binding = &mut column.column;
            column_binding.index = new_index;

            if agg.func_name == "count" {
                column_binding.data_type = Box::new(UInt64Type::data_type().wrap_nullable());
                scalar_item.scalar = wrap_cast(&scalar_item.scalar, &UInt64Type::data_type());
            }
            success = true;
        } else {
            // Otherwise, we need to replace the column index recursively.
            scalar_item.scalar.replace_column(old_index, new_index)?;
        }
    }
    Ok((success, old_index, new_index))
}

fn create_eager_count_multiply_scalar_item(
    aggregate_function: &mut AggregateFunction,
    eager_count_index: IndexType,
    extra_eval_scalar: &EvalScalar,
    metadata: &MetadataRef,
) -> Result<ScalarItem, ErrorCode> {
    let new_index = metadata.write().add_derived_column(
        format!("{} * _eager_count", aggregate_function.display_name),
        aggregate_function.args[0].data_type()?,
    );

    let new_scalar = if let ScalarExpr::BoundColumnRef(column) = &aggregate_function.args[0] {
        let mut scalar_idx = extra_eval_scalar.items.len();
        for (idx, eval_scalar) in extra_eval_scalar.items.iter().enumerate() {
            if eval_scalar.index == column.column.index {
                scalar_idx = idx;
            }
        }
        if scalar_idx != extra_eval_scalar.items.len() {
            extra_eval_scalar.items[scalar_idx].scalar.clone()
        } else {
            aggregate_function.args[0].clone()
        }
    } else {
        unreachable!()
    };

    let new_scalar_item = ScalarItem {
        scalar: ScalarExpr::FunctionCall(FunctionCall {
            span: None,
            func_name: "multiply".to_string(),
            params: vec![],
            arguments: vec![
                new_scalar,
                wrap_cast(
                    &ScalarExpr::BoundColumnRef(BoundColumnRef {
                        span: None,
                        column: ColumnBindingBuilder::new(
                            "_eager_count".to_string(),
                            eager_count_index,
                            Box::new(DataType::Nullable(Box::new(DataType::Number(
                                NumberDataType::UInt64,
                            )))),
                            Visibility::Visible,
                        )
                        .build(),
                    }),
                    &DataType::Number(NumberDataType::UInt64),
                ),
            ],
        }),
        index: new_index,
    };
    if let ScalarExpr::BoundColumnRef(column) = &mut aggregate_function.args[0] {
        column.column.index = new_index;
        column.column.data_type = Box::new(new_scalar_item.scalar.data_type()?);
    }
    Ok(new_scalar_item)
}

impl Side {
    fn build_join_child(
        self,
        join_expr: &SExpr,
        extra_eval_scalar: &EvalScalar,
        aggregate: Aggregate,
    ) -> SExpr {
        if !extra_eval_scalar.items.is_empty() {
            self.child(join_expr)
                .ref_build_unary(extra_eval_scalar.clone())
                .into()
        } else {
            self.child(join_expr)
        }
        .ref_build_unary(Aggregate {
            mode: AggregateMode::Partial,
            ..aggregate.clone()
        })
        .build_unary(aggregate)
    }
}

#[derive(Clone, Debug)]
struct Pair<T> {
    left: T,
    right: T,
}

impl<T> Pair<T> {
    fn new_with<F>(mut f: F) -> Self
    where F: FnMut(Side) -> T {
        let left = f(Side::Left);
        let right = f(Side::Right);
        Self { left, right }
    }

    fn try_new_with<F, E>(mut f: F) -> Result<Self, E>
    where F: FnMut(Side) -> Result<T, E> {
        let left = f(Side::Left)?;
        let right = f(Side::Right)?;
        Ok(Self { left, right })
    }

    fn for_each<F>(&self, mut f: F)
    where F: FnMut(Side, &T) {
        f(Side::Left, &self.left);
        f(Side::Right, &self.right);
    }

    fn for_each_mut<F>(&mut self, mut f: F)
    where F: FnMut(Side, &mut T) {
        f(Side::Left, &mut self.left);
        f(Side::Right, &mut self.right);
    }

    fn try_for_each<F, E>(&self, mut f: F) -> Result<(), E>
    where F: FnMut(Side, &T) -> Result<(), E> {
        f(Side::Left, &self.left)?;
        f(Side::Right, &self.right)?;
        Ok(())
    }

    fn try_for_each_mut<F, E>(&mut self, mut f: F) -> Result<(), E>
    where F: FnMut(Side, &mut T) -> Result<(), E> {
        f(Side::Left, &mut self.left)?;
        f(Side::Right, &mut self.right)?;
        Ok(())
    }
}

impl<T> std::ops::Index<Side> for Pair<T> {
    type Output = T;

    fn index(&self, side: Side) -> &Self::Output {
        match side {
            Side::Left => &self.left,
            Side::Right => &self.right,
        }
    }
}

impl<T> std::ops::IndexMut<Side> for Pair<T> {
    fn index_mut(&mut self, side: Side) -> &mut Self::Output {
        match side {
            Side::Left => &mut self.left,
            Side::Right => &mut self.right,
        }
    }
}
