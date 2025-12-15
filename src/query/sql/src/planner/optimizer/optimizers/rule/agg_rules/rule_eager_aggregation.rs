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
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::types::number::NumberDataType;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::UInt64Type;
use databend_common_functions::aggregates::AggregateFunctionFactory;

use crate::binder::wrap_cast;
use crate::binder::ColumnBindingBuilder;
use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::SExpr;
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

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
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

    fn apply_matcher(&self, i: usize, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
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
        let left_prop = join_expr.left_child().derive_relational_prop()?;
        let right_prop = join_expr.right_child().derive_relational_prop()?;
        let mut columns_sets = Pair {
            left: left_prop.output_columns.clone(),
            right: right_prop.output_columns.clone(),
        };

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
                columns_sets.for_each_mut(|side, columns_set| {
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

        // Find all `BoundColumnRef`, and then create a mapping from the column index
        // of `BoundColumnRef` to the item's vec index.
        let mut eval_scalar_items = HashMap::<_, Vec<_>>::new();
        for (index, item) in eval_scalar.items.iter().enumerate() {
            if let ScalarExpr::BoundColumnRef(column) = &item.scalar {
                eval_scalar_items
                    .entry(column.column.index)
                    .or_default()
                    .push(index);
            }
        }

        // Get all eager aggregate functions of the left child and right child.
        let function_factory = AggregateFunctionFactory::instance();
        let eager_aggregations = {
            let left = get_eager_aggregation_functions(
                final_agg,
                &columns_sets[Side::Left],
                &eval_scalar_items,
                function_factory,
            );
            let right = get_eager_aggregation_functions(
                final_agg,
                &columns_sets[Side::Right],
                &eval_scalar_items,
                function_factory,
            );
            Pair { left, right }
        };

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
        let mut group_columns_set = Pair {
            left: ColumnSet::new(),
            right: ColumnSet::new(),
        };
        for item in &final_agg.group_items {
            let ScalarExpr::BoundColumnRef(_) = &item.scalar else {
                return Ok(());
            };
            columns_sets.for_each(|side, columns_set| {
                if columns_set.contains(&item.index) {
                    group_columns_set[side].insert(item.index);
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

        for conditions in &join.equi_conditions {
            let ScalarExpr::BoundColumnRef(left_column) = &conditions.left else {
                return Ok(());
            };
            let ScalarExpr::BoundColumnRef(right_column) = &conditions.right else {
                return Ok(());
            };
            let column_index = Pair {
                left: left_column.column.index,
                right: right_column.column.index,
            };
            column_index.for_each(|side, index| {
                if group_columns_set[side].contains(index)
                    && !group_columns_set[side.other()].contains(&column_index[side.other()])
                {
                    final_agg.group_items.push(match side.other() {
                        Side::Left => ScalarItem {
                            scalar: conditions.left.clone(),
                            index: left_column.column.index,
                        },
                        Side::Right => ScalarItem {
                            scalar: conditions.right.clone(),
                            index: right_column.column.index,
                        },
                    });
                    group_columns_set[side.other()].insert(column_index[side.other()]);
                }
            });
        }

        // If a child's `can_eager` is true, its group_columns_set should include all
        // join conditions related to the child.
        let mut can_eager = Pair {
            left: true,
            right: true,
        };
        join_conditions.for_each(|side, conditions| {
            can_eager[side] = conditions
                .iter()
                .all(|cond| cond.used_columns().is_subset(&group_columns_set[side]));
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
            eval_scalar_items,
            columns_sets,
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
    eager_aggregations: Pair<Vec<(usize, IndexType, String)>>,
    eager_extra_eval_scalar_expr: Pair<EvalScalar>,
    eval_scalar_items: HashMap<usize, Vec<usize>>,
    columns_sets: Pair<ColumnSet>,
    original_group_items_len: usize,
}

impl<'a> EagerContext<'a> {
    fn apply(&mut self) -> Result<Vec<SExpr>> {
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
            && self.eager_aggregations[d.other()].is_empty()
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
    ) -> Result<bool> {
        let function_factory = AggregateFunctionFactory::instance();
        let original_final_agg = self.final_agg.clone();
        let mut final_eager_split = self.final_agg.clone();

        let mut count_to_be_removed = Vec::new();
        let mut avg_to_be_restored = Vec::new();
        let mut func_to_be_restored = Vec::new();

        let mut eager_count_indexes = Pair {
            left: (0_usize, 0_usize),
            right: (0_usize, 0_usize),
        };
        eager_count_indexes.for_each_mut(|side, (index, vec_index)| {
            (*index, *vec_index) = add_eager_count(&mut final_eager_split, self.metadata);
            self.eager_aggregations[side].push((*vec_index, *index, "count".to_string()));
        });

        let mut avg_components = HashMap::new();
        self.eager_aggregations
            .try_for_each_mut(|side, eager_aggregation| {
                let mut new_eager_aggregations = Vec::new();
                for (index, _, func_name) in eager_aggregation.iter_mut() {
                    if func_name.as_str() != "avg" {
                        continue;
                    }
                    let (sum_index, count_index, count_vec_index) = decompose_avg(
                        &mut final_eager_split,
                        *index,
                        func_name,
                        self.metadata,
                        function_factory,
                    )?;
                    avg_components.insert(sum_index, count_index);
                    new_eager_aggregations.push((
                        count_vec_index,
                        count_index,
                        "count".to_string(),
                    ));
                    if side == d.other() {
                        avg_to_be_restored
                            .push((sum_index, &original_final_agg.aggregate_functions[*index]));
                        count_to_be_removed.push(count_index);
                    }
                }
                eager_aggregation.extend(new_eager_aggregations);
                Ok(())
            })?;

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
            let mut eval_scalars = vec![&mut eager_split_eval_scalar];
            if side == d {
                eval_scalars.push(&mut eager_groupby_count_scalar);
            }
            for (index, _, func_name) in &self.eager_aggregations[side] {
                if idx == index {
                    continue;
                }
                let (cur_success, old_index, new_index) = update_aggregate_and_eval(
                    &mut final_eager_split,
                    *index,
                    func_name,
                    self.metadata,
                    &mut eval_scalars,
                    &avg_components,
                )?;
                success |= cur_success;

                old_to_new.insert(old_index, new_index);
                func_from.insert(old_index, side);
                func_from.insert(new_index, side);

                if side == d.other() {
                    func_to_be_restored.push((new_index, *index));
                }
            }
            Ok(())
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

        {
            let avg_to_be_restored = avg_to_be_restored
                .into_iter()
                .map(|(index, item)| (old_to_new[&index], item))
                .collect::<HashMap<_, _>>();

            for agg in final_eager_groupby_count.aggregate_functions.iter_mut() {
                if let Some(item) = avg_to_be_restored.get(&agg.index) {
                    *agg = (*item).clone();
                }
            }
        }

        for (index, idx) in func_to_be_restored {
            for agg in final_eager_groupby_count.aggregate_functions.iter_mut() {
                if agg.index == index && idx < original_final_agg.aggregate_functions.len() {
                    *agg = original_final_agg.aggregate_functions[idx].clone();
                    break;
                }
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
                        if aggregate_function.func_name.as_str() != "sum"
                            || (side == Side::Right && func_from[&agg.index] == d)
                        {
                            continue;
                        }
                        count_sum
                            .items
                            .push(create_eager_count_multiply_scalar_item(
                                aggregate_function,
                                eager_count_indexes[func_from[&agg.index].other()].0,
                                &self.extra_eval_scalar,
                                self.metadata,
                            )?);
                    }
                }
                Ok(())
            })?;
        }

        for (sum_index, count_index) in avg_components.iter() {
            if let Some(indexes) = self.eval_scalar_items.get(sum_index) {
                for idx in indexes {
                    let eval_scalar_item = &mut eager_split_eval_scalar.items[*idx];
                    eval_scalar_item.scalar =
                        create_avg_scalar_item(old_to_new[sum_index], old_to_new[count_index]);
                    if func_from[sum_index] == d {
                        let eager_groupby_count_eval_scalar_item =
                            &mut eager_groupby_count_scalar.items[*idx];
                        eager_groupby_count_eval_scalar_item.scalar =
                            create_avg_scalar_item(old_to_new[sum_index], old_to_new[count_index]);
                    }
                    success = true;
                }
            }
        }

        if !success {
            return Ok(false);
        }

        eager_group_by_and_eager_count.for_each_mut(|side, plan| {
            remove_group_by_items_and_aggregate_functions(
                plan,
                &self.columns_sets[side],
                &self.eager_aggregations[side.other()],
            );
        });

        let groupby_count_join = d
            .replace_child(
                self.join_expr,
                d.build_child_with_aggregate(
                    self.join_expr,
                    &self.eager_extra_eval_scalar_expr[d],
                    &eager_group_by_and_eager_count[d],
                ),
            )
            .build_unary(eager_groupby_count_count_sum);
        self.push_result(
            results,
            groupby_count_join,
            final_eager_groupby_count,
            eager_groupby_count_scalar,
        );

        let eager_split_join = self
            .join_expr
            .replace_left_child(Side::Left.build_child_with_aggregate(
                self.join_expr,
                &self.eager_extra_eval_scalar_expr[Side::Left],
                &eager_group_by_and_eager_count[Side::Left],
            ))
            .replace_right_child(Side::Right.build_child_with_aggregate(
                self.join_expr,
                &self.eager_extra_eval_scalar_expr[Side::Right],
                &eager_group_by_and_eager_count[Side::Right],
            ))
            .build_unary(eager_split_count_sum);
        self.push_result(
            results,
            eager_split_join,
            final_eager_split,
            eager_split_eval_scalar,
        );

        Ok(true)
    }

    fn apply_single_side_push(&mut self, d: Side, results: &mut Vec<SExpr>) -> Result<bool> {
        let function_factory = AggregateFunctionFactory::instance();
        let mut final_double_eager = self.final_agg.clone();
        let mut final_eager_count = self.final_agg.clone();

        let mut eager_count_index = 0;
        let mut eager_count_vec_idx = 0;
        if self.can_eager[d.other()] {
            (eager_count_index, eager_count_vec_idx) =
                add_eager_count(&mut final_double_eager, self.metadata);
            self.eager_aggregations[d.other()].push((
                eager_count_vec_idx,
                eager_count_index,
                "count".to_string(),
            ));
        }

        let mut avg_components = HashMap::new();
        let mut new_eager_aggregations = Vec::new();
        for (index, _, func_name) in self.eager_aggregations[d].iter_mut() {
            if func_name.as_str() != "avg" {
                continue;
            }
            let (sum_index, count_index, count_vec_index) = decompose_avg(
                &mut final_double_eager,
                *index,
                func_name,
                self.metadata,
                function_factory,
            )?;
            avg_components.insert(sum_index, count_index);
            new_eager_aggregations.push((count_vec_index, count_index, "count".to_string()));
        }
        self.eager_aggregations[d].extend(new_eager_aggregations);

        let mut eager_group_by = final_double_eager.clone();
        let mut eager_count = final_double_eager.clone();
        let mut double_eager_eval_scalar = self.eval_scalar.clone();
        let mut old_to_new = HashMap::new();
        let mut success = false;

        let mut eval_scalars = vec![&mut double_eager_eval_scalar];
        for (index, _, func_name) in self.eager_aggregations[d].iter() {
            if self.can_eager[d.other()] && eager_count_vec_idx == *index {
                continue;
            }
            let (cur_success, old_index, new_index) = update_aggregate_and_eval(
                &mut final_double_eager,
                *index,
                func_name,
                self.metadata,
                &mut eval_scalars,
                &avg_components,
            )?;
            success |= cur_success;
            old_to_new.insert(old_index, new_index);
        }

        if self.can_eager[d.other()] {
            final_double_eager
                .aggregate_functions
                .remove(eager_count_vec_idx);
        }

        let final_eager_group_by = final_double_eager.clone();

        let mut need_eager_count = false;
        let mut double_eager_count_sum = EvalScalar { items: vec![] };
        let mut eager_count_sum = EvalScalar { items: vec![] };
        if self.can_eager[d.other()] {
            let mut plans_and_counts = Pair {
                left: (&mut final_double_eager, &mut double_eager_count_sum),
                right: (&mut final_eager_count, &mut eager_count_sum),
            };
            plans_and_counts.try_for_each_mut(|_, (plan, sum)| {
                for agg in plan.aggregate_functions.iter_mut() {
                    if let ScalarExpr::AggregateFunction(aggregate_function) = &mut agg.scalar {
                        if aggregate_function.func_name.as_str() != "sum" {
                            continue;
                        }
                        sum.items.push(create_eager_count_multiply_scalar_item(
                            aggregate_function,
                            eager_count_index,
                            &self.extra_eval_scalar,
                            self.metadata,
                        )?);
                        need_eager_count = true;
                    }
                }
                Ok(())
            })?;
        }

        for (sum_index, count_index) in avg_components.iter() {
            if let Some(indexes) = self.eval_scalar_items.get(sum_index) {
                for idx in indexes {
                    let eval_scalar_item = &mut double_eager_eval_scalar.items[*idx];
                    eval_scalar_item.scalar =
                        create_avg_scalar_item(old_to_new[sum_index], old_to_new[count_index]);
                    success = true;
                }
            }
        }

        if !success {
            return Ok(false);
        }

        let eager_group_by_eval_scalar = double_eager_eval_scalar.clone();

        remove_group_by_items_and_aggregate_functions(
            &mut eager_group_by,
            &self.columns_sets[d],
            &self.eager_aggregations[d.other()],
        );
        if self.can_eager[d.other()] && need_eager_count {
            remove_group_by_items_and_aggregate_functions(
                &mut eager_count,
                &self.columns_sets[d.other()],
                &self.eager_aggregations[d],
            );
        }

        let eager_join = d.replace_child(
            self.join_expr,
            d.build_child_with_aggregate(
                self.join_expr,
                &self.eager_extra_eval_scalar_expr[d],
                &eager_group_by,
            ),
        );
        self.push_result(
            results,
            eager_join,
            final_eager_group_by,
            eager_group_by_eval_scalar,
        );

        if self.can_eager[d.other()] && need_eager_count {
            let eager_count_child = d
                .other()
                .child_arc(self.join_expr)
                .ref_build_unary(Aggregate {
                    mode: AggregateMode::Partial,
                    ..eager_count.clone()
                })
                .build_unary(eager_count.clone());
            let eager_count_join = d
                .other()
                .replace_child(self.join_expr, eager_count_child)
                .build_unary(eager_count_sum);
            self.push_result(
                results,
                eager_count_join,
                final_eager_count,
                self.eval_scalar.clone(),
            );

            let eager_group_child = {
                if !self.eager_extra_eval_scalar_expr[d].items.is_empty() {
                    d.child_arc(self.join_expr)
                        .ref_build_unary(self.eager_extra_eval_scalar_expr[d].clone())
                        .into()
                } else {
                    d.child_arc(self.join_expr)
                }
            }
            .ref_build_unary(Aggregate {
                mode: AggregateMode::Partial,
                ..eager_group_by.clone()
            })
            .build_unary(eager_group_by);
            let eager_count_child = d
                .other()
                .child_arc(self.join_expr)
                .ref_build_unary(Aggregate {
                    mode: AggregateMode::Partial,
                    ..eager_count.clone()
                })
                .build_unary(eager_count);
            let replaced = d.replace_child(self.join_expr, eager_group_child);
            let replaced = d.other().replace_child(&replaced, eager_count_child);
            let double_eager_join = replaced.build_unary(double_eager_count_sum);
            self.push_result(
                results,
                double_eager_join,
                final_double_eager,
                double_eager_eval_scalar,
            );
        }

        Ok(true)
    }

    fn push_result(
        &self,
        results: &mut Vec<SExpr>,
        join_expr: SExpr,
        mut final_aggr: Aggregate,
        eval_scalar: EvalScalar,
    ) {
        final_aggr
            .group_items
            .truncate(self.original_group_items_len);
        let plan = join_expr
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
// Return the (Vec index, func index, func_name) for each eager aggregation function.
fn get_eager_aggregation_functions(
    agg_final: &Aggregate,
    columns_set: &ColumnSet,
    eval_scalar_items: &HashMap<usize, Vec<usize>>,
    function_factory: &AggregateFunctionFactory,
) -> Vec<(usize, IndexType, String)> {
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
            if !function_factory.is_decomposable(&aggregate_function.func_name)
                || !eval_scalar_items.contains_key(&func.index)
                || !columns_set.contains(&column.column.index)
            {
                return None;
            }
            match &*column.column.data_type {
                DataType::Number(_) | DataType::Decimal(_) => (),
                DataType::Nullable(box DataType::Number(_) | box DataType::Decimal(_)) => {}
                _ => return None,
            }
            Some((index, func.index, aggregate_function.func_name.clone()))
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
    eager_aggregations: &[(usize, usize, String)],
) {
    // Remove group by items.
    let origin_group_by_index = agg_final
        .group_items
        .iter()
        .map(|x| x.index)
        .collect::<Vec<_>>();
    for group_by_index in origin_group_by_index {
        if !columns_set.contains(&group_by_index) {
            for (idx, item) in agg_final.group_items.iter().enumerate() {
                if item.index == group_by_index {
                    agg_final.group_items.remove(idx);
                    break;
                }
            }
        }
    }
    // Remove aggregate functions.
    let funcs_not_owned_by_eager: HashSet<usize> = eager_aggregations
        .iter()
        .map(|(_, index, _)| *index)
        .collect();
    for func_index in funcs_not_owned_by_eager.iter() {
        for (idx, item) in agg_final.aggregate_functions.iter().enumerate() {
            if &item.index == func_index {
                agg_final.aggregate_functions.remove(idx);
                break;
            }
        }
    }
}

fn create_avg_scalar_item(left_index: usize, right_index: usize) -> ScalarExpr {
    ScalarExpr::FunctionCall(FunctionCall {
        span: None,
        func_name: "divide".to_string(),
        params: vec![],
        arguments: vec![
            ScalarExpr::BoundColumnRef(BoundColumnRef {
                span: None,
                column: ColumnBindingBuilder::new(
                    "_eager_final_sum".to_string(),
                    left_index,
                    Box::new(DataType::Number(NumberDataType::Float64)),
                    Visibility::Visible,
                )
                .build(),
            }),
            wrap_cast(
                &ScalarExpr::BoundColumnRef(BoundColumnRef {
                    span: None,
                    column: ColumnBindingBuilder::new(
                        "_eager_final_count".to_string(),
                        right_index,
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
    })
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

// AVG(C) => SUM(C) / COUNT(C)
fn decompose_avg(
    final_agg: &mut Aggregate,
    index: usize,
    func_name: &mut String,
    metadata: &MetadataRef,
    function_factory: &AggregateFunctionFactory,
) -> Result<(usize, usize, usize)> {
    *func_name = "sum".to_string();
    // Add COUNT aggregate functions.
    final_agg
        .aggregate_functions
        .push(final_agg.aggregate_functions[index].clone());

    // Get SUM(currently still AVG) aggregate functions.
    let sum_aggregation_function = &mut final_agg.aggregate_functions[index];

    // AVG => SUM
    let sum_index = sum_aggregation_function.index;
    if let ScalarExpr::AggregateFunction(agg) = &mut sum_aggregation_function.scalar {
        if let ScalarExpr::BoundColumnRef(column) = &agg.args[0] {
            agg.func_name = "sum".to_string();
            metadata.write().change_derived_column_alias(
                sum_aggregation_function.index,
                format!(
                    "avg_{}({}.{})",
                    agg.func_name.clone(),
                    &column.column.table_name.clone().unwrap_or(String::new()),
                    &column.column.column_name.clone(),
                ),
            );
            let func = function_factory.get(
                &agg.func_name,
                agg.params.clone(),
                vec![*column.column.data_type.clone()],
                vec![],
            )?;
            agg.return_type = Box::new(func.return_type()?);
        }
    }

    // Get COUNT(currently still AVG) aggregate functions.
    let count_vec_index = final_agg.aggregate_functions.len() - 1;
    let count_aggregation_function = &mut final_agg.aggregate_functions[count_vec_index];

    // Generate a new column for COUNT.
    let mut table_name = String::new();
    let mut column_name = String::new();
    if let ScalarExpr::AggregateFunction(agg) = &count_aggregation_function.scalar {
        if let ScalarExpr::BoundColumnRef(column) = &agg.args[0] {
            table_name = column.column.table_name.clone().unwrap_or(String::new());
            column_name = column.column.column_name.clone();
        }
    }
    let count_index = metadata.write().add_derived_column(
        format!("avg_count_{}({}.{})", &func_name, table_name, column_name),
        count_aggregation_function.scalar.data_type()?,
    );

    // AVG => COUNT
    if let ScalarExpr::AggregateFunction(agg) = &mut count_aggregation_function.scalar {
        agg.func_name = "count".to_string();
        agg.return_type = Box::new(DataType::Number(NumberDataType::UInt64));
    }
    count_aggregation_function.index = count_index;

    Ok((sum_index, count_index, count_vec_index))
}

fn update_aggregate_and_eval(
    final_agg: &mut Aggregate,
    index: usize,
    func_name: &String,
    metadata: &MetadataRef,
    eval_scalars: &mut Vec<&mut EvalScalar>,
    avg_components: &HashMap<usize, usize>,
) -> Result<(bool, usize, usize)> {
    let final_aggregate_function = &mut final_agg.aggregate_functions[index];

    let old_index = final_aggregate_function.index;
    let new_index = metadata.write().add_derived_column(
        format!("_eager_final_{}", &func_name),
        final_aggregate_function.scalar.data_type()?,
    );

    // Modify final aggregate functions.
    if let ScalarExpr::AggregateFunction(agg) = &mut final_aggregate_function.scalar {
        // final_aggregate_functions is currently a clone of eager aggregation.
        modify_final_aggregate_function(agg, old_index);
        // final_aggregate_functions is a final aggregation now.
        final_aggregate_function.index = new_index;
    }

    let mut success = false;

    // Modify the eval scalars of all aggregate functions that are not AVG components.
    if !avg_components.contains_key(&old_index) {
        for eval_scalar in eval_scalars {
            for scalar_item in eval_scalar.items.iter_mut() {
                if let ScalarExpr::BoundColumnRef(column) = &mut scalar_item.scalar {
                    if column.column.index != old_index {
                        continue;
                    }
                    // If it's already a column, we can just update the column_binding index
                    let column_binding = &mut column.column;
                    column_binding.index = new_index;

                    if func_name == "count" {
                        column_binding.data_type = Box::new(DataType::Nullable(Box::new(
                            DataType::Number(NumberDataType::UInt64),
                        )));
                        scalar_item.scalar = wrap_cast(
                            &scalar_item.scalar,
                            &DataType::Number(NumberDataType::UInt64),
                        );
                    }
                    success = true;
                } else {
                    // Otherwise, we need to replace the column index recursively.
                    scalar_item.scalar.replace_column(old_index, new_index)?;
                }
            }
        }
    }
    Ok((success, old_index, new_index))
}

fn create_eager_count_multiply_scalar_item(
    aggregate_function: &mut AggregateFunction,
    eager_count_index: IndexType,
    extra_eval_scalar: &EvalScalar,
    metadata: &MetadataRef,
) -> Result<ScalarItem> {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Side {
    Left,
    Right,
}

impl Side {
    fn other(self) -> Self {
        match self {
            Side::Left => Side::Right,
            Side::Right => Side::Left,
        }
    }

    fn child_arc(self, join_expr: &SExpr) -> Arc<SExpr> {
        match self {
            Side::Left => join_expr.left_child_arc(),
            Side::Right => join_expr.right_child_arc(),
        }
    }

    fn replace_child(self, join_expr: &SExpr, child: SExpr) -> SExpr {
        match self {
            Side::Left => join_expr.replace_left_child(child),
            Side::Right => join_expr.replace_right_child(child),
        }
    }

    fn build_child_with_aggregate(
        self,
        join_expr: &SExpr,
        extra_eval_scalar: &EvalScalar,
        aggregate: &Aggregate,
    ) -> SExpr {
        if !extra_eval_scalar.items.is_empty() {
            self.child_arc(join_expr)
                .ref_build_unary(extra_eval_scalar.clone())
                .into()
        } else {
            self.child_arc(join_expr)
        }
        .ref_build_unary(Aggregate {
            mode: AggregateMode::Partial,
            ..aggregate.clone()
        })
        .build_unary(aggregate.clone())
    }
}

#[derive(Clone, Debug)]
struct Pair<T> {
    left: T,
    right: T,
}

impl<T> Pair<T> {
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

    fn try_for_each<F>(&self, mut f: F) -> Result<()>
    where F: FnMut(Side, &T) -> Result<()> {
        f(Side::Left, &self.left)?;
        f(Side::Right, &self.right)?;
        Ok(())
    }

    fn try_for_each_mut<F>(&mut self, mut f: F) -> Result<()>
    where F: FnMut(Side, &mut T) -> Result<()> {
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
