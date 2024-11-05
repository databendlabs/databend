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

use databend_common_expression::types::number::NumberDataType;
use databend_common_expression::types::DataType;
use databend_common_functions::aggregates::AggregateFunctionFactory;

use crate::binder::wrap_cast;
use crate::binder::ColumnBindingBuilder;
use crate::optimizer::extract::Matcher;
use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::ColumnSet;
use crate::optimizer::RelExpr;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::Aggregate;
use crate::plans::AggregateFunction;
use crate::plans::AggregateMode;
use crate::plans::BoundColumnRef;
use crate::plans::EvalScalar;
use crate::plans::FunctionCall;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::plans::ScalarItem;
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
    id: RuleID,
    matchers: Vec<Matcher>,
    metadata: MetadataRef,
}

impl RuleEagerAggregation {
    pub fn new(metadata: MetadataRef) -> Self {
        Self {
            id: RuleID::EagerAggregation,

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
        self.id
    }

    fn apply(
        &self,
        s_expr: &SExpr,
        state: &mut TransformResult,
    ) -> databend_common_exception::Result<()> {
        let mut matched_idx = 0;
        for (idx, matcher) in self.matchers.iter().enumerate() {
            if matcher.matches(s_expr) {
                matched_idx = idx + 1;
                break;
            }
        }
        if matched_idx == 0 {
            return Ok(());
        }

        let (has_sort, has_extra_eval) = match matched_idx {
            1 => (false, false),
            2 => (false, true),
            3 => (true, false),
            4 => (true, true),
            _ => unreachable!(),
        };

        let eval_scalar_expr = s_expr;
        let sort_expr = eval_scalar_expr.child(0)?;
        let final_agg_expr = match has_sort {
            true => sort_expr.child(0)?,
            false => sort_expr,
        };
        let final_agg_partial_expr = final_agg_expr.child(0)?;
        let extra_eval_scalar_expr = final_agg_partial_expr.child(0)?;
        let join_expr = match has_extra_eval {
            true => extra_eval_scalar_expr.child(0)?,
            false => extra_eval_scalar_expr,
        };

        let join: Join = join_expr.plan().clone().try_into()?;
        // Only supports inner/cross join and equal conditions.
        if !matches!(join.join_type, JoinType::Inner | JoinType::Cross)
            | !join.non_equi_conditions.is_empty()
        {
            return Ok(());
        }

        let eval_scalar: EvalScalar = eval_scalar_expr.plan().clone().try_into()?;
        let mut final_agg: Aggregate = final_agg_expr.plan().clone().try_into()?;

        // Get the original column set from the left child and right child of join.
        let mut columns_sets = Vec::with_capacity(2);
        for idx in 0..2 {
            let rel_expr = RelExpr::with_s_expr(&join_expr.children[idx]);
            let prop = rel_expr.derive_relational_prop()?;
            columns_sets.push(prop.output_columns.clone());
        }

        let extra_eval_scalar = if has_extra_eval {
            extra_eval_scalar_expr.plan().clone().try_into()?
        } else {
            EvalScalar { items: vec![] }
        };
        // Check if all extra eval scalars can be solved by one of the children.
        let mut eager_extra_eval_scalar_expr =
            [EvalScalar { items: vec![] }, EvalScalar { items: vec![] }];
        if has_extra_eval {
            for eval_item in extra_eval_scalar.items.iter() {
                let eval_used_columns = eval_item.scalar.used_columns();
                let mut resolved_by_one_child = false;
                for idx in 0..2 {
                    if eval_used_columns.is_subset(&columns_sets[idx]) {
                        eager_extra_eval_scalar_expr[idx]
                            .items
                            .push(eval_item.clone());
                        columns_sets[idx].insert(eval_item.index);
                        resolved_by_one_child = true;
                    }
                }
                if !resolved_by_one_child {
                    return Ok(());
                }
            }
        }

        // Find all `BoundColumnRef`, and then create a mapping from the column index
        // of `BoundColumnRef` to the item's vec index.
        let mut eval_scalar_items: HashMap<usize, Vec<usize>> = HashMap::new();
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
        let mut eager_aggregations = Vec::new();
        for (idx, columns_set) in columns_sets.iter().enumerate() {
            eager_aggregations.push(get_eager_aggregation_functions(
                idx,
                &final_agg,
                columns_set,
                &eval_scalar_items,
                function_factory,
            ))
        }

        // There are some aggregate functions that cannot be pushed down, if a func(x)
        // cannot be pushed down, it means that we need to add x to the group by items
        // of eager aggregation, it will change the semantics of the sql, So we should
        // stop eager aggregation.
        if eager_aggregations[0].len() + eager_aggregations[1].len()
            != final_agg.aggregate_functions.len()
        {
            return Ok(());
        }

        // Divide group by columns into two parts, where group_columns_set[0] comes from
        // the left child and group_columns_set[1] comes from the right child.
        let mut group_columns_set = [ColumnSet::new(), ColumnSet::new()];
        for item in final_agg.group_items.iter() {
            if let ScalarExpr::BoundColumnRef(_) = &item.scalar {
                for idx in 0..2 {
                    if columns_sets[idx].contains(&item.index) {
                        group_columns_set[idx].insert(item.index);
                    }
                }
            } else {
                return Ok(());
            }
        }

        // Using join conditions to propagate group item to another child.
        let left_conditions = join
            .equi_conditions
            .iter()
            .map(|condition| condition.left.clone())
            .collect::<Vec<_>>();
        let right_conditions = join
            .equi_conditions
            .iter()
            .map(|condition| condition.right.clone())
            .collect::<Vec<_>>();
        let join_conditions = [&left_conditions, &right_conditions];
        let original_group_items_len = final_agg.group_items.len();
        for (idx, conditions) in join_conditions.iter().enumerate() {
            for (cond_idx, cond) in conditions.iter().enumerate() {
                match &cond {
                    ScalarExpr::BoundColumnRef(join_column) => {
                        if group_columns_set[idx].contains(&join_column.column.index) {
                            let another_cond = &join_conditions[idx ^ 1][cond_idx];
                            if let ScalarExpr::BoundColumnRef(another_join_column) = another_cond {
                                if !group_columns_set[idx ^ 1]
                                    .contains(&another_join_column.column.index)
                                {
                                    let new_group_item = ScalarItem {
                                        scalar: another_cond.clone(),
                                        index: another_join_column.column.index,
                                    };
                                    final_agg.group_items.push(new_group_item);
                                    group_columns_set[idx ^ 1]
                                        .insert(another_join_column.column.index);
                                }
                            }
                        }
                    }
                    _ => return Ok(()),
                }
            }
        }

        // If a child's `can_eager` is true, its group_columns_set should include all
        // join conditions related to the child.
        let mut can_eager = [true; 2];
        for (idx, conditions) in join_conditions.iter().enumerate() {
            for cond in conditions.iter() {
                if !cond
                    .used_columns()
                    .iter()
                    .all(|c| group_columns_set[idx].contains(c))
                {
                    can_eager[idx] = false;
                    break;
                }
            }
        }

        let can_push_down = [
            !eager_aggregations[0].is_empty() && can_eager[0],
            !eager_aggregations[1].is_empty() && can_eager[1],
        ];

        let mut success = false;
        let d = if can_push_down[0] { 0 } else { 1 };

        // These variables are used to generate the final result.
        let mut final_agg_finals = Vec::new();
        let mut final_agg_partials = Vec::new();
        let mut final_eval_scalars = Vec::new();
        let mut join_exprs = Vec::new();

        if can_push_down[d] && can_push_down[d ^ 1] {
            // (1) apply eager split on d and d^1.
            // (2) apply eager groupby-count on d.
            // we use `final_eager_split` and `final_eager_groupby_count` to represent
            // the (1) and (2) respectively, the structure of operators is as follows:
            // final_eager_split / final_eager_groupby_count
            // |
            // join
            // | \
            // |  eager_group_by_and_eager_count
            // eager_group_by_and_eager_count

            let original_final_agg = final_agg.clone();
            let mut final_eager_split = final_agg;

            let mut count_to_be_removed = Vec::new();
            let mut avg_to_be_restored = Vec::new();
            let mut func_to_be_restored = Vec::new();

            // Add eager COUNT aggregate functions.
            let mut eager_count_indexes = [0, 0];
            let mut eager_count_vec_idx = [0, 0];
            for idx in 0..2 {
                (eager_count_indexes[idx], eager_count_vec_idx[idx]) =
                    add_eager_count(&mut final_eager_split, self.metadata.clone());
                eager_aggregations[idx].push((
                    eager_count_vec_idx[idx],
                    eager_count_indexes[idx],
                    "count".to_string(),
                ));
            }

            // Find all AVG aggregate functions, convert AVG to SUM and then add a COUNT aggregate function.
            let mut avg_components = HashMap::new();
            for (idx, eager_aggregation) in eager_aggregations.iter_mut().enumerate() {
                let mut new_eager_aggregations = Vec::new();
                for (index, _, func_name) in eager_aggregation.iter_mut() {
                    if func_name.as_str() != "avg" {
                        continue;
                    }
                    // AVG(C) => SUM(C) / COUNT(C)
                    let (sum_index, count_index, count_vec_index) = decompose_avg(
                        &mut final_eager_split,
                        *index,
                        func_name,
                        self.metadata.clone(),
                        function_factory,
                    )?;
                    // save AVG components to HashMap: (key=SUM, value=COUNT).
                    avg_components.insert(sum_index, count_index);
                    // Add COUNT to new_eager_aggregations.
                    new_eager_aggregations.push((
                        count_vec_index,
                        count_index,
                        "count".to_string(),
                    ));
                    if idx == d ^ 1 {
                        avg_to_be_restored.push((sum_index, *index));
                        count_to_be_removed.push(count_index);
                    }
                }
                eager_aggregation.extend(new_eager_aggregations);
            }

            let mut eager_group_by_and_eager_count =
                [final_eager_split.clone(), final_eager_split.clone()];

            let mut eager_groupby_count_scalar = eval_scalar.clone();
            let mut eager_split_eval_scalar = eval_scalar;

            // Map AVG components's old-index to new-index
            let mut old_to_new = HashMap::new();

            // func_from indicates whether the aggregate function comes from the left child(0) or the right child(1).
            let mut func_from = HashMap::new();

            // The eager_aggregations here only only contains MIN, MAX, SUM and COUNT,
            // and the AVG has been transformed into SUM and COUNT.
            for idx in 0..2 {
                let mut eval_scalars = vec![&mut eager_split_eval_scalar];
                let eager_aggregation = &eager_aggregations[idx];
                if idx == d {
                    eval_scalars.push(&mut eager_groupby_count_scalar);
                }
                for (index, _, func_name) in eager_aggregation.iter() {
                    if eager_count_vec_idx[idx] == *index {
                        continue;
                    }
                    let (cur_success, old_index, new_index) = update_aggregate_and_eval(
                        &mut final_eager_split,
                        *index,
                        func_name,
                        self.metadata.clone(),
                        &mut eval_scalars,
                        &eval_scalar_items,
                        &avg_components,
                    )?;
                    success |= cur_success;

                    old_to_new.insert(old_index, new_index);

                    func_from.insert(old_index, idx);
                    func_from.insert(new_index, idx);

                    if idx == d ^ 1 {
                        func_to_be_restored.push((new_index, index));
                    }
                }
            }

            // Remove eager-count function from `final_double_eager` because
            // eager-count function is now in `eager_count`.
            for eager_count_index in eager_count_indexes {
                for (idx, agg) in final_eager_split.aggregate_functions.iter().enumerate() {
                    if agg.index == eager_count_index {
                        final_eager_split.aggregate_functions.remove(idx);
                        break;
                    }
                }
            }

            // Restore some functions in `final_eager_split` to its original state to
            // generate `final_eager_groupby_count`, since AVG has already been converted
            // to SUM and COUNT, we will delete COUNT and restore SUM to AVG, finally,
            // restore the functions in `func_to_be_restored` to their original state.
            let mut final_eager_groupby_count = final_eager_split.clone();
            for index in count_to_be_removed {
                for (idx, agg) in final_eager_groupby_count
                    .aggregate_functions
                    .iter()
                    .enumerate()
                {
                    if agg.index == old_to_new[&index] {
                        final_eager_groupby_count.aggregate_functions.remove(idx);
                        break;
                    }
                }
            }
            for (index, idx) in avg_to_be_restored {
                for agg in final_eager_groupby_count.aggregate_functions.iter_mut() {
                    if agg.index == old_to_new[&index] {
                        *agg = original_final_agg.aggregate_functions[idx].clone();
                        break;
                    }
                }
            }
            for (index, idx) in func_to_be_restored {
                for agg in final_eager_groupby_count.aggregate_functions.iter_mut() {
                    if agg.index == index && *idx < original_final_agg.aggregate_functions.len() {
                        *agg = original_final_agg.aggregate_functions[*idx].clone();
                        break;
                    }
                }
            }

            // Try to multiply eager count for each sum aggregate column.
            let mut eager_split_count_sum = EvalScalar { items: vec![] };
            let mut eager_groupby_count_count_sum = EvalScalar { items: vec![] };
            let final_eager = [&mut final_eager_split, &mut final_eager_groupby_count];
            let count_sum = [
                &mut eager_split_count_sum,
                &mut eager_groupby_count_count_sum,
            ];
            for idx in 0..2 {
                for agg in final_eager[idx].aggregate_functions.iter_mut() {
                    if let ScalarExpr::AggregateFunction(aggregate_function) = &mut agg.scalar {
                        if aggregate_function.func_name.as_str() != "sum"
                            || (idx == 1 && func_from[&agg.index] == d)
                        {
                            continue;
                        }
                        count_sum[idx]
                            .items
                            .push(create_eager_count_multiply_scalar_item(
                                aggregate_function,
                                eager_count_indexes[func_from[&agg.index] ^ 1],
                                &extra_eval_scalar,
                                self.metadata.clone(),
                            )?);
                    }
                }
            }

            // AVG(C) = SUM(C) / COUNT(C)
            // Use AVG components(SUM and COUNT) to build AVG eval scalar.
            for (sum_index, count_index) in avg_components.iter() {
                if let Some(indexes) = eval_scalar_items.get(sum_index) {
                    for idx in indexes {
                        let eval_scalar_item = &mut eager_split_eval_scalar.items[*idx];
                        eval_scalar_item.scalar =
                            create_avg_scalar_item(old_to_new[sum_index], old_to_new[count_index]);
                        if func_from[sum_index] == d {
                            let eager_groupby_count_eval_scalar_item =
                                &mut eager_groupby_count_scalar.items[*idx];
                            eager_groupby_count_eval_scalar_item.scalar = create_avg_scalar_item(
                                old_to_new[sum_index],
                                old_to_new[count_index],
                            );
                        }
                        success = true;
                    }
                }
            }

            if !success {
                return Ok(());
            }

            // Remove group by items and aggregate functions that do not belong to idx.
            for idx in 0..2 {
                remove_group_by_items_and_aggregate_functions(
                    &mut eager_group_by_and_eager_count[idx],
                    &columns_sets[idx],
                    &eager_aggregations[idx ^ 1],
                );
            }

            // Apply eager groupby-count on d.
            let mut final_eager_groupby_count_partial = final_eager_groupby_count.clone();
            final_eager_groupby_count_partial.mode = AggregateMode::Partial;

            final_agg_finals.push(final_eager_groupby_count);
            final_agg_partials.push(final_eager_groupby_count_partial);
            final_eval_scalars.push(eager_groupby_count_scalar);

            let mut eager_group_by_count_partial = eager_group_by_and_eager_count[d].clone();
            eager_group_by_count_partial.mode = AggregateMode::Partial;

            join_exprs.push(if d == 0 {
                eval_scalar_expr
                    .replace_children(vec![Arc::new(join_expr.replace_children(vec![
                        Arc::new(SExpr::create_unary(
                            Arc::new(RelOperator::Aggregate(
                                eager_group_by_and_eager_count[d].clone(),
                            )),
                            Arc::new(SExpr::create_unary(
                                Arc::new(RelOperator::Aggregate(eager_group_by_count_partial)),
                                if !eager_extra_eval_scalar_expr[0].items.is_empty() {
                                    Arc::new(SExpr::create_unary(
                                        Arc::new(RelOperator::EvalScalar(
                                            eager_extra_eval_scalar_expr[0].clone(),
                                        )),
                                        Arc::new(join_expr.child(0)?.clone()),
                                    ))
                                } else {
                                    Arc::new(join_expr.child(0)?.clone())
                                },
                            )),
                        )),
                        Arc::new(join_expr.child(1)?.clone()),
                    ]))])
                    .replace_plan(Arc::new(eager_groupby_count_count_sum.into()))
            } else {
                eval_scalar_expr
                    .replace_children(vec![Arc::new(join_expr.replace_children(vec![
                        Arc::new(join_expr.child(0)?.clone()),
                        Arc::new(SExpr::create_unary(
                            Arc::new(RelOperator::Aggregate(
                                eager_group_by_and_eager_count[d].clone(),
                            )),
                            Arc::new(SExpr::create_unary(
                                Arc::new(RelOperator::Aggregate(eager_group_by_count_partial)),
                                if !eager_extra_eval_scalar_expr[1].items.is_empty() {
                                    Arc::new(SExpr::create_unary(
                                        Arc::new(RelOperator::EvalScalar(
                                            eager_extra_eval_scalar_expr[1].clone(),
                                        )),
                                        Arc::new(join_expr.child(1)?.clone()),
                                    ))
                                } else {
                                    Arc::new(join_expr.child(1)?.clone())
                                },
                            )),
                        )),
                    ]))])
                    .replace_plan(Arc::new(eager_groupby_count_count_sum.into()))
            });

            // Apply eager split on d and d^1.
            let mut final_eager_split_partial = final_eager_split.clone();
            final_eager_split_partial.mode = AggregateMode::Partial;

            final_agg_finals.push(final_eager_split);
            final_agg_partials.push(final_eager_split_partial);
            final_eval_scalars.push(eager_split_eval_scalar.clone());

            let mut eager_group_by_and_eager_count_partial = [
                eager_group_by_and_eager_count[0].clone(),
                eager_group_by_and_eager_count[1].clone(),
            ];
            eager_group_by_and_eager_count_partial[0].mode = AggregateMode::Partial;
            eager_group_by_and_eager_count_partial[1].mode = AggregateMode::Partial;

            join_exprs.push(
                eval_scalar_expr
                    .replace_children(vec![Arc::new(join_expr.replace_children(vec![
                        Arc::new(SExpr::create_unary(
                            Arc::new(RelOperator::Aggregate(
                                eager_group_by_and_eager_count[0].clone(),
                            )),
                            Arc::new(SExpr::create_unary(
                                Arc::new(RelOperator::Aggregate(
                                    eager_group_by_and_eager_count_partial[0].clone(),
                                )),
                                if !eager_extra_eval_scalar_expr[0].items.is_empty() {
                                    Arc::new(SExpr::create_unary(
                                        Arc::new(RelOperator::EvalScalar(
                                            eager_extra_eval_scalar_expr[0].clone(),
                                        )),
                                        Arc::new(join_expr.child(0)?.clone()),
                                    ))
                                } else {
                                    Arc::new(join_expr.child(0)?.clone())
                                },
                            )),
                        )),
                        Arc::new(SExpr::create_unary(
                            Arc::new(RelOperator::Aggregate(
                                eager_group_by_and_eager_count[1].clone(),
                            )),
                            Arc::new(SExpr::create_unary(
                                Arc::new(RelOperator::Aggregate(
                                    eager_group_by_and_eager_count_partial[1].clone(),
                                )),
                                if !eager_extra_eval_scalar_expr[1].items.is_empty() {
                                    Arc::new(SExpr::create_unary(
                                        Arc::new(RelOperator::EvalScalar(
                                            eager_extra_eval_scalar_expr[1].clone(),
                                        )),
                                        Arc::new(join_expr.child(1)?.clone()),
                                    ))
                                } else {
                                    Arc::new(join_expr.child(1)?.clone())
                                },
                            )),
                        )),
                    ]))])
                    .replace_plan(Arc::new(eager_split_count_sum.into())),
            );
        } else if can_push_down[d] && eager_aggregations[d ^ 1].is_empty() {
            // (1) Try to apply eager group-by on d.
            // (2) Try to apply eager count on d^1's sum aggregations.
            // (3) If (1) and (2) success, apply double eager on d and d^1.
            // we use `final_eager_group_by`, `final_eager_count` and `final_double_eager`
            // to represent the (1), (2) and (3) respectively, the structure of operators
            // is as follows:
            // final_eager_group_by / final_eager_count / final_double_eager
            // |
            // join
            // | \
            // |  eager_count
            // eager_group_by

            let mut final_double_eager = final_agg;
            let mut final_eager_count = final_double_eager.clone();

            // Add eager COUNT aggregate functions.
            let mut eager_count_index = 0;
            let mut eager_count_vec_idx = 0;
            if can_eager[d ^ 1] {
                (eager_count_index, eager_count_vec_idx) =
                    add_eager_count(&mut final_double_eager, self.metadata.clone());
                eager_aggregations[d ^ 1].push((
                    eager_count_vec_idx,
                    eager_count_index,
                    "count".to_string(),
                ));
            }

            // Find all AVG aggregate functions, convert AVG to SUM and then add a COUNT aggregate function.
            let mut avg_components = HashMap::new();
            let mut new_eager_aggregations = Vec::new();
            for (index, _, func_name) in eager_aggregations[d].iter_mut() {
                if func_name.as_str() != "avg" {
                    continue;
                }
                // AVG(C) => SUM(C) / COUNT(C)
                let (sum_index, count_index, count_vec_index) = decompose_avg(
                    &mut final_double_eager,
                    *index,
                    func_name,
                    self.metadata.clone(),
                    function_factory,
                )?;
                // save AVG components to HashMap: (key=SUM, value=COUNT).
                avg_components.insert(sum_index, count_index);
                // Add COUNT to new_eager_aggregations.
                new_eager_aggregations.push((count_vec_index, count_index, "count".to_string()));
            }
            eager_aggregations[d].extend(new_eager_aggregations);

            let mut eager_group_by = final_double_eager.clone();
            let mut eager_count = final_double_eager.clone();

            // eager_count_eval_scalar saves the original items
            let eager_count_eval_scalar = eval_scalar.clone();
            let mut double_eager_eval_scalar = eval_scalar;

            // Map AVG components's old-index to new-index
            let mut old_to_new = HashMap::new();

            // The eager_aggregations here only only contains MIN, MAX, SUM and COUNT,
            // and the AVG has been transformed into SUM and COUNT.
            let mut eval_scalars = vec![&mut double_eager_eval_scalar];
            for (index, _, func_name) in eager_aggregations[d].iter() {
                if can_eager[d ^ 1] && eager_count_vec_idx == *index {
                    continue;
                }
                let (cur_success, old_index, new_index) = update_aggregate_and_eval(
                    &mut final_double_eager,
                    *index,
                    func_name,
                    self.metadata.clone(),
                    &mut eval_scalars,
                    &eval_scalar_items,
                    &avg_components,
                )?;
                success |= cur_success;

                old_to_new.insert(old_index, new_index);
            }

            // Remove eager-count function from `final_double_eager` because
            // eager-count function is now in `eager_count`.
            if can_eager[d ^ 1] {
                final_double_eager
                    .aggregate_functions
                    .remove(eager_count_vec_idx);
            }

            let final_eager_group_by = final_double_eager.clone();

            let mut need_eager_count = false;
            // Try to multiply eager count for each sum aggregate column.
            let mut double_eager_count_sum = EvalScalar { items: vec![] };
            let mut eager_count_sum = EvalScalar { items: vec![] };
            if can_eager[d ^ 1] {
                let final_eager = [&mut final_double_eager, &mut final_eager_count];
                let count_sum = [&mut double_eager_count_sum, &mut eager_count_sum];
                for idx in 0..2 {
                    for agg in final_eager[idx].aggregate_functions.iter_mut() {
                        if let ScalarExpr::AggregateFunction(aggregate_function) = &mut agg.scalar {
                            if aggregate_function.func_name.as_str() != "sum" {
                                continue;
                            }
                            count_sum[idx]
                                .items
                                .push(create_eager_count_multiply_scalar_item(
                                    aggregate_function,
                                    eager_count_index,
                                    &extra_eval_scalar,
                                    self.metadata.clone(),
                                )?);
                            need_eager_count = true;
                        }
                    }
                }
            }

            // AVG(C) = SUM(C) / COUNT(C)
            // Use AVG components(SUM and COUNT) to build AVG eval scalar.
            for (sum_index, count_index) in avg_components.iter() {
                if let Some(indexes) = eval_scalar_items.get(sum_index) {
                    for idx in indexes {
                        let eval_scalar_item = &mut double_eager_eval_scalar.items[*idx];
                        eval_scalar_item.scalar =
                            create_avg_scalar_item(old_to_new[sum_index], old_to_new[count_index]);
                        success = true;
                    }
                }
            }

            if !success {
                return Ok(());
            }

            let eager_group_by_eval_scalar = double_eager_eval_scalar.clone();

            // Remove group by items and aggregate functions that do not belong to d.
            remove_group_by_items_and_aggregate_functions(
                &mut eager_group_by,
                &columns_sets[d],
                &eager_aggregations[d ^ 1],
            );
            if can_eager[d ^ 1] && need_eager_count {
                // Remove group by items and aggregate functions that do not belong to d ^ 1.
                remove_group_by_items_and_aggregate_functions(
                    &mut eager_count,
                    &columns_sets[d ^ 1],
                    &eager_aggregations[d],
                );
            }

            // Apply eager group-by on d.
            let mut final_eager_group_by_partial = final_eager_group_by.clone();
            final_eager_group_by_partial.mode = AggregateMode::Partial;

            final_agg_finals.push(final_eager_group_by);
            final_agg_partials.push(final_eager_group_by_partial);
            final_eval_scalars.push(eager_group_by_eval_scalar);

            let mut eager_group_by_partial = eager_group_by.clone();
            eager_group_by_partial.mode = AggregateMode::Partial;

            join_exprs.push(if d == 0 {
                join_expr.replace_children(vec![
                    Arc::new(SExpr::create_unary(
                        Arc::new(RelOperator::Aggregate(eager_group_by.clone())),
                        Arc::new(SExpr::create_unary(
                            Arc::new(RelOperator::Aggregate(eager_group_by_partial)),
                            if !eager_extra_eval_scalar_expr[0].items.is_empty() {
                                Arc::new(SExpr::create_unary(
                                    Arc::new(RelOperator::EvalScalar(
                                        eager_extra_eval_scalar_expr[0].clone(),
                                    )),
                                    Arc::new(join_expr.child(0)?.clone()),
                                ))
                            } else {
                                Arc::new(join_expr.child(0)?.clone())
                            },
                        )),
                    )),
                    Arc::new(join_expr.child(1)?.clone()),
                ])
            } else {
                join_expr.replace_children(vec![
                    Arc::new(join_expr.child(0)?.clone()),
                    Arc::new(SExpr::create_unary(
                        Arc::new(RelOperator::Aggregate(eager_group_by.clone())),
                        Arc::new(SExpr::create_unary(
                            Arc::new(RelOperator::Aggregate(eager_group_by_partial)),
                            if !eager_extra_eval_scalar_expr[1].items.is_empty() {
                                Arc::new(SExpr::create_unary(
                                    Arc::new(RelOperator::EvalScalar(
                                        eager_extra_eval_scalar_expr[1].clone(),
                                    )),
                                    Arc::new(join_expr.child(1)?.clone()),
                                ))
                            } else {
                                Arc::new(join_expr.child(1)?.clone())
                            },
                        )),
                    )),
                ])
            });

            if can_eager[d ^ 1] && need_eager_count {
                // Apply eager count on d^1.
                let mut final_eager_count_partial = final_eager_count.clone();
                final_eager_count_partial.mode = AggregateMode::Partial;

                final_agg_finals.push(final_eager_count);
                final_agg_partials.push(final_eager_count_partial);
                final_eval_scalars.push(eager_count_eval_scalar);

                let mut eager_count_partial = eager_count.clone();
                eager_count_partial.mode = AggregateMode::Partial;
                join_exprs.push(if d == 0 {
                    eval_scalar_expr
                        .replace_children(vec![Arc::new(join_expr.replace_children(vec![
                            Arc::new(join_expr.child(0)?.clone()),
                            Arc::new(SExpr::create_unary(
                                Arc::new(RelOperator::Aggregate(eager_count.clone())),
                                Arc::new(SExpr::create_unary(
                                    Arc::new(RelOperator::Aggregate(eager_count_partial)),
                                    Arc::new(join_expr.child(1)?.clone()),
                                )),
                            )),
                        ]))])
                        .replace_plan(Arc::new(eager_count_sum.into()))
                } else {
                    eval_scalar_expr
                        .replace_children(vec![Arc::new(join_expr.replace_children(vec![
                            Arc::new(SExpr::create_unary(
                                Arc::new(RelOperator::Aggregate(eager_count.clone())),
                                Arc::new(SExpr::create_unary(
                                    Arc::new(RelOperator::Aggregate(eager_count_partial)),
                                    Arc::new(join_expr.child(0)?.clone()),
                                )),
                            )),
                            Arc::new(join_expr.child(1)?.clone()),
                        ]))])
                        .replace_plan(Arc::new(eager_count_sum.into()))
                });

                // Apply double eager on d and d^1.
                let mut final_double_eager_partial = final_double_eager.clone();
                final_double_eager_partial.mode = AggregateMode::Partial;

                final_agg_finals.push(final_double_eager);
                final_agg_partials.push(final_double_eager_partial);
                final_eval_scalars.push(double_eager_eval_scalar);

                let mut eager_agg_partial = eager_group_by.clone();
                eager_agg_partial.mode = AggregateMode::Partial;
                let mut eager_count_partial = eager_count.clone();
                eager_count_partial.mode = AggregateMode::Partial;
                join_exprs.push(if d == 0 {
                    eval_scalar_expr
                        .replace_children(vec![Arc::new(join_expr.replace_children(vec![
                            Arc::new(SExpr::create_unary(
                                Arc::new(RelOperator::Aggregate(eager_group_by)),
                                Arc::new(SExpr::create_unary(
                                    Arc::new(RelOperator::Aggregate(eager_agg_partial)),
                                    if !eager_extra_eval_scalar_expr[0].items.is_empty() {
                                        Arc::new(SExpr::create_unary(
                                            Arc::new(RelOperator::EvalScalar(
                                                eager_extra_eval_scalar_expr[0].clone(),
                                            )),
                                            Arc::new(join_expr.child(0)?.clone()),
                                        ))
                                    } else {
                                        Arc::new(join_expr.child(0)?.clone())
                                    },
                                )),
                            )),
                            Arc::new(SExpr::create_unary(
                                Arc::new(RelOperator::Aggregate(eager_count)),
                                Arc::new(SExpr::create_unary(
                                    Arc::new(RelOperator::Aggregate(eager_count_partial)),
                                    Arc::new(join_expr.child(1)?.clone()),
                                )),
                            )),
                        ]))])
                        .replace_plan(Arc::new(double_eager_count_sum.into()))
                } else {
                    eval_scalar_expr
                        .replace_children(vec![Arc::new(join_expr.replace_children(vec![
                            Arc::new(SExpr::create_unary(
                                Arc::new(RelOperator::Aggregate(eager_count)),
                                Arc::new(SExpr::create_unary(
                                    Arc::new(RelOperator::Aggregate(eager_count_partial)),
                                    Arc::new(join_expr.child(0)?.clone()),
                                )),
                            )),
                            Arc::new(SExpr::create_unary(
                                Arc::new(RelOperator::Aggregate(eager_group_by)),
                                Arc::new(SExpr::create_unary(
                                    Arc::new(RelOperator::Aggregate(eager_agg_partial)),
                                    if !eager_extra_eval_scalar_expr[1].items.is_empty() {
                                        Arc::new(SExpr::create_unary(
                                            Arc::new(RelOperator::EvalScalar(
                                                eager_extra_eval_scalar_expr[1].clone(),
                                            )),
                                            Arc::new(join_expr.child(1)?.clone()),
                                        ))
                                    } else {
                                        Arc::new(join_expr.child(1)?.clone())
                                    },
                                )),
                            )),
                        ]))])
                        .replace_plan(Arc::new(double_eager_count_sum.into()))
                });
            }
        }

        // Remove redundant group items that propagated from another child.
        for final_agg in final_agg_finals.iter_mut() {
            while final_agg.group_items.len() > original_group_items_len {
                final_agg.group_items.pop();
            }
        }
        for final_agg in final_agg_partials.iter_mut() {
            while final_agg.group_items.len() > original_group_items_len {
                final_agg.group_items.pop();
            }
        }

        // Generate final result.
        for idx in 0..final_agg_finals.len() {
            let temp_final_agg_expr = final_agg_expr
                .replace_children(vec![Arc::new(
                    final_agg_partial_expr
                        .replace_children(vec![Arc::new(join_exprs[idx].clone())])
                        .replace_plan(Arc::new(final_agg_partials[idx].clone().into())),
                )])
                .replace_plan(Arc::new(final_agg_finals[idx].clone().into()));
            let mut result = if has_sort {
                eval_scalar_expr
                    .replace_children(vec![Arc::new(
                        sort_expr.replace_children(vec![Arc::new(temp_final_agg_expr)]),
                    )])
                    .replace_plan(Arc::new(final_eval_scalars[idx].clone().into()))
            } else {
                eval_scalar_expr
                    .replace_children(vec![Arc::new(temp_final_agg_expr)])
                    .replace_plan(Arc::new(final_eval_scalars[idx].clone().into()))
            };
            result.set_applied_rule(&self.id);
            state.add_result(result);
        }

        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }

    fn transformation(&self) -> bool {
        false
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
    _idx: usize,
    agg_final: &Aggregate,
    columns_set: &ColumnSet,
    eval_scalar_items: &HashMap<usize, Vec<usize>>,
    function_factory: &AggregateFunctionFactory,
) -> Vec<(usize, IndexType, String)> {
    agg_final
        .aggregate_functions
        .iter()
        .enumerate()
        .filter_map(|(index, aggregate_item)| {
            if let ScalarExpr::AggregateFunction(aggregate_function) = &aggregate_item.scalar {
                let mut valid = false;
                if aggregate_function.args.len() == 1
                    && function_factory.is_decomposable(&aggregate_function.func_name)
                    && eval_scalar_items.contains_key(&aggregate_item.index)
                {
                    if let ScalarExpr::BoundColumnRef(column) = &aggregate_function.args[0] {
                        if columns_set.contains(&column.column.index) {
                            match &*column.column.data_type {
                                DataType::Number(_) | DataType::Decimal(_) => {
                                    valid = true;
                                }
                                DataType::Nullable(ty) => {
                                    if let DataType::Number(_) | DataType::Decimal(_) = **ty {
                                        valid = true;
                                    }
                                }
                                _ => (),
                            }
                        }
                    }
                }
                if valid {
                    return Some((
                        index,
                        aggregate_item.index,
                        aggregate_function.func_name.clone(),
                    ));
                }
            }
            None
        })
        .collect()
}

// Final aggregate functions's data type = eager aggregate functions's return_type
// For COUNT, func_name: count => sum, return_type: Nullable(UInt64)
fn modify_final_aggregate_function(agg: &mut AggregateFunction, args_index: usize) {
    if agg.func_name.as_str() == "count" {
        agg.func_name = "sum".to_string();
        agg.return_type = Box::new(DataType::Nullable(Box::new(DataType::Number(
            NumberDataType::UInt64,
        ))));
    }
    let agg_func = ScalarExpr::BoundColumnRef(BoundColumnRef {
        span: None,
        column: ColumnBindingBuilder::new(
            "_eager".to_string(),
            args_index,
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

fn add_eager_count(final_agg: &mut Aggregate, metadata: MetadataRef) -> (usize, usize) {
    final_agg
        .aggregate_functions
        .push(final_agg.aggregate_functions[0].clone());

    let eager_count_vec_index = final_agg.aggregate_functions.len() - 1;

    let eager_count_aggregation_function =
        &mut final_agg.aggregate_functions[eager_count_vec_index];

    let eager_count_index = metadata.write().add_derived_column(
        "count(*)".to_string(),
        DataType::Number(NumberDataType::UInt64),
        None,
    );
    if let ScalarExpr::AggregateFunction(agg) = &mut eager_count_aggregation_function.scalar {
        agg.func_name = "count".to_string();
        agg.distinct = false;
        agg.return_type = Box::new(DataType::Number(NumberDataType::UInt64));
        agg.args = vec![];
        agg.display_name = "count(*)".to_string();
    }
    eager_count_aggregation_function.index = eager_count_index;
    (eager_count_index, eager_count_vec_index)
}

// AVG(C) => SUM(C) / COUNT(C)
fn decompose_avg(
    final_agg: &mut Aggregate,
    index: usize,
    func_name: &mut String,
    metadata: MetadataRef,
    function_factory: &AggregateFunctionFactory,
) -> databend_common_exception::Result<(usize, usize, usize)> {
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
            let func = function_factory.get(&agg.func_name, agg.params.clone(), vec![
                *column.column.data_type.clone(),
            ])?;
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
        None,
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
    metadata: MetadataRef,
    eval_scalars: &mut Vec<&mut EvalScalar>,
    eval_scalar_items: &HashMap<usize, Vec<usize>>,
    avg_components: &HashMap<usize, usize>,
) -> databend_common_exception::Result<(bool, usize, usize)> {
    let final_aggregate_function = &mut final_agg.aggregate_functions[index];

    let old_index = final_aggregate_function.index;
    let new_index = metadata.write().add_derived_column(
        format!("_eager_final_{}", &func_name),
        final_aggregate_function.scalar.data_type()?,
        None,
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
    if let Some(indexes) = eval_scalar_items.get(&old_index)
        && !avg_components.contains_key(&old_index)
    {
        for eval_scalar in eval_scalars {
            for item_idx in indexes {
                let eval_scalar_item = &mut (eval_scalar).items[*item_idx];
                if let ScalarExpr::BoundColumnRef(column) = &mut eval_scalar_item.scalar {
                    let column_binding = &mut column.column;
                    column_binding.index = new_index;
                    if func_name == "count" {
                        column_binding.data_type = Box::new(DataType::Nullable(Box::new(
                            DataType::Number(NumberDataType::UInt64),
                        )));
                        eval_scalar_item.scalar = wrap_cast(
                            &eval_scalar_item.scalar,
                            &DataType::Number(NumberDataType::UInt64),
                        );
                    }
                    success = true;
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
    metadata: MetadataRef,
) -> databend_common_exception::Result<ScalarItem> {
    let new_index = metadata.write().add_derived_column(
        format!("{} * _eager_count", aggregate_function.display_name),
        aggregate_function.args[0].data_type()?,
        None,
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
