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

use databend_common_exception::ErrorCode;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::number::NumberDataType;
use databend_common_functions::aggregates::AggregateFunctionFactory;

use crate::ColumnSet;
use crate::MetadataRef;
use crate::ScalarExpr;
use crate::Symbol;
use crate::Visibility;
use crate::binder::ColumnBindingBuilder;
use crate::binder::wrap_cast;
use crate::match_op;
use crate::optimizer::Optimizer;
use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::SExpr;
use crate::optimizer::ir::SExprVisitor;
use crate::optimizer::ir::Side;
use crate::optimizer::ir::VisitAction;
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
use crate::plans::ScalarItem;

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
                match_op!(EvalScalar -> Aggregate -> Aggregate -> Join[*, *]),
                match_op!(EvalScalar -> Aggregate -> Aggregate -> EvalScalar -> Join[*, *]),
                match_op!(EvalScalar -> Sort -> Aggregate -> Aggregate -> Join[*, *]),
                match_op!(EvalScalar -> Sort -> Aggregate -> Aggregate -> EvalScalar -> Join[*, *]),
            ],
            metadata,
        }
    }
}

impl Rule for RuleEagerAggregation {
    fn id(&self) -> RuleID {
        RuleID::EagerAggregation
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }

    fn transformation(&self) -> bool {
        false
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<(), ErrorCode> {
        let Some(input) = EagerInput::parse(s_expr) else {
            return Ok(());
        };

        let join = input.join_expr.plan().as_join().unwrap();
        // Only supports inner/cross join and equal conditions.
        if !matches!(join.join_type, JoinType::Inner | JoinType::Cross)
            | !join.non_equi_conditions.is_empty()
        {
            return Ok(());
        }

        for analysis in input.analyze(&self.metadata)? {
            if let Some(mut result) = analysis.apply(&self.metadata, &input)? {
                result.set_applied_rule(&self.id());
                state.add_result(result);
            }
        }

        Ok(())
    }
}

#[derive(Clone)]
struct EagerInput<'a> {
    eval_scalar: &'a EvalScalar,
    sort_expr: Option<&'a SExpr>,
    join_expr: &'a SExpr,
    extra_eval_scalar: EvalScalar,
    final_agg: Aggregate,
}

impl<'a> EagerInput<'a> {
    fn parse(s_expr: &'a SExpr) -> Option<Self> {
        let eval_scalar = s_expr.plan().as_eval_scalar()?;

        let mut current = s_expr.unary_child();
        let sort_expr = current.plan().as_sort().map(|_| {
            let sort_expr = current;
            current = sort_expr.unary_child();
            sort_expr
        });

        let final_agg = current.plan().as_aggregate()?.clone();
        current = current.unary_child();

        current.plan().as_aggregate()?;
        current = current.unary_child();

        let extra_eval_scalar = current
            .plan()
            .as_eval_scalar()
            .map(|eval| {
                let eval = eval.clone();
                current = current.unary_child();
                eval
            })
            .unwrap_or_else(|| EvalScalar { items: vec![] });

        current.plan().as_join()?;

        Some(Self {
            eval_scalar,
            sort_expr,
            join_expr: current,
            extra_eval_scalar,
            final_agg,
        })
    }

    fn analyze(&self, metadata: &MetadataRef) -> Result<Vec<EagerAnalysis>, ErrorCode> {
        let mut join_columns = Pair::try_new_with(|side| {
            Ok::<_, ErrorCode>(
                side.child(self.join_expr)
                    .derive_relational_prop()?
                    .output_columns
                    .clone(),
            )
        })?;

        let Some(eager_extra_eval_scalar_expr) =
            self.split_extra_eval_scalar_items(&mut join_columns, metadata)?
        else {
            return Ok(vec![]);
        };

        let eval_scalar_used_columns = self
            .eval_scalar
            .items
            .iter()
            .flat_map(|item| item.scalar.used_columns())
            .collect::<ColumnSet>();

        if self.has_mixed_sum_and_count_aggregates(&eval_scalar_used_columns) {
            return Ok(vec![]);
        }

        let function_factory = AggregateFunctionFactory::instance();
        let eager_candidates = EagerCandidates::collect(
            &self.final_agg,
            &join_columns,
            &eval_scalar_used_columns,
            function_factory,
        );

        if eager_candidates.by_side[Side::Left].len()
            + eager_candidates.by_side[Side::Right].len()
            + eager_candidates.any_side.len()
            != self.final_agg.aggregate_functions.len()
        {
            return Ok(vec![]);
        }

        let mut eager_group_columns = Pair {
            left: ColumnSet::new(),
            right: ColumnSet::new(),
        };
        for group_item in &self.final_agg.group_items {
            let ScalarExpr::BoundColumnRef(_) = &group_item.scalar else {
                return Ok(vec![]);
            };
            join_columns.for_each(|side, columns| {
                if columns.contains(&group_item.index) {
                    eager_group_columns[side].insert(group_item.index);
                }
            });
        }

        let (left_conditions, right_conditions): (Vec<_>, _) = self
            .join_expr
            .plan()
            .as_join()
            .unwrap()
            .equi_conditions
            .iter()
            .map(|condition| (&condition.left, &condition.right))
            .unzip();
        let join_conditions = Pair {
            left: left_conditions,
            right: right_conditions,
        };

        let original_group_items_len = self.final_agg.group_items.len();
        let mut final_agg = self.final_agg.clone();
        for condition in &self.join_expr.plan().as_join().unwrap().equi_conditions {
            let ScalarExpr::BoundColumnRef(left_column) = &condition.left else {
                return Ok(vec![]);
            };
            let ScalarExpr::BoundColumnRef(right_column) = &condition.right else {
                return Ok(vec![]);
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

        let can_eager = Pair::new_with(|side| {
            join_conditions[side]
                .iter()
                .all(|cond| cond.used_columns().is_subset(&eager_group_columns[side]))
        });
        if !can_eager[Side::Left] && !can_eager[Side::Right] {
            return Ok(vec![]);
        }

        let eager_aggregation_variants = eager_candidates.assignments();
        Ok(eager_aggregation_variants
            .into_iter()
            .flat_map(|assignment| {
                self.expand_analyses(
                    &final_agg,
                    original_group_items_len,
                    &join_columns,
                    &eager_extra_eval_scalar_expr,
                    &can_eager,
                    assignment,
                )
            })
            .collect())
    }

    fn has_mixed_sum_and_count_aggregates(&self, eval_scalar_used_columns: &ColumnSet) -> bool {
        let mut has_sum = false;
        let mut has_count = false;

        for aggregate in &self.final_agg.aggregate_functions {
            if !eval_scalar_used_columns.contains(&aggregate.index) {
                continue;
            }

            let ScalarExpr::AggregateFunction(aggregate_function) = &aggregate.scalar else {
                continue;
            };

            match aggregate_function.func_name.as_str() {
                "sum" => has_sum = true,
                "count" => has_count = true,
                _ => {}
            }

            if has_sum && has_count {
                // Mixed sum/count outputs are used by rewrites such as AVG = SUM / COUNT.
                // The current eager-count rewrite does not preserve this shape, so keep the
                // original aggregate plan instead of applying eager aggregation.
                return true;
            }
        }

        false
    }

    fn expand_analyses(
        &self,
        final_agg: &Aggregate,
        original_group_items_len: usize,
        join_columns: &Pair<ColumnSet>,
        eager_extra_eval_scalar_expr: &Pair<EvalScalar>,
        can_eager: &Pair<bool>,
        assignment: EagerAssignment,
    ) -> Vec<EagerAnalysis> {
        let can_push_down = Pair {
            left: !assignment.eager_aggregations[Side::Left].is_empty() && can_eager[Side::Left],
            right: !assignment.eager_aggregations[Side::Right].is_empty() && can_eager[Side::Right],
        };

        if can_push_down[Side::Left] && can_push_down[Side::Right] {
            return vec![
                EagerAnalysis {
                    final_agg: final_agg.clone(),
                    original_group_items_len,
                    join_columns: join_columns.clone(),
                    eager_extra_eval_scalar_expr: eager_extra_eval_scalar_expr.clone(),
                    eager_aggregations: assignment.eager_aggregations.clone(),
                    can_eager: can_eager.clone(),
                    rewrite_kind: EagerRewriteKind::DoubleGroupByCount(Side::Left),
                },
                EagerAnalysis {
                    final_agg: final_agg.clone(),
                    original_group_items_len,
                    join_columns: join_columns.clone(),
                    eager_extra_eval_scalar_expr: eager_extra_eval_scalar_expr.clone(),
                    eager_aggregations: assignment.eager_aggregations.clone(),
                    can_eager: can_eager.clone(),
                    rewrite_kind: EagerRewriteKind::DoubleSplit(Side::Left),
                },
            ];
        }

        let d = if can_push_down[Side::Left] {
            Side::Left
        } else if can_push_down[Side::Right] {
            Side::Right
        } else {
            return vec![];
        };

        if !assignment.eager_aggregations[d.opposite()].is_empty() {
            return vec![];
        }

        let mut analyses = vec![EagerAnalysis {
            final_agg: final_agg.clone(),
            original_group_items_len,
            join_columns: join_columns.clone(),
            eager_extra_eval_scalar_expr: eager_extra_eval_scalar_expr.clone(),
            eager_aggregations: assignment.eager_aggregations.clone(),
            can_eager: can_eager.clone(),
            rewrite_kind: EagerRewriteKind::SingleGroupBy(d),
        }];

        if can_eager[d.opposite()] {
            if self.has_sum_aggregate(final_agg) {
                analyses.push(EagerAnalysis {
                    final_agg: final_agg.clone(),
                    original_group_items_len,
                    join_columns: join_columns.clone(),
                    eager_extra_eval_scalar_expr: eager_extra_eval_scalar_expr.clone(),
                    eager_aggregations: assignment.eager_aggregations.clone(),
                    can_eager: can_eager.clone(),
                    rewrite_kind: EagerRewriteKind::SingleCount(d),
                });
            }
            if assignment.allow_double_eager
                && self.needs_eager_count_sum(final_agg, &assignment, d)
            {
                analyses.push(EagerAnalysis {
                    final_agg: final_agg.clone(),
                    original_group_items_len,
                    join_columns: join_columns.clone(),
                    eager_extra_eval_scalar_expr: eager_extra_eval_scalar_expr.clone(),
                    eager_aggregations: assignment.eager_aggregations.clone(),
                    can_eager: can_eager.clone(),
                    rewrite_kind: EagerRewriteKind::SingleDouble(d),
                });
            }
        }

        analyses
    }

    fn has_sum_aggregate(&self, final_agg: &Aggregate) -> bool {
        final_agg.aggregate_functions.iter().any(|agg| {
            matches!(
                &agg.scalar,
                ScalarExpr::AggregateFunction(aggregate_function)
                    if aggregate_function.func_name == "sum"
            )
        })
    }

    fn needs_eager_count_sum(
        &self,
        final_agg: &Aggregate,
        assignment: &EagerAssignment,
        d: Side,
    ) -> bool {
        self.has_sum_aggregate(final_agg)
            || assignment.eager_aggregations[d].iter().any(|candidate| {
                matches!(
                    &final_agg.aggregate_functions[candidate.agg_index].scalar,
                    ScalarExpr::AggregateFunction(aggregate_function)
                        if aggregate_function.func_name == "count"
                )
            })
    }

    fn split_extra_eval_scalar_items(
        &self,
        join_columns: &mut Pair<ColumnSet>,
        metadata: &MetadataRef,
    ) -> Result<Option<Pair<EvalScalar>>, ErrorCode> {
        let mut eager_extra_eval_scalar_expr = Pair {
            left: EvalScalar { items: vec![] },
            right: EvalScalar { items: vec![] },
        };

        for eval_item in &self.extra_eval_scalar.items {
            let eval_used_columns = eval_item.scalar.used_columns();
            if eval_used_columns
                .iter()
                .any(|column| metadata.read().is_removed_mark_index(*column))
            {
                continue;
            }

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
                return Ok(None);
            }
        }

        Ok(Some(eager_extra_eval_scalar_expr))
    }
}

struct EagerCandidates {
    by_side: Pair<Vec<EagerAggregationCandidate>>,
    any_side: Vec<EagerAggregationCandidate>,
}

struct EagerAssignment {
    eager_aggregations: Pair<Vec<EagerAggregationCandidate>>,
    allow_double_eager: bool,
}

impl EagerCandidates {
    // In the current implementation, if an aggregation function can be eager,
    // then it needs to satisfy the following constraints:
    // (1) The args.len() must equal to 1 if func_name is not "count".
    // (2) The aggregate function can be decomposed.
    // (3) The output index of the aggregate function is referenced by the top eval scalar.
    // (4) The data type of the aggregate column is either Number or Nullable(Number).
    // Return eager aggregation candidates grouped by side, plus the count(*) candidates that can
    // be assigned to either side.
    fn collect(
        agg_final: &Aggregate,
        join_columns: &Pair<ColumnSet>,
        eval_scalar_used_columns: &ColumnSet,
        function_factory: &AggregateFunctionFactory,
    ) -> Self {
        let mut candidates = Self {
            by_side: Pair::new_with(|_| vec![]),
            any_side: vec![],
        };

        for (index, func) in agg_final.aggregate_functions.iter().enumerate() {
            let ScalarExpr::AggregateFunction(aggregate_function) = &func.scalar else {
                continue;
            };
            if !function_factory.is_decomposable(&aggregate_function.func_name)
                || !eval_scalar_used_columns.contains(&func.index)
            {
                continue;
            }

            let candidate = EagerAggregationCandidate {
                agg_index: index,
                output_index: func.index,
            };

            if aggregate_function.func_name == "count" && aggregate_function.args.is_empty() {
                candidates.any_side.push(candidate);
                continue;
            }

            let [ScalarExpr::BoundColumnRef(column)] = &aggregate_function.args[..] else {
                continue;
            };
            match &*column.column.data_type {
                DataType::Number(_) | DataType::Decimal(_) => (),
                DataType::Nullable(box DataType::Number(_) | box DataType::Decimal(_)) => {}
                _ => continue,
            }

            for side in [Side::Left, Side::Right] {
                if join_columns[side].contains(&column.column.index) {
                    candidates.by_side[side].push(candidate);
                    break;
                }
            }
        }

        candidates
    }

    fn assignments(&self) -> Vec<EagerAssignment> {
        if self.any_side.is_empty() {
            return vec![EagerAssignment {
                eager_aggregations: self.by_side.clone(),
                allow_double_eager: true,
            }];
        }

        match (
            self.by_side[Side::Left].is_empty(),
            self.by_side[Side::Right].is_empty(),
        ) {
            // For count(*)-only cases, keep both single-side variants, but emit the symmetric
            // double-eager rewrite only once from the canonical left assignment.
            (true, true) => vec![
                self.assignment(Side::Left, true),
                self.assignment(Side::Right, false),
            ],
            // If the eagerable aggs already belong to one side, keep count(*) with them.
            (false, true) => vec![self.assignment(Side::Left, true)],
            (true, false) => vec![self.assignment(Side::Right, true)],
            // Only keep both variants when both sides already have side-bound eager aggs.
            (false, false) => vec![
                self.assignment(Side::Left, true),
                self.assignment(Side::Right, true),
            ],
        }
    }

    fn assignment(&self, side: Side, allow_double_eager: bool) -> EagerAssignment {
        let mut eager_aggregations = self.by_side.clone();
        eager_aggregations[side].extend(self.any_side.iter().copied());
        EagerAssignment {
            eager_aggregations,
            allow_double_eager,
        }
    }
}

struct EagerAnalysis {
    final_agg: Aggregate,
    original_group_items_len: usize,
    join_columns: Pair<ColumnSet>,
    eager_extra_eval_scalar_expr: Pair<EvalScalar>,
    eager_aggregations: Pair<Vec<EagerAggregationCandidate>>,
    can_eager: Pair<bool>,
    rewrite_kind: EagerRewriteKind,
}

#[derive(Clone, Copy)]
enum EagerRewriteKind {
    DoubleGroupByCount(Side),
    DoubleSplit(Side),
    SingleGroupBy(Side),
    SingleCount(Side),
    SingleDouble(Side),
}

#[derive(Clone, Copy, Debug)]
struct EagerAggregationCandidate {
    agg_index: usize,
    output_index: Symbol,
}

impl EagerAnalysis {
    fn apply(self, metadata: &MetadataRef, input: &EagerInput) -> Result<Option<SExpr>, ErrorCode> {
        match self.rewrite_kind {
            // EvalScalar
            // └── Aggregate(Final: rewrite agg on child(d), keep agg on child(d^1) as count-adjusted)
            //     └── Join
            //         ├── child(d):     Aggregate(Final: eager group-by + eager count)
            //         └── child(d^1):   original
            EagerRewriteKind::DoubleGroupByCount(d) => {
                self.apply_double_group_by_count(metadata, input, d)
            }
            // EvalScalar
            // └── Aggregate(Final: rewrite aggs on both children, each multiplied by the opposite count)
            //     └── Join
            //         ├── child(d):     Aggregate(Final: eager group-by + eager count)
            //         └── child(d^1):   Aggregate(Final: eager group-by + eager count)
            EagerRewriteKind::DoubleSplit(d) => self.apply_double_split(metadata, input, d),
            // EvalScalar
            // └── Aggregate(Final: rewrite agg on child(d))
            //     └── Join
            //         ├── child(d):     Aggregate(Final: eager group-by)
            //         └── child(d^1):   original
            EagerRewriteKind::SingleGroupBy(d) => self.apply_single_group_by(metadata, input, d),
            // EvalScalar
            // └── Aggregate(Final: keep original agg, multiply by count from child(d^1))
            //     └── Join
            //         ├── child(d):     original
            //         └── child(d^1):   Aggregate(Final: eager count)
            EagerRewriteKind::SingleCount(d) => self.apply_single_count(metadata, input, d),
            // EvalScalar
            // └── Aggregate(Final: rewrite agg on child(d), then multiply by count from child(d^1))
            //     └── Join
            //         ├── child(d):     Aggregate(Final: eager group-by)
            //         └── child(d^1):   Aggregate(Final: eager count)
            EagerRewriteKind::SingleDouble(d) => self.apply_single_double(metadata, input, d),
        }
    }

    fn apply_double_split(
        self,
        metadata: &MetadataRef,
        input: &EagerInput,
        d: Side,
    ) -> Result<Option<SExpr>, ErrorCode> {
        let mut final_eager_split = self.final_agg.clone();

        let mut eager_split_eval_scalar = input.eval_scalar.clone();

        let mut rewrites = EagerOutputRewriteMap::default();
        let mut success = false;

        [Side::Left, Side::Right].iter().try_for_each(|side| {
            for candidate in &self.eager_aggregations[*side] {
                let aggr_function = &mut final_eager_split.aggregate_functions[candidate.agg_index];
                let eval_scalars_items = eager_split_eval_scalar.items.iter_mut();
                let rewrite = Self::rewrite_aggregate_output(
                    metadata,
                    aggr_function,
                    *side,
                    eval_scalars_items,
                )?;
                success |= rewrite.rewritten_in_eval;
                rewrites.record(rewrite.binding);
            }
            Ok::<_, ErrorCode>(())
        })?;

        let mut eager_split_count_sum = EvalScalar { items: vec![] };
        let eager_group_by_and_eager_count = Pair::new_with(|side| {
            let mut aggregate = self.pruned_aggregate_for_side(side);
            let (count_index, _) = Self::add_eager_count(metadata, &mut aggregate);
            (aggregate, count_index)
        });

        for agg in final_eager_split.aggregate_functions.iter_mut() {
            if let ScalarExpr::AggregateFunction(aggregate_function) = &mut agg.scalar {
                if aggregate_function.func_name != "sum" {
                    continue;
                }
                let Some(agg_side) = rewrites.source_side(agg.index) else {
                    continue;
                };
                eager_split_count_sum
                    .items
                    .push(Self::create_eager_count_multiply_scalar_item(
                        metadata,
                        input,
                        aggregate_function,
                        eager_group_by_and_eager_count[agg_side.opposite()].1,
                    )?);
            }
        }

        if !success {
            return Ok(None);
        }

        let split_join = input
            .join_expr
            .replace_side_child(
                d,
                d.build_join_child(
                    input.join_expr,
                    &self.eager_extra_eval_scalar_expr[d],
                    eager_group_by_and_eager_count[d].0.clone(),
                ),
            )
            .replace_side_child(
                d.opposite(),
                d.opposite().build_join_child(
                    input.join_expr,
                    &self.eager_extra_eval_scalar_expr[d.opposite()],
                    eager_group_by_and_eager_count[d.opposite()].0.clone(),
                ),
            );

        Ok(Some(self.build_result(
            input,
            split_join.build_unary(eager_split_count_sum),
            final_eager_split,
            eager_split_eval_scalar,
        )))
    }

    fn apply_double_group_by_count(
        self,
        metadata: &MetadataRef,
        input: &EagerInput,
        d: Side,
    ) -> Result<Option<SExpr>, ErrorCode> {
        let mut final_eager_groupby_count = self.final_agg.clone();
        let mut eager_groupby_count_scalar = input.eval_scalar.clone();
        let mut rewrites = EagerOutputRewriteMap::default();
        let mut success = false;

        for candidate in &self.eager_aggregations[d] {
            let aggr_function =
                &mut final_eager_groupby_count.aggregate_functions[candidate.agg_index];
            let rewrite = Self::rewrite_aggregate_output(
                metadata,
                aggr_function,
                d,
                eager_groupby_count_scalar.items.iter_mut(),
            )?;
            success |= rewrite.rewritten_in_eval;
            rewrites.record(rewrite.binding);
        }

        if !success {
            return Ok(None);
        }

        let mut eager_group_by_and_eager_count = self.pruned_aggregate_for_side(d);
        let (eager_count_index, _) =
            Self::add_eager_count(metadata, &mut eager_group_by_and_eager_count);

        let mut eager_groupby_count_count_sum = EvalScalar { items: vec![] };
        for agg in final_eager_groupby_count.aggregate_functions.iter_mut() {
            if let ScalarExpr::AggregateFunction(aggregate_function) = &mut agg.scalar {
                if aggregate_function.func_name != "sum"
                    || rewrites.source_side(agg.index) == Some(d)
                {
                    continue;
                }
                eager_groupby_count_count_sum.items.push(
                    Self::create_eager_count_multiply_scalar_item(
                        metadata,
                        input,
                        aggregate_function,
                        eager_count_index,
                    )?,
                );
            }
        }

        let groupby_count_join = input.join_expr.replace_side_child(
            d,
            d.build_join_child(
                input.join_expr,
                &self.eager_extra_eval_scalar_expr[d],
                eager_group_by_and_eager_count,
            ),
        );

        let groupby_count_join = if eager_groupby_count_count_sum.items.is_empty() {
            groupby_count_join
        } else {
            groupby_count_join.build_unary(eager_groupby_count_count_sum)
        };

        Ok(Some(self.build_result(
            input,
            groupby_count_join,
            final_eager_groupby_count,
            eager_groupby_count_scalar,
        )))
    }

    fn apply_single_group_by(
        self,
        metadata: &MetadataRef,
        input: &EagerInput,
        d: Side,
    ) -> Result<Option<SExpr>, ErrorCode> {
        let mut final_eager_group_by = self.final_agg.clone();
        let mut eager_group_by_eval_scalar = input.eval_scalar.clone();
        let mut success = false;

        for candidate in &self.eager_aggregations[d] {
            let aggr_function = &mut final_eager_group_by.aggregate_functions[candidate.agg_index];
            let rewrite = Self::rewrite_aggregate_output(
                metadata,
                aggr_function,
                d,
                eager_group_by_eval_scalar.items.iter_mut(),
            )?;
            success |= rewrite.rewritten_in_eval;
        }

        if !success {
            return Ok(None);
        }

        let eager_group_by = self.pruned_aggregate_for_side(d);

        let group_by_child = Arc::new(d.build_join_child(
            input.join_expr,
            &self.eager_extra_eval_scalar_expr[d],
            eager_group_by,
        ));

        Ok(Some(self.build_result(
            input,
            input.join_expr.replace_side_child(d, group_by_child),
            final_eager_group_by,
            eager_group_by_eval_scalar,
        )))
    }

    fn apply_single_count(
        self,
        metadata: &MetadataRef,
        input: &EagerInput,
        d: Side,
    ) -> Result<Option<SExpr>, ErrorCode> {
        if !self.can_eager[d.opposite()] {
            return Ok(None);
        }

        let mut final_eager_count = self.final_agg.clone();
        let mut eager_count = self.pruned_aggregate_for_side(d.opposite());
        let (eager_count_index, _) = Self::add_eager_count(metadata, &mut eager_count);

        let mut eager_count_sum = EvalScalar { items: vec![] };
        for agg in final_eager_count.aggregate_functions.iter_mut() {
            if let ScalarExpr::AggregateFunction(aggregate_function) = &mut agg.scalar
                && aggregate_function.func_name == "sum"
            {
                eager_count_sum
                    .items
                    .push(Self::create_eager_count_multiply_scalar_item(
                        metadata,
                        input,
                        aggregate_function,
                        eager_count_index,
                    )?);
            }
        }

        if eager_count_sum.items.is_empty() {
            return Ok(None);
        }

        let new_join = input.join_expr.replace_side_child(
            d.opposite(),
            d.opposite().build_join_child(
                input.join_expr,
                &EvalScalar { items: vec![] },
                eager_count,
            ),
        );

        Ok(Some(self.build_result(
            input,
            new_join.build_unary(eager_count_sum),
            final_eager_count,
            input.eval_scalar.clone(),
        )))
    }

    fn apply_single_double(
        self,
        metadata: &MetadataRef,
        input: &EagerInput,
        d: Side,
    ) -> Result<Option<SExpr>, ErrorCode> {
        if !self.can_eager[d.opposite()] {
            return Ok(None);
        }

        let mut final_double_eager = self.final_agg.clone();
        let mut double_eager_eval_scalar = input.eval_scalar.clone();
        let mut success = false;

        for candidate in &self.eager_aggregations[d] {
            let aggr_function = &mut final_double_eager.aggregate_functions[candidate.agg_index];
            let rewrite = Self::rewrite_aggregate_output(
                metadata,
                aggr_function,
                d,
                double_eager_eval_scalar.items.iter_mut(),
            )?;
            success |= rewrite.rewritten_in_eval;
        }

        if !success {
            return Ok(None);
        }

        let eager_group_by = self.pruned_aggregate_for_side(d);

        let mut eager_count = self.pruned_aggregate_for_side(d.opposite());
        let (eager_count_index, _) = Self::add_eager_count(metadata, &mut eager_count);
        let mut double_eager_count_sum = EvalScalar { items: vec![] };
        for agg in final_double_eager.aggregate_functions.iter_mut() {
            if let ScalarExpr::AggregateFunction(aggregate_function) = &mut agg.scalar
                && aggregate_function.func_name == "sum"
            {
                double_eager_count_sum
                    .items
                    .push(Self::create_eager_count_multiply_scalar_item(
                        metadata,
                        input,
                        aggregate_function,
                        eager_count_index,
                    )?);
            }
        }

        if double_eager_count_sum.items.is_empty() {
            return Ok(None);
        }

        let group_by_child = Arc::new(d.build_join_child(
            input.join_expr,
            &self.eager_extra_eval_scalar_expr[d],
            eager_group_by,
        ));
        let new_join = input.join_expr.replace_side_child(
            d.opposite(),
            d.opposite().build_join_child(
                input.join_expr,
                &EvalScalar { items: vec![] },
                eager_count,
            ),
        );

        Ok(Some(
            self.build_result(
                input,
                new_join
                    .replace_side_child(d, group_by_child)
                    .build_unary(double_eager_count_sum),
                final_double_eager,
                double_eager_eval_scalar,
            ),
        ))
    }

    fn build_result(
        &self,
        input: &EagerInput,
        s_expr: SExpr,
        mut final_aggr: Aggregate,
        eval_scalar: EvalScalar,
    ) -> SExpr {
        final_aggr
            .group_items
            .truncate(self.original_group_items_len);
        let plan = s_expr
            .build_unary(Aggregate {
                mode: AggregateMode::Partial,
                ..final_aggr.clone()
            })
            .build_unary(final_aggr);
        let plan = match input.sort_expr {
            Some(sort_expr) => plan.build_unary(sort_expr.plan.clone()),
            None => plan,
        };
        plan.build_unary(eval_scalar)
    }

    fn pruned_aggregate_for_side(&self, side: Side) -> Aggregate {
        Aggregate {
            group_items: self
                .final_agg
                .group_items
                .iter()
                .filter(|item| self.join_columns[side].contains(&item.index))
                .cloned()
                .collect(),
            aggregate_functions: self
                .final_agg
                .aggregate_functions
                .iter()
                .filter(|item| {
                    self.eager_aggregations[side.opposite()]
                        .iter()
                        .all(|candidate| candidate.output_index != item.index)
                })
                .cloned()
                .collect(),
            ..self.final_agg.clone()
        }
    }

    fn add_eager_count(metadata: &MetadataRef, final_agg: &mut Aggregate) -> (Symbol, usize) {
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

    fn rewrite_aggregate_output<'b>(
        metadata: &MetadataRef,
        aggr_function: &mut ScalarItem,
        source_side: Side,
        eval_scalar_items: impl Iterator<Item = &'b mut ScalarItem>,
    ) -> Result<AggregateOutputRewrite, ErrorCode> {
        let ScalarExpr::AggregateFunction(agg) = &mut aggr_function.scalar else {
            unreachable!()
        };
        let was_count = agg.func_name == "count";

        let old_index = aggr_function.index;
        let new_index = metadata.write().add_derived_column(
            format!("_eager_final_{}", agg.func_name),
            *(agg.return_type).clone(),
        );

        Self::modify_final_aggregate_function(agg, old_index);
        aggr_function.index = new_index;

        let mut success = false;
        let count_output = was_count.then(|| Self::rewritten_count_output(new_index));

        for scalar_item in eval_scalar_items {
            if !scalar_item.scalar.used_columns().contains(&old_index) {
                continue;
            }

            if let Some(count_output) = &count_output {
                scalar_item
                    .scalar
                    .replace_column_with_scalar(old_index, count_output)?;
            } else {
                scalar_item.scalar.replace_column(old_index, new_index)?;
            }
            success = true;
        }
        Ok(AggregateOutputRewrite {
            binding: EagerOutputRewrite {
                source_side,
                original_index: old_index,
                rewritten_index: new_index,
            },
            rewritten_in_eval: success,
        })
    }

    fn modify_final_aggregate_function(agg: &mut AggregateFunction, old_index: Symbol) {
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
            agg.args.push(agg_func);
        } else {
            agg.args[0] = agg_func;
        }
    }

    fn rewritten_count_output(new_index: Symbol) -> ScalarExpr {
        wrap_cast(
            &ScalarExpr::BoundColumnRef(BoundColumnRef {
                span: None,
                column: ColumnBindingBuilder::new(
                    "_eager_final_count".to_string(),
                    new_index,
                    Box::new(UInt64Type::data_type().wrap_nullable()),
                    Visibility::Visible,
                )
                .build(),
            }),
            &UInt64Type::data_type(),
        )
    }

    fn create_eager_count_multiply_scalar_item(
        metadata: &MetadataRef,
        input: &EagerInput,
        aggregate_function: &mut AggregateFunction,
        eager_count_index: Symbol,
    ) -> Result<ScalarItem, ErrorCode> {
        let new_scalar = if let ScalarExpr::BoundColumnRef(column) = &aggregate_function.args[0] {
            let mut scalar_idx = input.extra_eval_scalar.items.len();
            for (idx, eval_scalar) in input.extra_eval_scalar.items.iter().enumerate() {
                if eval_scalar.index == column.column.index {
                    scalar_idx = idx;
                }
            }
            if scalar_idx != input.extra_eval_scalar.items.len() {
                input.extra_eval_scalar.items[scalar_idx].scalar.clone()
            } else {
                aggregate_function.args[0].clone()
            }
        } else {
            unreachable!()
        };

        let multiplied_scalar = ScalarExpr::FunctionCall(FunctionCall {
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
        });
        let multiplied_type = multiplied_scalar.data_type()?;
        let new_index = metadata.write().add_derived_column(
            format!("{} * _eager_count", aggregate_function.display_name),
            multiplied_type.clone(),
        );

        let new_scalar_item = ScalarItem {
            scalar: multiplied_scalar,
            index: new_index,
        };
        aggregate_function.return_type = Box::new(
            AggregateFunctionFactory::instance()
                .get(
                    &aggregate_function.func_name,
                    aggregate_function.params.clone(),
                    vec![multiplied_type.clone()],
                    vec![],
                )?
                .return_type()?,
        );
        if let ScalarExpr::BoundColumnRef(column) = &mut aggregate_function.args[0] {
            column.column.index = new_index;
            column.column.data_type = Box::new(multiplied_type);
        }
        Ok(new_scalar_item)
    }
}

struct AggregateOutputRewrite {
    binding: EagerOutputRewrite,
    rewritten_in_eval: bool,
}

#[derive(Clone, Copy, Debug)]
struct EagerOutputRewrite {
    source_side: Side,
    original_index: Symbol,
    rewritten_index: Symbol,
}

#[derive(Default)]
struct EagerOutputRewriteMap {
    rewrites: Vec<EagerOutputRewrite>,
}

impl EagerOutputRewriteMap {
    fn record(&mut self, rewrite: EagerOutputRewrite) {
        self.rewrites.push(rewrite);
    }

    fn source_side(&self, index: Symbol) -> Option<Side> {
        self.rewrites.iter().find_map(|rewrite| {
            ((rewrite.original_index == index) || (rewrite.rewritten_index == index))
                .then_some(rewrite.source_side)
        })
    }
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

#[async_trait::async_trait]
impl Optimizer for RuleEagerAggregation {
    fn name(&self) -> String {
        "RuleEagerAggregationOptimizer".to_string()
    }

    async fn optimize(&mut self, s_expr: &SExpr) -> Result<SExpr, ErrorCode> {
        self.optimize_sync(s_expr)
    }
}

unsafe impl Sync for RuleEagerAggregation {}
unsafe impl Send for RuleEagerAggregation {}

impl RuleEagerAggregation {
    pub fn optimize_sync(&mut self, s_expr: &SExpr) -> Result<SExpr, ErrorCode> {
        s_expr
            .clone()
            .accept(self)
            .map(|res| res.unwrap_or_else(|| s_expr.clone()))
    }
}

impl SExprVisitor for RuleEagerAggregation {
    fn visit(&mut self, expr: &SExpr) -> Result<VisitAction, ErrorCode> {
        for (i, matcher) in self.matchers.iter().enumerate() {
            let mut state = TransformResult::new();
            if matcher.matches(expr) {
                self.apply_matcher(i, expr, &mut state)?;
                let results = state.results();
                if results.is_empty() {
                    continue;
                } else {
                    return Ok(VisitAction::Replace(results[results.len() - 1].clone()));
                }
            }
        }
        Ok(VisitAction::Continue)
    }
}
