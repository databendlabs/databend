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

use databend_common_exception::Result;
use databend_common_expression::types::DataType;

use crate::ColumnSet;
use crate::ScalarExpr;
use crate::Symbol;
use crate::binder::conjunctions;
use crate::optimizer::Optimizer;
use crate::optimizer::ir::ExprVisitor;
use crate::optimizer::ir::SExpr;
use crate::optimizer::ir::VisitAction;
use crate::optimizer::optimizers::partition_filter::window_filter_columns;
use crate::optimizer::optimizers::rule::eliminate_outer_join_type;
use crate::plans::Aggregate;
use crate::plans::Filter;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::RelOperator;
use crate::plans::Window;
use crate::plans::WindowFuncType;

/// Simplify outer joins before filter pull-up. A requirement says that NULL on
/// this input edge cannot contribute to the enclosing result, not that the
/// input already produces non-NULL values. Keep the original predicates and
/// never materialize the requirements as filters.
pub struct SimplifyOuterJoinOptimizer;

#[async_trait::async_trait]
impl Optimizer for SimplifyOuterJoinOptimizer {
    fn name(&self) -> String {
        "SimplifyOuterJoinOptimizer".to_string()
    }

    async fn optimize(&mut self, expr: SExpr) -> Result<SExpr> {
        Ok(expr
            .accept(&mut SimplifyOuterJoinVisitor::default())?
            .unwrap_or(expr))
    }
}

#[derive(Default)]
struct SimplifyOuterJoinVisitor {
    frames: Vec<RewriteFrame>,
    incoming_child_index: usize,
}

enum RewriteFrame {
    // Stop inherited requirements, but still visit children for local sources.
    Barrier,
    Unary(ColumnSet),
    Binary([ColumnSet; 2]),
    // Decide before descending so children see the new join type. Apply after
    // the walker has incorporated the rewritten children.
    RewriteJoin {
        child_required: [ColumnSet; 2],
        join_type: JoinType,
    },
}

impl ExprVisitor for SimplifyOuterJoinVisitor {
    fn visit(&mut self, expr: &SExpr) -> Result<VisitAction> {
        let empty = ColumnSet::new();
        let required = match self.frames.last() {
            Some(RewriteFrame::Unary(required)) => {
                assert_eq!(self.incoming_child_index, 0);
                required
            }
            Some(
                RewriteFrame::Binary(required)
                | RewriteFrame::RewriteJoin {
                    child_required: required,
                    ..
                },
            ) => &required[self.incoming_child_index],
            Some(RewriteFrame::Barrier) | None => &empty,
        };
        self.frames.push(prepare_rewrite(expr, required)?);
        Ok(VisitAction::Continue)
    }

    fn before_child(&mut self, index: usize) {
        self.incoming_child_index = index;
    }

    fn post_visit(&mut self, expr: &SExpr) -> Result<VisitAction> {
        let frame = self.frames.pop().expect("post-visit must follow visit");
        let RewriteFrame::RewriteJoin { join_type, .. } = frame else {
            return Ok(VisitAction::Continue);
        };
        let join = expr.plan().as_join().expect("join rewrite requires a join");
        Ok(VisitAction::Replace(expr.replace_plan(RelOperator::Join(
            Join {
                join_type,
                ..join.clone()
            },
        ))))
    }
}

fn prepare_rewrite(expr: &SExpr, required: &ColumnSet) -> Result<RewriteFrame> {
    Ok(match expr.plan() {
        RelOperator::Filter(filter) => match filter_requirements(filter) {
            Some(mut columns) => {
                columns.extend(required);
                RewriteFrame::Unary(columns)
            }
            None => RewriteFrame::Barrier,
        },
        // Removing rows earlier must not skip evaluation of another item
        // that could throw or observe the removed rows. Only pass through
        // projections of columns, literals, and nullability-only casts.
        RelOperator::EvalScalar(eval)
            if !required.is_empty()
                && eval.items.iter().all(|item| is_simple_value(&item.scalar)) =>
        {
            RewriteFrame::Unary(
                required
                    .iter()
                    .filter_map(|column| {
                        match eval.items.iter().find(|item| item.index == *column) {
                            Some(item) => input_column(&item.scalar),
                            None => Some(*column),
                        }
                    })
                    .collect(),
            )
        }
        RelOperator::UnionAll(union)
            if !required.is_empty()
                && union.logical_recursive_cte_id.is_none()
                && union.cte_scan_names.is_empty() =>
        {
            RewriteFrame::Binary([&union.left_outputs, &union.right_outputs].map(|outputs| {
                if !outputs.iter().all(|(_, expr)| {
                    expr.as_ref()
                        .is_none_or(|expr| input_column(expr).is_some())
                }) {
                    return ColumnSet::new();
                }
                union
                    .output_indexes
                    .iter()
                    .zip(outputs)
                    .filter(|(output, _)| required.contains(*output))
                    .filter_map(|(_, (input, coercion))| match coercion {
                        Some(expr) => input_column(expr),
                        None => Some(*input),
                    })
                    .collect()
            }))
        }
        RelOperator::Sort(sort) if sort.preserves_rows() => RewriteFrame::Unary(required.clone()),
        RelOperator::Window(_) | RelOperator::WindowGroup(_)
            if !required.is_empty() && can_drop_window_partitions(expr.plan()) =>
        {
            match window_filter_columns(expr)? {
                Some(allowed) => {
                    RewriteFrame::Unary(required.intersection(&allowed).copied().collect())
                }
                None => RewriteFrame::Barrier,
            }
        }
        RelOperator::Aggregate(aggregate)
            if !required.is_empty() && can_drop_aggregate_groups(aggregate) =>
        {
            // Drop entire ordinary groups, never rows from a group that is kept.
            let child = expr.unary_child().derive_relational_prop()?;
            let allowed = aggregate.child_group_columns(&child.output_columns);
            RewriteFrame::Unary(required.intersection(&allowed).copied().collect())
        }
        RelOperator::Join(join) if supported_join(join) => {
            let mut child_required = [ColumnSet::new(), ColumnSet::new()];
            match join_requirements(expr, join, required, &mut child_required)? {
                Some(join_type) => RewriteFrame::RewriteJoin {
                    child_required,
                    join_type,
                },
                None => RewriteFrame::Binary(child_required),
            }
        }
        // In particular, LIMIT/TopN and row-dependent expressions are barriers.
        _ => RewriteFrame::Barrier,
    })
}

fn can_drop_aggregate_groups(aggregate: &Aggregate) -> bool {
    aggregate.grouping_sets.is_none()
        && aggregate.rank_limit.is_none()
        && aggregate.adaptive_topk_count.is_none()
        && aggregate.group_items.iter().all(|item| is_simple_value(&item.scalar))
        // Keep arbitrary aggregate evaluation (including possible errors) on
        // the original rows. Plain COUNT and grouping without functions are safe.
        && aggregate.aggregate_functions.iter().all(|item| {
            matches!(&item.scalar, ScalarExpr::AggregateFunction(agg)
                if agg.func_name == "count" && !agg.distinct && agg.sort_descs.is_empty()
                    && agg.args.iter().all(is_simple_value))
        })
}

fn can_drop_window_partitions(plan: &RelOperator) -> bool {
    fn simple_ranking(window: &Window) -> bool {
        window.limit.is_none()
            && window.top.is_none()
            && window.arguments.is_empty()
            && matches!(
                window.function,
                WindowFuncType::RowNumber | WindowFuncType::Rank | WindowFuncType::DenseRank
            )
            && window
                .partition_by
                .iter()
                .all(|item| is_simple_value(&item.scalar))
            && window
                .order_by
                .iter()
                .all(|order| is_simple_value(&order.order_by_item.scalar))
    }

    // The column check alone doesn't let us suppress evaluation of arbitrary
    // window functions or scalars evaluated by the WindowGroup itself.
    match plan {
        RelOperator::Window(window) => simple_ranking(window),
        RelOperator::WindowGroup(group) => {
            group.windows.iter().all(simple_ranking)
                && group
                    .scalar_items
                    .iter()
                    .all(|item| is_simple_value(&item.scalar))
        }
        _ => false,
    }
}

fn supported_join(join: &Join) -> bool {
    matches!(
        join.join_type,
        JoinType::Inner
            | JoinType::Cross
            | JoinType::Left
            | JoinType::Right
            | JoinType::Full
            | JoinType::LeftSemi
            | JoinType::RightSemi
    ) && !join.is_lateral
        && join.single_to_inner.is_none()
        && join.build_side_cache_info.is_none()
        && join.spatial_join.is_none()
        && join.runtime_filters.is_empty()
        && join.non_equi_conditions.is_empty()
        && join.equi_conditions.iter().all(|condition| {
            input_column(&condition.left).is_some()
                && input_column(&condition.right).is_some()
                && same_comparison_type(&condition.left, &condition.right)
        })
}

fn join_requirements(
    expr: &SExpr,
    join: &Join,
    required: &ColumnSet,
    children: &mut [ColumnSet; 2],
) -> Result<Option<JoinType>> {
    let [left, right] = children;
    if !required.is_empty() {
        let left_prop = expr.left_child().derive_relational_prop()?;
        let right_prop = expr.right_child().derive_relational_prop()?;
        let left_columns = &left_prop.output_columns;
        let right_columns = &right_prop.output_columns;
        for column in required {
            match (
                left_columns.contains(column),
                right_columns.contains(column),
            ) {
                (true, false) => _ = left.insert(*column),
                (false, true) => _ = right.insert(*column),
                _ => {}
            }
        }
    }

    let join_type = eliminate_outer_join_type(join.join_type, !left.is_empty(), !right.is_empty());
    // Translate inherited requirements only through preserved inputs, or
    // through both inputs when only matched rows are returned. For a FULL
    // join that stays full, there is no side to constrain.
    if matches!(
        join_type,
        JoinType::Right | JoinType::Full | JoinType::RightSemi
    ) {
        left.clear();
    }
    if matches!(
        join_type,
        JoinType::Left | JoinType::Full | JoinType::LeftSemi
    ) {
        right.clear();
    }

    // Use the NEW join type, so a conversion can expose facts for deeper
    // joins in the same traversal. ON only constrains matching on an outer
    // join's non-preserved side, not its preserved side.
    for condition in &join.equi_conditions {
        if condition.is_null_equal {
            continue;
        }
        if matches!(
            join_type,
            JoinType::Inner | JoinType::Right | JoinType::LeftSemi | JoinType::RightSemi
        ) {
            left.extend(input_column(&condition.left));
        }
        if matches!(
            join_type,
            JoinType::Inner | JoinType::Left | JoinType::LeftSemi | JoinType::RightSemi
        ) {
            right.extend(input_column(&condition.right));
        }
    }
    Ok((join_type != join.join_type).then_some(join_type))
}

fn filter_requirements(filter: &Filter) -> Option<ColumnSet> {
    let mut columns = ColumnSet::new();
    // Do not move the row removal across an unproven conjunct, even when
    // another conjunct alone would reject NULLs. That can suppress an error
    // or change a row-dependent result before the original filter runs.
    for predicate in filter.predicates.iter().flat_map(conjunctions) {
        add_filter_requirements(predicate, &mut columns)?;
    }
    Some(columns)
}

#[recursive::recursive]
fn add_filter_requirements(predicate: &ScalarExpr, columns: &mut ColumnSet) -> Option<()> {
    let ScalarExpr::FunctionCall(call) = predicate else {
        return is_simple_value(predicate).then_some(());
    };
    match (call.func_name.as_str(), call.arguments.as_slice()) {
        ("is_true", [arg]) => add_filter_requirements(arg, columns),
        ("is_not_null", [arg]) if is_simple_value(arg) => {
            columns.extend(input_column(arg));
            Some(())
        }
        ("eq" | "noteq" | "lt" | "lte" | "gt" | "gte", [left, right])
            if is_simple_value(left)
                && is_simple_value(right)
                && same_comparison_type(left, right) =>
        {
            columns.extend(input_column(left));
            columns.extend(input_column(right));
            Some(())
        }
        _ => None,
    }
}

fn same_comparison_type(left: &ScalarExpr, right: &ScalarExpr) -> bool {
    let left = left.data_type();
    let right = right.data_type();
    let ty = left.remove_nullable();
    ty == right.remove_nullable()
        && matches!(
            ty,
            DataType::Boolean
                | DataType::Number(_)
                | DataType::String
                | DataType::Date
                | DataType::Timestamp
        )
}

fn is_simple_value(expr: &ScalarExpr) -> bool {
    matches!(
        expr,
        ScalarExpr::ConstantExpr(_) | ScalarExpr::TypedConstantExpr(_, _)
    ) || input_column(expr).is_some()
}

fn input_column(expr: &ScalarExpr) -> Option<Symbol> {
    match expr {
        ScalarExpr::BoundColumnRef(column) => Some(column.column.index),
        ScalarExpr::CastExpr(cast)
            if !cast.is_try
                && cast.target_type.is_nullable()
                && cast.target_type.remove_nullable()
                    == cast.argument.data_type().remove_nullable() =>
        {
            match cast.argument.as_ref() {
                ScalarExpr::BoundColumnRef(column) => Some(column.column.index),
                _ => None,
            }
        }
        _ => None,
    }
}
