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
use databend_common_expression::types::DataType;

use crate::match_op;
use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::optimizer::optimizers::rule::scan_order::push_down_scan_order;
use crate::plans::Aggregate;
use crate::plans::Limit;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::plans::ScalarItem;
use crate::plans::Sort;
use crate::plans::SortItem;
use crate::plans::TopN;

/// Input:  Limit | Sort | TopN
///           \
///          Aggregate
///             \
///              *
///
/// Output: Limit | Sort
///           \
///          Aggregate(padding limit | rank_limit)
///             \
///               *
pub struct RulePushDownRankLimitAggregate {
    matchers: Vec<Matcher>,
    max_limit: usize,
    // Whether ordering by aggregate results also pushes the limit down, see `apply_sort`.
    enable_final_top_n: bool,
}

impl RulePushDownRankLimitAggregate {
    pub fn new(max_limit: usize, enable_final_top_n: bool) -> Self {
        Self {
            matchers: vec![
                match_op!(Limit -> Aggregate -> *),
                match_op!(Sort -> Aggregate -> *),
                match_op!(Sort -> EvalScalar -> Aggregate -> *),
                match_op!(TopN -> Aggregate -> *),
                match_op!(TopN -> EvalScalar -> Aggregate -> *),
            ],
            max_limit,
            enable_final_top_n,
        }
    }

    // There is no order by, so we don't care the order of result.
    // To make query works with consistent result and more efficient, we will inject a order by before limit
    fn apply_limit(
        &self,
        s_expr: &SExpr,
        state: &mut TransformResult,
    ) -> databend_common_exception::Result<()> {
        let limit: Limit = s_expr.plan().clone().try_into()?;
        let Some(mut count) = limit.limit else {
            return Ok(());
        };
        count += limit.offset;
        if count > self.max_limit {
            return Ok(());
        }
        let agg = s_expr.unary_child();
        let mut agg_limit: Aggregate = agg.plan().clone().try_into()?;

        // The rank limit drops rows of the groups that rank after `count`, so only the first
        // `count` groups by all the group keys are whole. With aggregate functions, the sort
        // injected below must pick exactly those, which it cannot do with a key it cannot
        // sort by. Without any, a group that lost rows is still the same group.
        let sortable = |item: &ScalarItem| is_sortable(&item.scalar.data_type());
        if !agg_limit.aggregate_functions.is_empty() && !agg_limit.group_items.iter().all(sortable)
        {
            return Ok(());
        }

        let sort_item = |item: &ScalarItem| SortItem {
            index: item.index,
            asc: true,
            nulls_first: false,
        };
        let rank_items = agg_limit
            .group_items
            .iter()
            .map(sort_item)
            .collect::<Vec<_>>();
        let sort_items = agg_limit
            .group_items
            .iter()
            .filter(|item| sortable(item))
            .map(sort_item)
            .collect::<Vec<_>>();
        // The scans below can prune by the runtime rank-limit boundary on the first key.
        let aggregate_input = push_down_scan_order(agg.unary_child(), &rank_items)
            .map_or_else(|| agg.unary_child_arc(), Arc::new);
        agg_limit.rank_limit = Some((rank_items, count));

        let agg = aggregate_input.ref_build_unary(agg_limit);
        let mut result = if sort_items.is_empty() {
            s_expr.replace_children(vec![Arc::new(agg)])
        } else {
            s_expr.replace_children(vec![Arc::new(agg.build_unary(Sort {
                items: sort_items,
                limit: Some(count),
                after_exchange: None,
                pre_projection: None,
                window_partition: None,
            }))])
        };
        result.set_applied_rule(&self.id());

        state.add_result(result);
        Ok(())
    }

    fn apply_sort(
        &self,
        s_expr: &SExpr,
        state: &mut TransformResult,
    ) -> databend_common_exception::Result<()> {
        let (order_items, order_limit): (Vec<SortItem>, Option<usize>) = match s_expr.plan() {
            RelOperator::Sort(sort) => (sort.items.clone(), sort.limit),
            RelOperator::TopN(top_n) => {
                let top_n: TopN = top_n.clone();
                (top_n.items.clone(), Some(top_n.candidate_count()))
            }
            _ => return Ok(()),
        };
        let mut has_eval_scalar = false;
        let agg_limit_expr = match s_expr.child(0)?.plan().rel_op() {
            RelOp::Aggregate => s_expr.child(0)?,
            RelOp::EvalScalar => {
                has_eval_scalar = true;
                s_expr.child(0)?.child(0)?
            }
            _ => return Ok(()),
        };

        let Some(limit) = order_limit else {
            return Ok(());
        };

        let mut agg_limit: Aggregate = agg_limit_expr.plan().clone().try_into()?;

        let is_order_subset = order_items
            .iter()
            .all(|k| agg_limit.group_items.iter().any(|g| g.index == k.index));
        // Ordered by aggregate results too: only the final aggregation can rank the groups, and
        // it keeps the first `limit` rows while producing them, so `limit` must be small.
        let is_order_output = self.enable_final_top_n
            && limit <= self.max_limit
            && !agg_limit.group_items.is_empty()
            && agg_limit.grouping_sets.is_none()
            && order_items.iter().all(|k| {
                agg_limit.group_items.iter().any(|g| g.index == k.index)
                    || agg_limit
                        .aggregate_functions
                        .iter()
                        .any(|f| f.index == k.index)
            });
        if !is_order_subset && !is_order_output {
            return Ok(());
        }

        // The rank limit must follow the ORDER BY key order, then append the remaining
        // group keys so that each rank identifies exactly one group. A repeated ORDER BY
        // column is redundant after its first occurrence, so keep only the first one.
        let mut sort_items: Vec<SortItem> = Vec::with_capacity(agg_limit.group_items.len());
        for order_item in &order_items {
            if !sort_items.iter().any(|k| k.index == order_item.index) {
                sort_items.push(order_item.clone());
            }
        }
        let ordered = sort_items.len();
        // Ordered by group keys only, the rank limit drops rows of the groups that rank after
        // `limit` before they are aggregated, so only the first `limit` groups by all the rank
        // keys are whole. With aggregate functions, the operator above must pick exactly those:
        // it breaks the ties of the ORDER BY keys by the group keys appended to the rank limit.
        // Without any, a group that lost rows is still the same group.
        let break_ties = is_order_subset && !agg_limit.aggregate_functions.is_empty();
        for group_item in &agg_limit.group_items {
            if sort_items.iter().any(|k| k.index == group_item.index) {
                continue;
            }
            if break_ties && !is_sortable(&group_item.scalar.data_type()) {
                return Ok(());
            }
            sort_items.push(SortItem {
                index: group_item.index,
                asc: true,
                nulls_first: false,
            });
        }

        let mut order_plan = s_expr.plan().clone();
        if break_ties {
            let tie_breakers = sort_items[ordered..].iter().cloned();
            match &mut order_plan {
                RelOperator::Sort(sort) => sort.items.extend(tie_breakers),
                RelOperator::TopN(top_n) => top_n.items.extend(tie_breakers),
                _ => {}
            }
        }

        agg_limit.rank_limit = Some((sort_items.clone(), limit));

        // The group keys also let the scans below, including every branch of a
        // UNION ALL, prune by the runtime rank-limit boundary on the first key.
        let aggregate_input = is_order_subset
            .then(|| push_down_scan_order(agg_limit_expr.unary_child(), &sort_items))
            .flatten()
            .map_or_else(|| agg_limit_expr.unary_child_arc(), Arc::new);
        let agg = aggregate_input.ref_build_unary(agg_limit);
        let s_expr = s_expr.replace_plan(order_plan);
        let mut result = if has_eval_scalar {
            let eval_scalar = s_expr.unary_child().replace_children(vec![Arc::new(agg)]);
            s_expr.replace_children(vec![Arc::new(eval_scalar)])
        } else {
            s_expr.replace_children(vec![Arc::new(agg)])
        };
        result.set_applied_rule(&self.id());
        state.add_result(result);
        Ok(())
    }
}

/// Whether the sort operator can sort by a key of `data_type`: the types its row format
/// supports.
fn is_sortable(data_type: &DataType) -> bool {
    matches!(
        data_type.remove_nullable(),
        DataType::Null
            | DataType::Boolean
            | DataType::Number(_)
            | DataType::Decimal(_)
            | DataType::Timestamp
            | DataType::TimestampTz
            | DataType::Interval
            | DataType::Date
            | DataType::Binary
            | DataType::String
            | DataType::Variant
    )
}

impl Rule for RulePushDownRankLimitAggregate {
    fn id(&self) -> RuleID {
        RuleID::PushDownRankLimitAggregate
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<(), ErrorCode> {
        let i = self
            .matchers
            .iter()
            .position(|matcher| matcher.matches(s_expr))
            .unwrap();
        self.apply_matcher(i, s_expr, state)
    }

    fn apply_matcher(
        &self,
        i: usize,
        s_expr: &SExpr,
        state: &mut TransformResult,
    ) -> Result<(), ErrorCode> {
        match i {
            0 => self.apply_limit(s_expr, state),
            1..=4 => self.apply_sort(s_expr, state),
            _ => unreachable!(),
        }
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}
