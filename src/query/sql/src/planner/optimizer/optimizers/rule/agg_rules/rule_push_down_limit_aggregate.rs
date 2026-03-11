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
use crate::plans::Aggregate;
use crate::plans::Limit;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::Sort;
use crate::plans::SortItem;

/// Input:  Limit | Sort
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
}

impl RulePushDownRankLimitAggregate {
    pub fn new(max_limit: usize) -> Self {
        Self {
            matchers: vec![
                match_op!(Limit -> Aggregate -> *),
                match_op!(Sort -> Aggregate -> *),
                match_op!(Sort -> EvalScalar -> Aggregate -> *),
            ],
            max_limit,
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

        let sort_items = agg_limit
            .group_items
            .iter()
            .map(|g| SortItem {
                index: g.index,
                asc: true,
                nulls_first: false,
            })
            .collect::<Vec<_>>();
        agg_limit.rank_limit = Some((sort_items, count));

        let mut sort_items = Vec::new();
        for item in &agg_limit.group_items {
            match item.scalar.data_type()?.remove_nullable() {
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
                | DataType::Variant => {}
                _ => continue,
            }

            sort_items.push(SortItem {
                index: item.index,
                asc: true,
                nulls_first: false,
            });
        }

        let agg = agg.unary_child_arc().ref_build_unary(agg_limit);
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
        let sort: Sort = s_expr.plan().clone().try_into()?;
        let mut has_eval_scalar = false;
        let agg_limit_expr = match s_expr.child(0)?.plan().rel_op() {
            RelOp::Aggregate => s_expr.child(0)?,
            RelOp::EvalScalar => {
                has_eval_scalar = true;
                s_expr.child(0)?.child(0)?
            }
            _ => return Ok(()),
        };

        let Some(limit) = sort.limit else {
            return Ok(());
        };

        let mut agg_limit: Aggregate = agg_limit_expr.plan().clone().try_into()?;

        let is_order_subset = sort
            .items
            .iter()
            .all(|k| agg_limit.group_items.iter().any(|g| g.index == k.index));
        if !is_order_subset {
            return Ok(());
        }

        let mut sort_items = Vec::with_capacity(agg_limit.group_items.len());
        let mut not_found_sort_items = vec![];
        for i in 0..agg_limit.group_items.len() {
            let group_item = &agg_limit.group_items[i];
            if let Some(sort_item) = sort.items.iter().find(|k| k.index == group_item.index) {
                sort_items.push(SortItem {
                    index: group_item.index,
                    asc: sort_item.asc,
                    nulls_first: sort_item.nulls_first,
                });
            } else {
                not_found_sort_items.push(SortItem {
                    index: group_item.index,
                    asc: true,
                    nulls_first: false,
                });
            }
        }
        sort_items.extend(not_found_sort_items);

        agg_limit.rank_limit = Some((sort_items, limit));

        let agg = agg_limit_expr.unary_child_arc().ref_build_unary(agg_limit);
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
            1 | 2 => self.apply_sort(s_expr, state),
            _ => unreachable!(),
        }
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}
