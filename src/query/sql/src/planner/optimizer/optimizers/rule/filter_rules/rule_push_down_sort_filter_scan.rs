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

use crate::optimizer::ir::Matcher;
use crate::optimizer::ir::SExpr;
use crate::optimizer::optimizers::rule::Rule;
use crate::optimizer::optimizers::rule::RuleID;
use crate::optimizer::optimizers::rule::TransformResult;
use crate::plans::Filter;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::plans::Scan;
use crate::plans::Sort;

/// Input:
/// (1)    Sort
///          \
///          Filter
///            \
///            Scan
/// (2)    Sort
///          \
///          EvalScalar
///            \
///            Filter
///              \
///              Scan
///
/// Output:
/// (1)    Sort
///          \
///          Filter
///            \
///            Scan(padding order_by and limit)
/// (2)    Sort
///          \
///          EvalScalar
///            \
///            Filter
///              \
///              Scan(padding order_by and limit)
pub struct RulePushDownSortFilterScan {
    id: RuleID,
    matchers: Vec<Matcher>,
}

impl RulePushDownSortFilterScan {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownSortFilterScan,
            matchers: vec![
                Matcher::MatchOp {
                    op_type: RelOp::Sort,
                    children: vec![Matcher::MatchOp {
                        op_type: RelOp::Filter,
                        children: vec![Matcher::MatchOp {
                            op_type: RelOp::Scan,
                            children: vec![],
                        }],
                    }],
                },
                Matcher::MatchOp {
                    op_type: RelOp::Sort,
                    children: vec![Matcher::MatchOp {
                        op_type: RelOp::EvalScalar,
                        children: vec![Matcher::MatchOp {
                            op_type: RelOp::Filter,
                            children: vec![Matcher::MatchOp {
                                op_type: RelOp::Scan,
                                children: vec![],
                            }],
                        }],
                    }],
                },
            ],
        }
    }
}

impl Rule for RulePushDownSortFilterScan {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let sort: Sort = s_expr.plan().clone().try_into()?;
        let child = s_expr.child(0)?;
        let (eval_scalar, filter, mut scan) = match child.plan() {
            RelOperator::Filter(filter) => {
                let grand_child = child.child(0)?;
                let scan: Scan = grand_child.plan().clone().try_into()?;
                (None, filter.clone(), scan)
            }
            RelOperator::EvalScalar(eval_scalar) => {
                let child = child.child(0)?;
                let filter: Filter = child.plan().clone().try_into()?;
                let grand_child = child.child(0)?;
                let scan: Scan = grand_child.plan().clone().try_into()?;
                (Some(eval_scalar.clone()), filter, scan)
            }
            _ => unreachable!(),
        };

        // The following conditions must be met push down filter and sort for vector index:
        // 1. Scan must contain `vector_index`, because .
        // 2. The number of `push_down_predicates` in Scan must be the same as the number of `predicates`
        //    in Filter to ensure that all filter conditions are pushed down.
        //    (Filter `predicates` has been pushed down in `RulePushDownFilterScan` rule.)
        // 3. Sort must have limit in order to prune unused blocks.
        let push_down_predicates = scan.push_down_predicates.clone().unwrap_or_default();
        if scan.vector_index.is_none()
            || push_down_predicates.len() != filter.predicates.len()
            || sort.limit.is_none()
        {
            return Ok(());
        }

        scan.order_by = Some(sort.items);
        scan.limit = sort.limit;

        let new_scan = SExpr::create_leaf(Arc::new(RelOperator::Scan(scan)));

        let mut result = if eval_scalar.is_some() {
            let grandchild = child.child(0)?;
            let new_filter = grandchild.replace_children(vec![Arc::new(new_scan)]);
            let new_eval_scalar = child.replace_children(vec![Arc::new(new_filter)]);
            s_expr.replace_children(vec![Arc::new(new_eval_scalar)])
        } else {
            let new_filter = child.replace_children(vec![Arc::new(new_scan)]);
            s_expr.replace_children(vec![Arc::new(new_filter)])
        };

        result.set_applied_rule(&self.id);
        state.add_result(result);
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

impl Default for RulePushDownSortFilterScan {
    fn default() -> Self {
        Self::new()
    }
}
