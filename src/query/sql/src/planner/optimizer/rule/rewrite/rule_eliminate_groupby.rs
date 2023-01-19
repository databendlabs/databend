// Copyright 2023 Datafuse Labs.
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

use std::collections::HashSet;

use common_exception::Result;

use crate::optimizer::rule::Rule;
use crate::optimizer::rule::RuleID;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::SExpr;
use crate::plans::Aggregate;
use crate::plans::AggregateMode;
use crate::plans::BoundColumnRef;
use crate::plans::FunctionCall;
use crate::plans::PatternPlan;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;

pub struct RuleEliminateGroupBy {
    id: RuleID,
    pattern: SExpr,
}

impl RuleEliminateGroupBy {
    pub fn new() -> Self {
        Self {
            id: RuleID::EliminateGroupBy,
            // Aggregate
            //  \
            //   *
            pattern: SExpr::create_unary(
                PatternPlan {
                    plan_type: RelOp::Aggregate,
                }
                .into(),
                SExpr::create_leaf(
                    PatternPlan {
                        plan_type: RelOp::Pattern,
                    }
                    .into(),
                ),
            ),
        }
    }
}

impl Rule for RuleEliminateGroupBy {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let mut agg: Aggregate = s_expr.plan().clone().try_into()?;

        if agg.mode == AggregateMode::Final || agg.mode == AggregateMode::Partial {
            return Ok(());
        }

        let mut all_fn_items = vec![];
        let mut group_item_cols: HashSet<BoundColumnRef> = HashSet::new();
        for (idx, group_item) in agg.group_items.iter().enumerate() {
            match group_item.scalar.clone() {
                ScalarExpr::FunctionCall(FunctionCall { arguments, .. }) => {
                    let mut args = vec![];
                    for argument in arguments {
                        match argument {
                            ScalarExpr::BoundColumnRef(col) => args.push(col),
                            _ => return Ok(()),
                        }
                    }
                    all_fn_items.push((idx, args));
                }
                ScalarExpr::BoundColumnRef(col) => {
                    group_item_cols.insert(col);
                }
                _ => return Ok(()),
            }
        }

        if all_fn_items.is_empty() {
            return Ok(());
        }
        let group_col_nums = group_item_cols.len();
        if all_fn_items
            .iter()
            .all(|item| item.1.len() <= group_col_nums)
        {
            let mut need_remove = false;
            for item in all_fn_items {
                if item.1.iter().all(|col| group_item_cols.contains(col)) {
                    need_remove = true;
                    agg.group_items.remove(item.0);
                }
            }
            if need_remove {
                state.add_result(SExpr::create_unary(agg.into(), s_expr.child(0)?.clone()));
            } else {
                return Ok(());
            }
        }

        Ok(())
    }

    fn pattern(&self) -> &SExpr {
        &self.pattern
    }
}
