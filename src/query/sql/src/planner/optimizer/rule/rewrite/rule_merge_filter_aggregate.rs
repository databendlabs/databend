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

use crate::optimizer::extract::Matcher;
use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::Aggregate;
use crate::plans::Filter;
use crate::plans::RelOp;

/// Input:   Aggregate
///           \
///            Filter
///             \
///              *
///
/// Output:
///       Aggregate with filter
///           \
///            *

/// Input:   Aggregate
///           \
///            EvalScalar
///             \
///              Filer
///
/// Output:
///       Aggregate with filter
///           \
///            EvalScalar

pub struct RuleMergeFilterAggregate {
    id: RuleID,
    matchers: Vec<Matcher>,
}

impl RuleMergeFilterAggregate {
    pub fn new() -> Self {
        Self {
            id: RuleID::MergeFilterAggregate,

            matchers: vec![
                Matcher::MatchOp {
                    op_type: RelOp::Aggregate,
                    children: vec![Matcher::MatchOp {
                        op_type: RelOp::Filter,
                        children: vec![Matcher::Leaf],
                    }],
                },
                Matcher::MatchOp {
                    op_type: RelOp::Aggregate,
                    children: vec![Matcher::MatchOp {
                        op_type: RelOp::EvalScalar,
                        children: vec![Matcher::MatchOp {
                            op_type: RelOp::Filter,
                            children: vec![Matcher::Leaf],
                        }],
                    }],
                },
            ],
        }
    }
}

impl Rule for RuleMergeFilterAggregate {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(
        &self,
        s_expr: &SExpr,
        state: &mut TransformResult,
    ) -> databend_common_exception::Result<()> {
        let mut aggregate: Aggregate = s_expr.plan().clone().try_into()?;
        // if !aggregate.group_items.is_empty() {
        //     return Ok(());
        // }

        let child = s_expr.child(0)?;
        let result = match child.plan.as_ref() {
            crate::plans::RelOperator::EvalScalar(eval_scalar) => {
                let filter: Filter = child.child(0)?.plan().clone().try_into()?;
                aggregate.pushdown_filter = filter.predicates;

                let eval_scalar = SExpr::create_unary(
                    Arc::new(eval_scalar.clone().into()),
                    Arc::new(child.child(0)?.child(0)?.clone()),
                );

                SExpr::create_unary(Arc::new(aggregate.into()), Arc::new(eval_scalar))
            }
            crate::plans::RelOperator::Filter(filter) => {
                aggregate.pushdown_filter = filter.predicates.clone();

                SExpr::create_unary(
                    Arc::new(aggregate.into()),
                    Arc::new(child.child(0)?.clone()),
                )
            }
            _ => return Ok(()),
        };

        state.add_result(result);
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}
