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

use crate::optimizer::extract::Matcher;
use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RelExpr;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::Filter;
use crate::plans::FunctionCall;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::ScalarExpr;

const NULL_THRESHOLD: u64 = 1024;
pub struct RuleFilterNulls {
    id: RuleID,
    matchers: Vec<Matcher>,
}

impl RuleFilterNulls {
    pub fn new() -> Self {
        Self {
            id: RuleID::FilterNulls,
            // Join
            // /  \
            //... ...
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Join,
                children: vec![Matcher::Leaf, Matcher::Leaf],
            }],
        }
    }
}

impl Rule for RuleFilterNulls {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let join: Join = s_expr.plan().clone().try_into()?;
        if join.join_type != JoinType::Inner {
            state.add_result(s_expr.clone());
            return Ok(());
        }
        let mut left_child = s_expr.child(0)?.clone();
        let mut right_child = s_expr.child(1)?.clone();

        let left_stat = RelExpr::with_s_expr(&left_child).derive_cardinality()?;
        let right_stat = RelExpr::with_s_expr(&right_child).derive_cardinality()?;
        let mut left_null_predicates = vec![];
        let mut right_null_predicates = vec![];
        for join_key in join.equi_conditions.iter() {
            let left_key = &join_key.left;
            let right_key = &join_key.right;
            if left_key.has_one_column_ref() {
                if let Some(left_key_stat) = left_stat
                    .statistics
                    .column_stats
                    .get(left_key.used_columns().iter().next().unwrap())
                {
                    if left_key_stat.null_count >= NULL_THRESHOLD {
                        left_null_predicates.push(join_key_null_filter(left_key));
                    }
                }
            }
            if right_key.has_one_column_ref() {
                if let Some(right_key_stat) = right_stat
                    .statistics
                    .column_stats
                    .get(right_key.used_columns().iter().next().unwrap())
                {
                    if right_key_stat.null_count >= NULL_THRESHOLD {
                        right_null_predicates.push(join_key_null_filter(right_key));
                    }
                }
            }
        }
        if !left_null_predicates.is_empty() {
            let left_null_filter = Filter {
                predicates: left_null_predicates,
            };
            left_child = SExpr::create_unary(
                Arc::new(RelOperator::Filter(left_null_filter)),
                Arc::new(left_child.clone()),
            );
        }

        if !right_null_predicates.is_empty() {
            let right_null_filter = Filter {
                predicates: right_null_predicates,
            };
            right_child = SExpr::create_unary(
                Arc::new(RelOperator::Filter(right_null_filter)),
                Arc::new(right_child.clone()),
            );
        }

        let mut res = s_expr.replace_children(vec![Arc::new(left_child), Arc::new(right_child)]);
        res.set_applied_rule(&self.id());
        state.add_result(res);

        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

fn join_key_null_filter(key: &ScalarExpr) -> ScalarExpr {
    // Construct the null filter, `xxx is not NULL`
    ScalarExpr::FunctionCall(FunctionCall {
        span: None,
        func_name: "is_not_null".to_string(),
        params: vec![],
        arguments: vec![key.clone()],
    })
}
