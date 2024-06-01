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

use crate::binder::JoinPredicate;
use crate::optimizer::extract::Matcher;
use crate::optimizer::filter::InferFilterOptimizer;
use crate::optimizer::filter::JoinProperty;
use crate::optimizer::rule::constant::false_constant;
use crate::optimizer::rule::constant::is_falsy;
use crate::optimizer::rule::rewrite::push_down_filter_join::can_filter_null;
use crate::optimizer::rule::rewrite::push_down_filter_join::convert_mark_to_semi_join;
use crate::optimizer::rule::rewrite::push_down_filter_join::outer_join_to_inner_join;
use crate::optimizer::rule::rewrite::push_down_filter_join::rewrite_predicates;
use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RelExpr;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::ComparisonOp;
use crate::plans::Filter;
use crate::plans::FunctionCall;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;
use crate::MetadataRef;

pub struct RulePushDownFilterJoin {
    id: RuleID,
    matchers: Vec<Matcher>,
    metadata: MetadataRef,
}

impl RulePushDownFilterJoin {
    pub fn new(metadata: MetadataRef) -> Self {
        Self {
            id: RuleID::PushDownFilterJoin,
            // Filter
            //  \
            //   Join
            //   | \
            //   |  *
            //   *
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Filter,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::Join,
                    children: vec![Matcher::Leaf, Matcher::Leaf],
                }],
            }],
            metadata,
        }
    }
}

impl Rule for RulePushDownFilterJoin {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        // First, try to convert outer join to inner join
        let (s_expr, outer_to_inner) = outer_join_to_inner_join(s_expr, self.metadata.clone())?;

        // Second, check if can convert mark join to semi join
        let (s_expr, mark_to_semi) = convert_mark_to_semi_join(&s_expr)?;
        if s_expr.plan().rel_op() != RelOp::Filter {
            state.add_result(s_expr);
            return Ok(());
        }
        let filter: Filter = s_expr.plan().clone().try_into()?;
        if filter.predicates.is_empty() {
            state.add_result(s_expr);
            return Ok(());
        }

        // Finally, push down filter to join.
        let (need_push, mut result) = try_push_down_filter_join(&s_expr, self.metadata.clone())?;
        if !need_push && !outer_to_inner && !mark_to_semi {
            return Ok(());
        }

        result.set_applied_rule(&self.id);
        state.add_result(result);

        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

pub fn try_push_down_filter_join(s_expr: &SExpr, metadata: MetadataRef) -> Result<(bool, SExpr)> {
    // Extract or predicates from Filter to push down them to join.
    // For example: `select * from t1, t2 where (t1.a=1 and t2.b=2) or (t1.a=2 and t2.b=1)`
    // The predicate will be rewritten to `((t1.a=1 and t2.b=2) or (t1.a=2 and t2.b=1)) and (t1.a=1 or t1.a=2) and (t2.b=2 or t2.b=1)`
    // So `(t1.a=1 or t1.a=1), (t2.b=2 or t2.b=1)` may be pushed down join and reduce rows between join
    let predicates = rewrite_predicates(s_expr)?;

    let join_expr = s_expr.child(0)?;
    let mut join: Join = join_expr.plan().clone().try_into()?;

    let rel_expr = RelExpr::with_s_expr(join_expr);
    let left_prop = rel_expr.derive_relational_prop_child(0)?;
    let right_prop = rel_expr.derive_relational_prop_child(1)?;

    let original_predicates_count = predicates.len();
    let mut original_predicates = vec![];
    let mut left_push_down = vec![];
    let mut right_push_down = vec![];
    let mut push_down_predicates = vec![];
    let mut non_equi_predicates = vec![];
    for predicate in predicates.into_iter() {
        if is_falsy(&predicate) {
            push_down_predicates = vec![false_constant()];
            break;
        }
        let pred = JoinPredicate::new(&predicate, &left_prop, &right_prop);
        match pred {
            JoinPredicate::ALL(_) => {
                push_down_predicates.push(predicate);
            }
            JoinPredicate::Left(_) => {
                if matches!(
                    join.join_type,
                    JoinType::Right | JoinType::RightSingle | JoinType::Full
                ) {
                    if can_filter_null(
                        &predicate,
                        &left_prop.output_columns,
                        &join.join_type,
                        metadata.clone(),
                    )? {
                        left_push_down.push(predicate);
                    } else {
                        original_predicates.push(predicate);
                    }
                } else {
                    left_push_down.push(predicate);
                }
            }
            JoinPredicate::Right(_) => {
                if matches!(
                    join.join_type,
                    JoinType::Left | JoinType::LeftSingle | JoinType::Full
                ) {
                    if can_filter_null(
                        &predicate,
                        &right_prop.output_columns,
                        &join.join_type,
                        metadata.clone(),
                    )? {
                        right_push_down.push(predicate);
                    } else {
                        original_predicates.push(predicate);
                    }
                } else {
                    right_push_down.push(predicate);
                }
            }
            JoinPredicate::Other(_) => original_predicates.push(predicate),
            JoinPredicate::Both { is_equal_op, .. } => {
                if matches!(join.join_type, JoinType::Inner | JoinType::Cross) {
                    if is_equal_op {
                        push_down_predicates.push(predicate);
                    } else {
                        non_equi_predicates.push(predicate);
                    }
                    join.join_type = JoinType::Inner;
                } else {
                    original_predicates.push(predicate);
                }
            }
        }
    }

    if original_predicates.len() == original_predicates_count {
        return Ok((false, s_expr.clone()));
    }

    if !matches!(join.join_type, JoinType::Full) {
        // Infer new predicate and push down filter.
        for (left_condition, right_condition) in join
            .left_conditions
            .iter()
            .zip(join.right_conditions.iter())
        {
            push_down_predicates.push(ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: String::from(ComparisonOp::Equal.to_func_name()),
                params: vec![],
                arguments: vec![left_condition.clone(), right_condition.clone()],
            }));
        }
        join.left_conditions.clear();
        join.right_conditions.clear();
        match join.join_type {
            JoinType::Left | JoinType::LeftSingle => {
                push_down_predicates.extend(left_push_down);
                left_push_down = vec![];
            }
            JoinType::Right | JoinType::RightSingle => {
                push_down_predicates.extend(right_push_down);
                right_push_down = vec![];
            }
            _ => {
                push_down_predicates.extend(left_push_down);
                left_push_down = vec![];
                push_down_predicates.extend(right_push_down);
                right_push_down = vec![];
            }
        }
        let join_prop = JoinProperty::new(&left_prop.output_columns, &right_prop.output_columns);
        let infer_filter = InferFilterOptimizer::new(Some(join_prop));
        push_down_predicates = infer_filter.run(push_down_predicates)?;
    }

    let mut all_push_down = vec![];
    for predicate in push_down_predicates.into_iter() {
        if is_falsy(&predicate) {
            left_push_down = vec![false_constant()];
            right_push_down = vec![false_constant()];
            break;
        }
        let pred = JoinPredicate::new(&predicate, &left_prop, &right_prop);
        match pred {
            JoinPredicate::ALL(_) => {
                all_push_down.push(predicate);
            }
            JoinPredicate::Left(_) => {
                left_push_down.push(predicate);
            }
            JoinPredicate::Right(_) => {
                right_push_down.push(predicate);
            }
            JoinPredicate::Both { left, right, .. } => {
                join.left_conditions.push(left.clone());
                join.right_conditions.push(right.clone());
            }
            _ => original_predicates.push(predicate),
        }
    }
    join.non_equi_conditions.extend(non_equi_predicates);
    if !all_push_down.is_empty() {
        left_push_down.extend(all_push_down.to_vec());
        right_push_down.extend(all_push_down);
    }

    let mut left_child = join_expr.child(0)?.clone();
    let mut right_child = join_expr.child(1)?.clone();

    if !left_push_down.is_empty() {
        left_child = SExpr::create_unary(
            Arc::new(
                Filter {
                    predicates: left_push_down,
                }
                .into(),
            ),
            Arc::new(left_child),
        );
    }

    if !right_push_down.is_empty() {
        right_child = SExpr::create_unary(
            Arc::new(
                Filter {
                    predicates: right_push_down,
                }
                .into(),
            ),
            Arc::new(right_child),
        );
    }

    let mut result = SExpr::create_binary(
        Arc::new(join.into()),
        Arc::new(left_child),
        Arc::new(right_child),
    );

    if !original_predicates.is_empty() {
        result = SExpr::create_unary(
            Arc::new(
                Filter {
                    predicates: original_predicates,
                }
                .into(),
            ),
            Arc::new(result),
        );
    }

    Ok((true, result))
}
