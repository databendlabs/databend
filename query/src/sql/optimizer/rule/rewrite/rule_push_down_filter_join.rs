// Copyright 2022 Datafuse Labs.
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

use common_datavalues::type_coercion::merge_types;
use common_exception::Result;

use crate::sql::binder::satisfied_by;
use crate::sql::binder::wrap_cast_if_needed;
use crate::sql::optimizer::rule::Rule;
use crate::sql::optimizer::rule::TransformState;
use crate::sql::optimizer::RelExpr;
use crate::sql::optimizer::RelationalProperty;
use crate::sql::optimizer::RuleID;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::ComparisonExpr;
use crate::sql::plans::ComparisonOp;
use crate::sql::plans::Filter;
use crate::sql::plans::JoinType;
use crate::sql::plans::LogicalInnerJoin;
use crate::sql::plans::PatternPlan;
use crate::sql::plans::RelOp;
use crate::sql::plans::Scalar;
use crate::sql::plans::ScalarExpr;

/// Predicate types to determine how to push down
/// a predicate.
/// Given a query: `SELECT * FROM t(a), t1(b) WHERE a = 1 AND b = 1 AND a = b AND a+b = 1`,
/// the predicate types are:
/// - Left: `a = 1`
/// - Right: `b = 1`
/// - Both: `a = b`
/// - Other: `a+b = 1`
enum Predicate<'a> {
    Left(&'a Scalar),
    Right(&'a Scalar),
    Both { left: &'a Scalar, right: &'a Scalar },
    Other(&'a Scalar),
}

impl<'a> Predicate<'a> {
    pub fn new(
        scalar: &'a Scalar,
        left_prop: &RelationalProperty,
        right_prop: &RelationalProperty,
    ) -> Self {
        if satisfied_by(scalar, left_prop) {
            return Self::Left(scalar);
        }

        if satisfied_by(scalar, right_prop) {
            return Self::Right(scalar);
        }

        if let Scalar::ComparisonExpr(ComparisonExpr {
            op: ComparisonOp::Equal,
            left,
            right,
            ..
        }) = scalar
        {
            if satisfied_by(left, left_prop) && satisfied_by(right, right_prop) {
                return Self::Both { left, right };
            }

            if satisfied_by(right, left_prop) && satisfied_by(left, right_prop) {
                return Self::Both {
                    left: right,
                    right: left,
                };
            }
        }

        Self::Other(scalar)
    }
}

pub struct RulePushDownFilterJoin {
    id: RuleID,
    pattern: SExpr,
}

impl RulePushDownFilterJoin {
    pub fn new() -> Self {
        Self {
            id: RuleID::PushDownFilterJoin,
            // Filter
            //  \
            //   InnerJoin
            //   | \
            //   |  *
            //   *
            pattern: SExpr::create_unary(
                PatternPlan {
                    plan_type: RelOp::Filter,
                }
                .into(),
                SExpr::create_binary(
                    PatternPlan {
                        plan_type: RelOp::LogicalInnerJoin,
                    }
                    .into(),
                    SExpr::create_leaf(
                        PatternPlan {
                            plan_type: RelOp::Pattern,
                        }
                        .into(),
                    ),
                    SExpr::create_leaf(
                        PatternPlan {
                            plan_type: RelOp::Pattern,
                        }
                        .into(),
                    ),
                ),
            ),
        }
    }
}

impl Rule for RulePushDownFilterJoin {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformState) -> Result<()> {
        let filter: Filter = s_expr.plan().clone().try_into()?;
        let join_expr = s_expr.child(0)?;
        let mut join: LogicalInnerJoin = join_expr.plan().clone().try_into()?;

        let rel_expr = RelExpr::with_s_expr(join_expr);
        let left_prop = rel_expr.derive_relational_prop_child(0)?;
        let right_prop = rel_expr.derive_relational_prop_child(1)?;

        let mut left_push_down = vec![];
        let mut right_push_down = vec![];
        let mut original_predicates = vec![];

        let mut need_push = false;

        for predicate in filter.predicates.into_iter() {
            let pred = Predicate::new(&predicate, &left_prop, &right_prop);
            match pred {
                Predicate::Left(_) => {
                    need_push = true;
                    left_push_down.push(predicate);
                }
                Predicate::Right(_) => {
                    need_push = true;
                    right_push_down.push(predicate);
                }
                Predicate::Other(_) => original_predicates.push(predicate),

                Predicate::Both { left, right } => {
                    let left_type = left.data_type();
                    let right_type = right.data_type();
                    let join_key_type = merge_types(&left_type, &right_type);

                    // We have to check if left_type and right_type can be coerced to
                    // a super type. If the coercion is failed, we cannot push the
                    // predicate into join.
                    if let Ok(join_key_type) = join_key_type {
                        if join.join_type == JoinType::Cross {
                            join.join_type = JoinType::Inner;
                        }
                        let left = wrap_cast_if_needed(left.clone(), &join_key_type);
                        let right = wrap_cast_if_needed(right.clone(), &join_key_type);
                        join.left_conditions.push(left);
                        join.right_conditions.push(right);
                        need_push = true;
                    } else {
                        original_predicates.push(predicate);
                    }
                }
            }
        }

        if !need_push {
            return Ok(());
        }

        let mut left_child = join_expr.child(0)?.clone();
        let mut right_child = join_expr.child(1)?.clone();

        if !left_push_down.is_empty() {
            left_child = SExpr::create_unary(
                Filter {
                    predicates: left_push_down,
                    is_having: false,
                }
                .into(),
                left_child,
            );
        }

        if !right_push_down.is_empty() {
            right_child = SExpr::create_unary(
                Filter {
                    predicates: right_push_down,
                    is_having: false,
                }
                .into(),
                right_child,
            );
        }

        let mut result = SExpr::create_binary(join.into(), left_child, right_child);

        if !original_predicates.is_empty() {
            result = SExpr::create_unary(
                Filter {
                    predicates: original_predicates,
                    is_having: false,
                }
                .into(),
                result,
            );
        }

        state.add_result(result);

        Ok(())
    }

    fn pattern(&self) -> &SExpr {
        &self.pattern
    }
}
