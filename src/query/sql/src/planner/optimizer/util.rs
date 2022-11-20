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

use common_datavalues::type_coercion::compare_coercion;
use common_exception::Result;

use super::SExpr;
use crate::optimizer::RelExpr;
use crate::planner::binder::wrap_cast;
use crate::planner::binder::JoinPredicate;
use crate::plans::Filter;
use crate::plans::JoinType;
use crate::plans::LogicalJoin;
use crate::plans::RelOperator;
use crate::plans::Scalar;
use crate::MetadataRef;
use crate::ScalarExpr;

/// Check if a query will read data from local tables(e.g. system tables).
pub fn contains_local_table_scan(s_expr: &SExpr, metadata: &MetadataRef) -> bool {
    s_expr
        .children()
        .iter()
        .any(|s_expr| contains_local_table_scan(s_expr, metadata))
        || if let RelOperator::LogicalGet(get) = s_expr.plan() {
            metadata.read().table(get.table_index).table().is_local()
        } else {
            false
        }
}

pub fn try_push_down_filter_join(s_expr: &SExpr, predicates: Vec<Scalar>) -> Result<(bool, SExpr)> {
    if predicates.is_empty() {
        return Ok((false, s_expr.clone()));
    }
    let join_expr = s_expr.child(0)?;
    let mut join: LogicalJoin = join_expr.plan().clone().try_into()?;

    let rel_expr = RelExpr::with_s_expr(join_expr);
    let left_prop = rel_expr.derive_relational_prop_child(0)?;
    let right_prop = rel_expr.derive_relational_prop_child(1)?;

    let mut left_push_down = vec![];
    let mut right_push_down = vec![];
    let mut original_predicates = vec![];

    let mut need_push = false;

    for predicate in predicates.into_iter() {
        let pred = JoinPredicate::new(&predicate, &left_prop, &right_prop);
        match pred {
            JoinPredicate::Left(_) => {
                if matches!(join.join_type, JoinType::Right) {
                    original_predicates.push(predicate);
                    continue;
                }
                need_push = true;
                left_push_down.push(predicate);
            }
            JoinPredicate::Right(_) => {
                if matches!(join.join_type, JoinType::Left) {
                    original_predicates.push(predicate);
                    continue;
                }
                need_push = true;
                right_push_down.push(predicate);
            }
            JoinPredicate::Other(_) => original_predicates.push(predicate),

            JoinPredicate::Both { left, right } => {
                let left_type = left.data_type();
                let right_type = right.data_type();
                let join_key_type = compare_coercion(&left_type, &right_type);

                // We have to check if left_type and right_type can be coerced to
                // a super type. If the coercion is failed, we cannot push the
                // predicate into join.
                if let Ok(join_key_type) = join_key_type {
                    if join.join_type == JoinType::Cross {
                        join.join_type = JoinType::Inner;
                    }
                    if left.data_type().ne(&right.data_type()) {
                        let left = wrap_cast(left.clone(), &join_key_type);
                        let right = wrap_cast(right.clone(), &join_key_type);
                        join.left_conditions.push(left);
                        join.right_conditions.push(right);
                    } else {
                        join.left_conditions.push(left.clone());
                        join.right_conditions.push(right.clone());
                    }
                    need_push = true;
                } else {
                    original_predicates.push(predicate);
                }
            }
        }
    }

    if !need_push {
        return Ok((false, s_expr.clone()));
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
    Ok((need_push, result))
}
