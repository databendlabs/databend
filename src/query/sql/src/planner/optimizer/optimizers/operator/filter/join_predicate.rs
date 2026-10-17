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

use crate::optimizer::ir::SExpr;
use crate::plans::Filter;
use crate::plans::Join;
use crate::plans::JoinEquiCondition;
use crate::plans::ScalarExpr;

/// Writes an already classified condition, without inference or relocation.
pub(crate) enum JoinCondition<'a> {
    Equi {
        left: &'a ScalarExpr,
        right: &'a ScalarExpr,
        is_null_equal: bool,
    },
    NonEqui(&'a ScalarExpr),
}

impl JoinCondition<'_> {
    /// Insert an existing condition once, preserving independent volatile
    /// occurrences and distinguishing ordinary from NULL-safe equality.
    /// The predicate_reorder SQL tests cover actual collisions: outer-to-inner
    /// conversion, skipped inference with NULL-safe keys, and DPhyp placement
    /// of equal residuals collected from different scopes.
    pub(crate) fn insert_into(self, join: &mut Join) {
        match self {
            Self::Equi {
                left,
                right,
                is_null_equal,
            } => {
                let exists = left.is_deterministic()
                    && right.is_deterministic()
                    && join.equi_conditions.iter().any(|condition| {
                        condition.is_null_equal == is_null_equal
                            && condition.left == *left
                            && condition.right == *right
                    });
                if !exists {
                    join.equi_conditions.push(JoinEquiCondition::new(
                        left.clone(),
                        right.clone(),
                        is_null_equal,
                    ));
                }
            }
            Self::NonEqui(predicate) => {
                if !predicate.is_deterministic() || !join.non_equi_conditions.contains(predicate) {
                    join.non_equi_conditions.push(predicate.clone());
                }
            }
        }
    }
}

/// Placement result. Deciding legality and deriving new predicates belong to
/// the caller; this only assembles the existing predicates into a plan.
#[derive(Default)]
pub(crate) struct JoinFilters {
    pub left: Vec<ScalarExpr>,
    pub right: Vec<ScalarExpr>,
    pub residual: Vec<ScalarExpr>,
}

impl JoinFilters {
    pub(crate) fn build(self, join: Join, left: SExpr, right: SExpr) -> SExpr {
        let expr = SExpr::create_binary(
            join,
            Self::wrap(left, self.left),
            Self::wrap(right, self.right),
        );
        Self::wrap(expr, self.residual)
    }

    pub(crate) fn wrap(expr: SExpr, predicates: Vec<ScalarExpr>) -> SExpr {
        if predicates.is_empty() {
            expr
        } else {
            expr.build_unary(Filter { predicates })
        }
    }
}
