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

#[cfg(feature = "z3-prove")]
use std::sync::Arc;

use databend_common_exception::Result;

use crate::optimizer::SExpr;
use crate::plans::Join;

pub fn outer_to_inner(s_expr: &SExpr) -> Result<SExpr> {
    let join: Join = s_expr.child(0)?.plan().clone().try_into()?;
    let origin_join_type = join.join_type.clone();
    if !origin_join_type.is_outer_join() {
        return Ok(s_expr.clone());
    }

    #[cfg(feature = "z3-prove")]
    {
        use crate::optimizer::RelExpr;
        use crate::plans::Filter;
        use crate::plans::JoinType;

        let mut join = join;
        let filter: Filter = s_expr.plan().clone().try_into()?;
        let constraint_set = crate::optimizer::ConstraintSet::new(&filter.predicates);

        let join_expr = RelExpr::with_s_expr(s_expr.child(0)?);
        let left_columns = join_expr
            .derive_relational_prop_child(0)?
            .output_columns
            .clone();
        let right_columns = join_expr
            .derive_relational_prop_child(1)?
            .output_columns
            .clone();

        let eliminate_left_null = left_columns
            .iter()
            .any(|col| constraint_set.is_null_reject(col));
        let eliminate_right_null = right_columns
            .iter()
            .any(|col| constraint_set.is_null_reject(col));

        let new_join_type = match join.join_type {
            JoinType::Left => {
                if eliminate_right_null {
                    JoinType::Inner
                } else {
                    JoinType::Left
                }
            }
            JoinType::Right => {
                if eliminate_left_null {
                    JoinType::Inner
                } else {
                    JoinType::Right
                }
            }
            JoinType::Full => {
                if eliminate_left_null && eliminate_right_null {
                    JoinType::Inner
                } else if eliminate_left_null {
                    JoinType::Left
                } else if eliminate_right_null {
                    JoinType::Right
                } else {
                    JoinType::Full
                }
            }
            _ => unreachable!(),
        };

        join.join_type = new_join_type;
        Ok(SExpr::create_unary(
            Arc::new(filter.into()),
            Arc::new(SExpr::create_binary(
                Arc::new(join.into()),
                Arc::new(s_expr.child(0)?.child(0)?.clone()),
                Arc::new(s_expr.child(0)?.child(1)?.clone()),
            )),
        ))
    }

    #[cfg(not(feature = "z3-prove"))]
    Ok(s_expr.clone())
}
