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

use common_exception::Result;

use crate::binder::JoinPredicate;
use crate::optimizer::RelExpr;
use crate::optimizer::RelationalProperty;
use crate::optimizer::SExpr;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::ScalarExpr;

pub mod hash_join;
pub mod ie_join;

pub enum PhysicalJoinType {
    Hash,
    IEJoin,
    SortMerge,
}

// Choose physical join type by join conditions
pub fn physical_join(join: &Join, s_expr: &SExpr) -> Result<PhysicalJoinType> {
    if !join.left_conditions.is_empty() {
        // Contain equi condition, use hash join
        return Ok(PhysicalJoinType::Hash);
    }

    let left_prop = RelExpr::with_s_expr(s_expr.child(0)?).derive_relational_prop()?;
    let right_prop = RelExpr::with_s_expr(s_expr.child(1)?).derive_relational_prop()?;
    let mut ie_num = 0;
    let mut non_ie_num = 0;
    for condition in join.non_equi_conditions.iter() {
        check_non_equi_condition(
            condition,
            &left_prop,
            &right_prop,
            &mut ie_num,
            &mut non_ie_num,
        )
    }
    if ie_num >= 2 && matches!(join.join_type, JoinType::Inner | JoinType::Cross) {
        // Contain more than 2 ie conditions, use ie join
        return Ok(PhysicalJoinType::IEJoin);
    }

    if non_ie_num != 0 {
        // Contain non ie conditions, use sort merge join
        return Ok(PhysicalJoinType::SortMerge);
    }

    // Leverage hash join to execute nested loop join
    Ok(PhysicalJoinType::Hash)
}

fn check_non_equi_condition(
    expr: &ScalarExpr,
    left_prop: &RelationalProperty,
    right_prop: &RelationalProperty,
    ie_num: &mut usize,
    non_ie_num: &mut usize,
) {
    if let ScalarExpr::FunctionCall(func) = expr {
        if func.arguments.len() != 2 {
            return;
        }
        let mut left = false;
        let mut right = false;
        for arg in func.arguments.iter() {
            let join_predicate = JoinPredicate::new(arg, left_prop, right_prop);
            match join_predicate {
                JoinPredicate::Left(_) => left = true,
                JoinPredicate::Right(_) => right = true,
                JoinPredicate::Both { .. } | JoinPredicate::Other(_) => {
                    return;
                }
            }
        }
        if left && right {
            *non_ie_num += 1;
            if matches!(func.func_name.as_str(), "gt" | "lt" | "gte" | "lte") {
                *ie_num += 1;
            }
        }
    }
}
