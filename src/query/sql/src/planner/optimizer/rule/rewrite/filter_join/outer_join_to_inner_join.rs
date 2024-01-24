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

use crate::optimizer::RelExpr;
use crate::optimizer::SExpr;
use crate::plans::Filter;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::ColumnSet;
use crate::IndexType;
use crate::ScalarExpr;

pub fn outer_to_inner(s_expr: &SExpr) -> Result<SExpr> {
    let join: Join = s_expr.child(0)?.plan().clone().try_into()?;
    let origin_join_type = join.join_type.clone();
    if !origin_join_type.is_outer_join() {
        return Ok(s_expr.clone());
    }

    let (s_expr, res) = outer_to_inner_impl(s_expr)?;
    if res {
        return Ok(s_expr);
    }

    #[cfg(feature = "z3-prove")]
    {
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
            JoinType::Left | JoinType::LeftSingle => {
                if eliminate_right_null {
                    JoinType::Inner
                } else {
                    join.join_type
                }
            }
            JoinType::Right | JoinType::RightSingle => {
                if eliminate_left_null {
                    JoinType::Inner
                } else {
                    join.join_type
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

        if new_join_type == JoinType::Inner {
            if origin_join_type == JoinType::LeftSingle {
                join.original_join_type = Some(JoinType::LeftSingle);
            } else {
                join.original_join_type = Some(JoinType::RightSingle);
            }
        }

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
    {
        let (s_expr, _) = outer_to_inner_impl(s_expr)?;
        Ok(s_expr)
    }
}

fn outer_to_inner_impl(s_expr: &SExpr) -> Result<(SExpr, bool)> {
    let filter: Filter = s_expr.plan().clone().try_into()?;
    let mut join: Join = s_expr.child(0)?.plan().clone().try_into()?;
    let origin_join_type = join.join_type.clone();
    let s_join_expr = s_expr.child(0)?;
    let join_expr = RelExpr::with_s_expr(s_join_expr);
    let left_child_output_column = join_expr
        .derive_relational_prop_child(0)?
        .output_columns
        .clone();
    let right_child_output_column = join_expr
        .derive_relational_prop_child(1)?
        .output_columns
        .clone();
    let predicates = &filter.predicates;
    let mut nullable_columns: Vec<IndexType> = vec![];
    for predicate in predicates {
        find_nullable_columns(
            predicate,
            &left_child_output_column,
            &right_child_output_column,
            &mut nullable_columns,
        )?;
    }

    if join.join_type.is_outer_join() {
        let mut left_join = false;
        let mut right_join = false;
        for col in nullable_columns.iter() {
            if left_child_output_column.contains(col) {
                right_join = true;
            }
            if right_child_output_column.contains(col) {
                left_join = true;
            }
        }

        match join.join_type {
            JoinType::Left | JoinType::LeftSingle => {
                if left_join {
                    join.join_type = JoinType::Inner
                }
            }
            JoinType::Right | JoinType::RightSingle => {
                if right_join {
                    join.join_type = JoinType::Inner
                }
            }
            JoinType::Full => {
                if left_join && right_join {
                    join.join_type = JoinType::Inner
                } else if left_join {
                    join.join_type = JoinType::Right
                } else if right_join {
                    join.join_type = JoinType::Left
                }
            }
            _ => unreachable!(),
        }
    }

    let changed_join_type = join.join_type.clone();
    if origin_join_type == changed_join_type {
        return Ok((s_expr.clone(), false));
    }

    if origin_join_type == JoinType::LeftSingle {
        join.original_join_type = Some(JoinType::LeftSingle);
    } else {
        join.original_join_type = Some(JoinType::RightSingle);
    }

    let mut result = SExpr::create_binary(
        Arc::new(join.into()),
        Arc::new(s_join_expr.child(0)?.clone()),
        Arc::new(s_join_expr.child(1)?.clone()),
    );
    // wrap filter s_expr
    result = SExpr::create_unary(Arc::new(filter.into()), Arc::new(result));
    Ok((result, true))
}
#[allow(clippy::only_used_in_recursion)]
fn find_nullable_columns(
    predicate: &ScalarExpr,
    left_output_columns: &ColumnSet,
    right_output_columns: &ColumnSet,
    nullable_columns: &mut Vec<IndexType>,
) -> Result<()> {
    match predicate {
        ScalarExpr::BoundColumnRef(column_binding) => {
            nullable_columns.push(column_binding.column.index);
        }
        ScalarExpr::FunctionCall(func) => {
            match func.func_name.as_str() {
                "or" => {
                    let mut left_cols = vec![];
                    let mut right_cols = vec![];
                    find_nullable_columns(
                        &func.arguments[0],
                        left_output_columns,
                        right_output_columns,
                        &mut left_cols,
                    )?;
                    find_nullable_columns(
                        &func.arguments[1],
                        left_output_columns,
                        right_output_columns,
                        &mut right_cols,
                    )?;
                    if !left_cols.is_empty() && !right_cols.is_empty() {
                        for left_col in left_cols.iter() {
                            for right_col in right_cols.iter() {
                                if (left_output_columns.contains(left_col)
                                    && left_output_columns.contains(right_col))
                                    || (right_output_columns.contains(left_col)
                                        && right_output_columns.contains(right_col))
                                {
                                    nullable_columns.push(*left_col);
                                    break;
                                }
                            }
                        }
                    }
                }
                "eq" | "lt" | "lte" | "gt" | "gte" | "noteq" | "and" => {
                    find_nullable_columns(
                        &func.arguments[0],
                        left_output_columns,
                        right_output_columns,
                        nullable_columns,
                    )?;
                    find_nullable_columns(
                        &func.arguments[1],
                        left_output_columns,
                        right_output_columns,
                        nullable_columns,
                    )?;
                }
                // Todo: support more functions.
                _ => {}
            }
        }
        ScalarExpr::CastExpr(expr) => {
            find_nullable_columns(
                &expr.argument,
                left_output_columns,
                right_output_columns,
                nullable_columns,
            )?;
        }
        // Todo: support more ScalarExpr.
        _ => {}
    }
    Ok(())
}
