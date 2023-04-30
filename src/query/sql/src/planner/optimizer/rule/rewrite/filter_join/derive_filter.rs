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

use std::collections::HashMap;

use common_exception::Result;

use crate::optimizer::SExpr;
use crate::plans::Filter;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::WindowFuncType;
use crate::IndexType;
use crate::ScalarExpr;

/// Derive filter to push down
pub fn try_derive_predicates(
    s_expr: &SExpr,
    join: Join,
    mut left_push_down: Vec<ScalarExpr>,
    mut right_push_down: Vec<ScalarExpr>,
) -> Result<SExpr> {
    let join_expr = s_expr.child(0)?;
    let mut left_child = join_expr.child(0)?.clone();
    let mut right_child = join_expr.child(1)?.clone();

    if join.join_type == JoinType::Inner {
        let mut new_left_push_down = vec![];
        let mut new_right_push_down = vec![];
        for predicate in left_push_down.iter() {
            let used_columns = predicate.used_columns();
            let mut col_to_scalar = HashMap::with_capacity(used_columns.len());
            for column in used_columns.iter() {
                for (idx, left_condition) in join.left_conditions.iter().enumerate() {
                    if left_condition.used_columns().len() > 1
                        || !left_condition.used_columns().contains(column)
                    {
                        continue;
                    }
                    col_to_scalar.insert(column, &join.right_conditions[idx]);
                    break;
                }
            }
            if col_to_scalar.len() == used_columns.len() {
                derive_predicate(&col_to_scalar, predicate, &mut new_right_push_down)?;
            }
        }
        for predicate in right_push_down.iter() {
            let used_columns = predicate.used_columns();
            let mut col_to_scalar = HashMap::with_capacity(used_columns.len());
            for column in used_columns.iter() {
                for (idx, right_condition) in join.right_conditions.iter().enumerate() {
                    if right_condition.used_columns().len() > 1
                        || !right_condition.used_columns().contains(column)
                    {
                        continue;
                    }
                    col_to_scalar.insert(column, &join.left_conditions[idx]);
                    break;
                }
            }
            if col_to_scalar.len() == used_columns.len() {
                derive_predicate(&col_to_scalar, predicate, &mut new_left_push_down)?;
            }
        }
        left_push_down.extend(new_left_push_down);
        right_push_down.extend(new_right_push_down);
    }

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
    Ok(SExpr::create_binary(join.into(), left_child, right_child))
}

fn derive_predicate(
    col_to_scalar: &HashMap<&IndexType, &ScalarExpr>,
    predicate: &ScalarExpr,
    new_push_down: &mut Vec<ScalarExpr>,
) -> Result<()> {
    let mut replaced_predicate = predicate.clone();
    replace_column(&mut replaced_predicate, col_to_scalar);
    if &replaced_predicate != predicate {
        new_push_down.push(replaced_predicate);
    }
    Ok(())
}

fn replace_column(scalar: &mut ScalarExpr, col_to_scalar: &HashMap<&IndexType, &ScalarExpr>) {
    match scalar {
        ScalarExpr::BoundColumnRef(column) => {
            let column_index = column.column.index;
            // Safe to unwrap
            *scalar = (*col_to_scalar.get(&column_index).unwrap()).clone();
        }
        ScalarExpr::WindowFunction(expr) => {
            if let WindowFuncType::Aggregate(agg) = &mut expr.func {
                for arg in agg.args.iter_mut() {
                    replace_column(arg, col_to_scalar);
                }
            }
            for arg in expr.partition_by.iter_mut() {
                replace_column(arg, col_to_scalar)
            }
            for arg in expr.order_by.iter_mut() {
                replace_column(&mut arg.expr, col_to_scalar)
            }
        }
        ScalarExpr::AggregateFunction(expr) => {
            for arg in expr.args.iter_mut() {
                replace_column(arg, col_to_scalar)
            }
        }
        ScalarExpr::FunctionCall(expr) => {
            for arg in expr.arguments.iter_mut() {
                replace_column(arg, col_to_scalar)
            }
        }
        ScalarExpr::CastExpr(expr) => {
            replace_column(&mut expr.argument, col_to_scalar);
        }
        ScalarExpr::ConstantExpr(_) | ScalarExpr::SubqueryExpr(_) => {}
    }
}
