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

use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::Column;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::Expr;
use databend_common_expression::RemoteExpr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::executor::cast_expr_to_non_null_boolean;
use databend_common_sql::plans::BoundColumnRef;
use databend_common_sql::plans::FunctionCall;
use databend_common_sql::ColumnBinding;
use databend_common_sql::ScalarExpr;
use databend_common_sql::TypeCheck;
use databend_common_sql::Visibility;
use itertools::Itertools;
use rand::Rng;

// Generate a random filter expression with the given `max_depth` and `num_columns`.
pub fn random_filter_expr(
    data_type: &DataType,
    max_depth: usize,
    num_columns: usize,
) -> Result<Expr> {
    // 1. Generate a random predicate tree with the given depth.
    let predicate_tree = random_predicate_tree_with_depth(max_depth);

    // 2. Convert the predicate tree to `ScalarExpr`:
    //    (1) The `func_name` of `FunctionCall` is randomly selected from ["eq", "noteq", "gt", "lt", "gte", "lte"].
    //    (2) All BoundColumnRef temprarilly use 0 as index.
    let scalar_expr = convert_predicate_tree_to_scalar_expr(predicate_tree.clone(), data_type);

    // 3. Convert the `ScalarExpr` to `RemoteExpr`.
    let remote_expr = convert_scalar_expr_to_remote_expr(scalar_expr, data_type)?;

    // 4. Convert the `RemoteExpr` to `Expr`.
    let expr = convert_remote_expr_to_expr(remote_expr)?;

    // 5. Replace the `ColumnRef` in the `Expr` to `ColumnRef` or `Constant`,
    //    the `id` of `ColumnRef` is in range 0..num_columns.
    let expr = replace_const_to_const_and_column_ref(expr, data_type, num_columns);

    Ok(expr)
}

// Only used for test.
// PredicateNode is used for representing the predicate tree.
#[derive(Clone, Debug)]
enum PredicateNode {
    And(Vec<PredicateNode>),
    Or(Vec<PredicateNode>),
    Not(Box<PredicateNode>),
    Leaf,
}

// Generate a random predicate tree with the given depth.
fn random_predicate_tree_with_depth(depth: usize) -> PredicateNode {
    let mut rng = rand::thread_rng();
    // The probability of generating a `Not` node is 5%.
    if rng.gen_bool(0.05) {
        return PredicateNode::Not(Box::new(random_predicate_tree_with_depth(depth)));
    }
    // The probability of generating a `Leaf` node is 25%.
    if depth == 0 || rng.gen_bool(0.25) {
        return PredicateNode::Leaf;
    }
    let children = (0..2)
        .map(|_| random_predicate_tree_with_depth(depth - 1))
        .collect_vec();
    // Generate a `And` or `Or` node.
    if rng.gen_bool(0.5) {
        PredicateNode::And(children)
    } else {
        PredicateNode::Or(children)
    }
}

// Convert the predicate tree to `ScalarExpr`:
// 1. The `func_name` of `FunctionCall` is randomly selected from ["eq", "noteq", "gt", "lt", "gte", "lte"].
// 2. All BoundColumnRef temprarilly use 0 as index.
fn convert_predicate_tree_to_scalar_expr(node: PredicateNode, data_type: &DataType) -> ScalarExpr {
    match node {
        PredicateNode::And(children) => {
            let mut and_args = Vec::with_capacity(children.len());
            for child in children {
                let child_expr = convert_predicate_tree_to_scalar_expr(child, data_type);
                and_args.push(child_expr);
            }
            ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "and".to_string(),
                params: vec![],
                arguments: and_args,
            })
        }
        PredicateNode::Or(children) => {
            let mut or_args = Vec::with_capacity(children.len());
            for child in children {
                let child_expr = convert_predicate_tree_to_scalar_expr(child, data_type);
                or_args.push(child_expr);
            }
            ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "or".to_string(),
                params: vec![],
                arguments: or_args,
            })
        }
        PredicateNode::Not(child) => {
            let child_expr = convert_predicate_tree_to_scalar_expr(*child, data_type);
            ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "not".to_string(),
                params: vec![],
                arguments: vec![child_expr],
            })
        }
        PredicateNode::Leaf => {
            let mut rng = rand::thread_rng();
            let op = match rng.gen_range(0..6) {
                0 => "eq",
                1 => "noteq",
                2 => "gt",
                3 => "lt",
                4 => "gte",
                5 => "lte",
                _ => unreachable!(),
            };
            let column = ColumnBinding {
                database_name: None,
                table_name: None,
                column_position: None,
                table_index: None,
                column_name: "".to_string(),
                index: 0,
                data_type: Box::new(data_type.clone()),
                visibility: Visibility::Visible,
                virtual_computed_expr: None,
            };
            let scalar_expr = ScalarExpr::BoundColumnRef(BoundColumnRef { span: None, column });
            ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: op.to_string(),
                params: vec![],
                arguments: vec![scalar_expr.clone(), scalar_expr],
            })
        }
    }
}

// Convert the `ScalarExpr` to `RemoteExpr`.
fn convert_scalar_expr_to_remote_expr(
    scalar_expr: ScalarExpr,
    data_type: &DataType,
) -> Result<RemoteExpr> {
    let schema = Arc::new(DataSchema::new(vec![DataField::new(
        "0",
        data_type.clone(),
    )]));
    let expr = scalar_expr
        .type_check(schema.as_ref())?
        .project_column_ref(|index| schema.index_of(&index.to_string()).unwrap());
    Ok(expr.as_remote_expr())
}

// Convert the `RemoteExpr` to `Expr`.
fn convert_remote_expr_to_expr(remote_expr: RemoteExpr) -> Result<Expr> {
    let expr = remote_expr.as_expr(&BUILTIN_FUNCTIONS);
    cast_expr_to_non_null_boolean(expr)
}

// Replace the `ColumnRef` in the `Expr` to `ColumnRef` or `Constant`,
// the `id` of `ColumnRef` is in range 0..num_columns.
fn replace_const_to_const_and_column_ref(
    expr: Expr,
    data_type: &DataType,
    num_columns: usize,
) -> Expr {
    match expr {
        Expr::FunctionCall {
            span,
            id,
            function,
            args,
            generics,
            return_type,
        } => {
            let mut new_args = Vec::with_capacity(args.len());
            for arg in args {
                new_args.push(replace_const_to_const_and_column_ref(
                    arg,
                    data_type,
                    num_columns,
                ));
            }
            Expr::FunctionCall {
                span,
                id,
                function,
                args: new_args,
                generics,
                return_type,
            }
        }
        Expr::ColumnRef { .. } => {
            let mut rng = rand::thread_rng();
            if rng.gen_bool(0.5) {
                // Replace the child to `ColumnRef`
                let mut rng = rand::thread_rng();
                let index = rng.gen_range(0..num_columns);
                Expr::ColumnRef {
                    span: None,
                    id: index,
                    data_type: data_type.clone(),
                    display_name: "".to_string(),
                }
            } else {
                // Replace the child to `Constant`
                let scalar = Column::random(data_type, 1, None)
                    .index(0)
                    .unwrap()
                    .to_owned();
                Expr::Constant {
                    span: None,
                    scalar,
                    data_type: data_type.clone(),
                }
            }
        }
        Expr::Cast {
            span,
            is_try,
            expr,
            dest_type,
        } => {
            let expr = replace_const_to_const_and_column_ref(*expr, data_type, num_columns);
            Expr::Cast {
                span,
                is_try,
                expr: Box::new(expr),
                dest_type,
            }
        }
        _ => unreachable!("expr = {:?}", expr),
    }
}
