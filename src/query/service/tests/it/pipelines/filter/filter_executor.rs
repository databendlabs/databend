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

use common_exception::Result;
use common_expression::build_select_expr;
use common_expression::filter::FilterExecutor;
use common_expression::types::BooleanType;
use common_expression::types::DataType;
use common_expression::types::DecimalDataType;
use common_expression::types::DecimalSize;
use common_expression::types::NumberDataType;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::DataSchema;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_expression::RemoteExpr;
use common_functions::BUILTIN_FUNCTIONS;
use common_sql::plans::ConstantExpr;
use common_sql::plans::FunctionCall;
use common_sql::ScalarExpr;
use common_sql::TypeCheck;
use itertools::Itertools;
use rand::Rng;

#[derive(Clone, Debug)]
enum PredicateNode {
    And(Vec<PredicateNode>),
    Or(Vec<PredicateNode>),
    Leaf,
}

fn random_predicate_tree_with_depth(depth: usize) -> PredicateNode {
    let mut rng = rand::thread_rng();
    if depth == 0 || rng.gen_bool(0.25) {
        return PredicateNode::Leaf;
    }
    let children = (0..2)
        .map(|_| random_predicate_tree_with_depth(depth - 1))
        .collect_vec();
    if rng.gen_bool(0.5) {
        PredicateNode::And(children)
    } else {
        PredicateNode::Or(children)
    }
}

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
            let scalar = Column::random(data_type, 1).index(0).unwrap().to_owned();
            dbg!("scalar = {:?}", &scalar);
            let left = ScalarExpr::ConstantExpr(ConstantExpr {
                span: None,
                value: scalar.clone(),
            });
            let right = ScalarExpr::ConstantExpr(ConstantExpr {
                span: None,
                value: scalar,
            });
            ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: op.to_string(),
                params: vec![],
                arguments: vec![left, right],
            })
        }
    }
}

fn convert_scalar_expr_to_remote_expr(scalar_expr: ScalarExpr) -> Result<RemoteExpr> {
    let schema = Arc::new(DataSchema::new(vec![]));
    let expr = scalar_expr
        .type_check(schema.as_ref())?
        .project_column_ref(|index| schema.index_of(&index.to_string()).unwrap());
    Ok(expr.as_remote_expr())
}

fn convert_remote_expr_to_expr(remote_expr: RemoteExpr) -> Expr {
    remote_expr.as_expr(&BUILTIN_FUNCTIONS)
}

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
        Expr::Constant { .. } => {
            let mut rng = rand::thread_rng();
            if rng.gen_bool(0.5) {
                // Replace the child to column ref
                let mut rng = rand::thread_rng();
                let index = rng.gen_range(0..num_columns);
                Expr::ColumnRef {
                    span: None,
                    id: index,
                    data_type: data_type.clone(),
                    display_name: "".to_string(),
                }
            } else {
                // Replace the child to constant
                let scalar = Column::random(data_type, 1).index(0).unwrap().to_owned();
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

#[test]
pub fn test_filter() -> common_exception::Result<()> {
    let predicate_tree = random_predicate_tree_with_depth(5);
    // dbg!("predicate_tree = {:?}", &predicate_tree);
    let mut rng = rand::thread_rng();
    let data_types = get_some_test_data_types();
    for data_type in data_types {
        let num_rows = rng.gen_range(2..30);
        let num_columns = rng.gen_range(3..5);
        let columns = (0..num_columns)
            .map(|_| Column::random(&data_type, num_rows))
            .collect_vec();
        let block = DataBlock::new_from_columns(columns);
        block.check_valid()?;
        let scalar_expr = convert_predicate_tree_to_scalar_expr(predicate_tree.clone(), &data_type);
        let remote_expr = convert_scalar_expr_to_remote_expr(scalar_expr)?;
        let expr = convert_remote_expr_to_expr(remote_expr);
        let func_ctx = &FunctionContext::default();
        let expr = replace_const_to_const_and_column_ref(expr, &data_type, num_columns - 1);
        // dbg!("expr = {:?}", &expr);
        let evaluator = Evaluator::new(&block, func_ctx, &BUILTIN_FUNCTIONS);

        // let value = evaluator.run(&expr)?;
        // let filter = match value.try_downcast::<BooleanType>() {
        //     Some(filter) => filter,
        //     None => {
        //         dbg!("data_type = {:?}", &data_type);
        //         dbg!("value = {:?}", &value);
        //         panic!()
        //     }
        // };
        let filter = evaluator.run(&expr)?.try_downcast::<BooleanType>().unwrap();

        let (select_expr, has_or) = build_select_expr(&expr);
        let mut filter_executor = FilterExecutor::new(
            select_expr,
            func_ctx.clone(),
            has_or,
            num_rows,
            None,
            &BUILTIN_FUNCTIONS,
            true,
        );
        let block_1 = filter_executor.filter(block.clone())?;
        let block_2 = block.clone().filter_boolean_value(&filter)?;

        // dbg!("block = {:?}", &block);
        // dbg!("block_1 = {:?}", &block_1);
        // dbg!("block_2 = {:?}", &block_2);

        // dbg!("filter = {:?}", &filter);
        // dbg!(
        //     "true_selection = {:?}",
        //     &filter_executor.mut_true_selection()[0..block_1.num_rows()]
        // );

        assert_eq!(block_1.num_columns(), block_2.num_columns());
        assert_eq!(block_1.num_rows(), block_2.num_rows());

        let columns_1 = block_1.columns();
        let columns_2 = block_2.columns();
        for idx in 0..columns_1.len() {
            assert_eq!(columns_1[idx].data_type, columns_2[idx].data_type);
            assert_eq!(columns_1[idx].value, columns_2[idx].value);
        }
    }

    Ok(())
}

fn get_some_test_data_types() -> Vec<DataType> {
    vec![
        // passed
        DataType::EmptyArray,
        DataType::Boolean,
        DataType::String,
        DataType::Variant,
        DataType::Timestamp,
        DataType::Date,
        DataType::Number(NumberDataType::UInt8),
        DataType::Number(NumberDataType::UInt16),
        DataType::Number(NumberDataType::UInt32),
        DataType::Number(NumberDataType::UInt64),
        DataType::Number(NumberDataType::Int8),
        DataType::Number(NumberDataType::Int16),
        DataType::Number(NumberDataType::Int32),
        DataType::Number(NumberDataType::Int64),
        DataType::Number(NumberDataType::Float32),
        DataType::Number(NumberDataType::Float64),
        DataType::Decimal(DecimalDataType::Decimal128(DecimalSize {
            precision: 10,
            scale: 2,
        })),
        DataType::Decimal(DecimalDataType::Decimal128(DecimalSize {
            precision: 35,
            scale: 3,
        })),
        DataType::Array(Box::new(DataType::Number(NumberDataType::UInt32))),
        // unsupported
        // DataType::EmptyMap,
        // DataType::Map(Box::new(DataType::Tuple(vec![
        //     DataType::Number(NumberDataType::UInt64),
        //     DataType::String,
        // ]))),
        // DataType::Bitmap,

        // todo
        // DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt32))),
        // DataType::Nullable(Box::new(DataType::String)),
        // DataType::Null,
    ]
}
