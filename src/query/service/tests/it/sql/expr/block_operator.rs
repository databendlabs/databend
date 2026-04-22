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
use std::collections::BTreeSet;

use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::Expr;
use databend_common_expression::RawExpr;
use databend_common_expression::Scalar;
use databend_common_expression::type_check::check;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::evaluator::apply_cse;
use itertools::Itertools;

fn count_function_calls(expr: &Expr, fn_name: &str) -> usize {
    match expr {
        Expr::FunctionCall(call) => {
            usize::from(call.function.signature.name == fn_name)
                + call
                    .args
                    .iter()
                    .map(|arg| count_function_calls(arg, fn_name))
                    .sum::<usize>()
        }
        Expr::LambdaFunctionCall(call) => call
            .args
            .iter()
            .map(|arg| count_function_calls(arg, fn_name))
            .sum(),
        Expr::Cast(cast) => count_function_calls(&cast.expr, fn_name),
        Expr::Constant(_) | Expr::ColumnRef(_) => 0,
    }
}

#[test]
fn test_cse() {
    let schema = DataSchemaRefExt::create(vec![DataField::new(
        "a",
        DataType::Number(NumberDataType::Int32),
    )]);

    // a + 1,  (a + 1) *2
    let exprs = vec![
        RawExpr::FunctionCall {
            span: None,
            name: "plus".to_string(),
            params: vec![],
            args: vec![
                RawExpr::ColumnRef {
                    span: None,
                    id: 0usize,
                    data_type: schema.field(0).data_type().clone(),
                    display_name: schema.field(0).name().clone(),
                },
                RawExpr::Constant {
                    span: None,
                    scalar: Scalar::Number(NumberScalar::UInt64(1)),
                    data_type: None,
                },
            ],
        },
        RawExpr::FunctionCall {
            span: None,
            name: "multiply".to_string(),
            params: vec![],
            args: vec![
                RawExpr::FunctionCall {
                    span: None,
                    name: "plus".to_string(),
                    params: vec![],
                    args: vec![
                        RawExpr::ColumnRef {
                            span: None,
                            id: 0usize,
                            data_type: schema.field(0).data_type().clone(),
                            display_name: schema.field(0).name().clone(),
                        },
                        RawExpr::Constant {
                            span: None,
                            scalar: Scalar::Number(NumberScalar::UInt64(1)),
                            data_type: None,
                        },
                    ],
                },
                RawExpr::Constant {
                    span: None,
                    scalar: Scalar::Number(NumberScalar::UInt64(2)),
                    data_type: None,
                },
            ],
        },
    ];

    let exprs: Vec<Expr> = exprs
        .iter()
        .map(|expr| check(expr, &BUILTIN_FUNCTIONS).unwrap())
        .collect();

    let mut projections = BTreeSet::new();
    projections.insert(1);
    projections.insert(2);
    let operators = vec![BlockOperator::Map {
        exprs,
        projections: Some(projections),
    }];

    let mut operators = apply_cse(operators, 1);

    assert_eq!(operators.len(), 1);

    match operators.pop().unwrap() {
        BlockOperator::Map { exprs, projections } => {
            assert_eq!(exprs.len(), 3);
            assert_eq!(exprs[0].sql_display(), "a + 1");
            assert_eq!(exprs[1].sql_display(), "__temp_cse_1");
            assert_eq!(exprs[2].sql_display(), "__temp_cse_1 * 2");
            assert_eq!(
                projections
                    .unwrap()
                    .into_iter()
                    .sorted()
                    .collect::<Vec<_>>(),
                vec![2, 3]
            );
        }

        _ => unreachable!(),
    }
}

#[test]
fn test_cse_parse_json_reuse() {
    let schema = DataSchemaRefExt::create(vec![DataField::new("repo", DataType::String)]);

    let parse_repo = || RawExpr::FunctionCall {
        span: None,
        name: "parse_json".to_string(),
        params: vec![],
        args: vec![RawExpr::ColumnRef {
            span: None,
            id: 0usize,
            data_type: schema.field(0).data_type().clone(),
            display_name: schema.field(0).name().clone(),
        }],
    };

    let exprs = vec![
        RawExpr::FunctionCall {
            span: None,
            name: "get".to_string(),
            params: vec![],
            args: vec![parse_repo(), RawExpr::Constant {
                span: None,
                scalar: Scalar::String("name".to_string()),
                data_type: None,
            }],
        },
        RawExpr::FunctionCall {
            span: None,
            name: "get".to_string(),
            params: vec![],
            args: vec![parse_repo(), RawExpr::Constant {
                span: None,
                scalar: Scalar::String("url".to_string()),
                data_type: None,
            }],
        },
    ];

    let exprs: Vec<Expr> = exprs
        .iter()
        .map(|expr| check(expr, &BUILTIN_FUNCTIONS).unwrap())
        .collect();

    let mut projections = BTreeSet::new();
    projections.insert(1);
    projections.insert(2);
    let operators = vec![BlockOperator::Map {
        exprs,
        projections: Some(projections),
    }];

    let mut operators = apply_cse(operators, 1);

    assert_eq!(operators.len(), 1);

    match operators.pop().unwrap() {
        BlockOperator::Map { exprs, projections } => {
            assert_eq!(exprs.len(), 3);
            assert_eq!(
                exprs
                    .iter()
                    .map(|expr| count_function_calls(expr, "parse_json"))
                    .sum::<usize>(),
                1
            );
            assert_eq!(
                projections
                    .unwrap()
                    .into_iter()
                    .sorted()
                    .collect::<Vec<_>>(),
                vec![2, 3]
            );
        }
        _ => unreachable!(),
    }
}
