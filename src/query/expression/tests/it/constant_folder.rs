// Copyright 2026 Datafuse Labs.
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
use std::sync::Arc;

use databend_common_expression::ConstantFolder;
use databend_common_expression::Domain;
use databend_common_expression::Function;
use databend_common_expression::FunctionContext;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionFactory;
use databend_common_expression::FunctionID;
use databend_common_expression::FunctionProperty;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_expression::domain_evaluator;
use databend_common_expression::expr::ColumnRef;
use databend_common_expression::expr::Constant;
use databend_common_expression::expr::Expr;
use databend_common_expression::expr::FunctionCall;
use databend_common_expression::scalar_evaluator;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberDomain;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::SimpleDomain;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::nullable::NullableDomain;

fn bool_column(id: usize, display_name: &str) -> Expr<usize> {
    Expr::ColumnRef(ColumnRef {
        span: None,
        id,
        data_type: DataType::Boolean,
        display_name: display_name.to_string(),
    })
}

fn nullable_bool_column(id: usize, display_name: &str) -> Expr<usize> {
    Expr::ColumnRef(ColumnRef {
        span: None,
        id,
        data_type: DataType::Nullable(Box::new(DataType::Boolean)),
        display_name: display_name.to_string(),
    })
}

fn bool_condition(scalar: Scalar) -> Expr<usize> {
    Expr::Constant(Constant {
        span: None,
        scalar,
        data_type: DataType::Nullable(Box::new(DataType::Boolean)),
    })
}

fn uint_column(id: usize, display_name: &str) -> Expr<usize> {
    Expr::ColumnRef(ColumnRef {
        span: None,
        id,
        data_type: DataType::Number(NumberDataType::UInt64),
        display_name: display_name.to_string(),
    })
}

fn uint_constant(value: u64) -> Expr<usize> {
    Expr::Constant(Constant {
        span: None,
        scalar: Scalar::Number(NumberScalar::UInt64(value)),
        data_type: DataType::Number(NumberDataType::UInt64),
    })
}

fn if_test_function(args_type: Vec<DataType>, return_type: DataType) -> Arc<Function> {
    Arc::new(Function {
        signature: FunctionSignature {
            name: "if".to_string(),
            args_type,
            return_type,
        },
        eval: FunctionEval::Scalar {
            calc_domain: domain_evaluator(|_, _| FunctionDomain::Full),
            eval: scalar_evaluator(|_, _| Value::Scalar(Scalar::Null)),
            derive_stat: None,
        },
    })
}

fn scalar_test_function(
    name: &str,
    args_type: Vec<DataType>,
    return_type: DataType,
) -> Arc<Function> {
    Arc::new(Function {
        signature: FunctionSignature {
            name: name.to_string(),
            args_type,
            return_type,
        },
        eval: FunctionEval::Scalar {
            calc_domain: domain_evaluator(|_, _| FunctionDomain::Full),
            eval: scalar_evaluator(|_, _| Value::Scalar(Scalar::Null)),
            derive_stat: None,
        },
    })
}

fn if_test_registry() -> FunctionRegistry {
    let mut registry = FunctionRegistry::empty();
    let factory = FunctionFactory::Closure(Box::new(|_, args_type: &[DataType]| {
        if args_type.len() < 3 || args_type.len().is_multiple_of(2) {
            return None;
        }

        let sig_args_type = (0..(args_type.len() - 1) / 2)
            .flat_map(|_| {
                [
                    DataType::Nullable(Box::new(DataType::Boolean)),
                    DataType::Generic(0),
                ]
            })
            .chain([DataType::Generic(0)])
            .collect();

        Some(if_test_function(sig_args_type, DataType::Generic(0)))
    }));
    registry.register_function_factory("if", factory);
    registry
}

fn comparison_expr(name: &str, left: Expr<usize>, right: Expr<usize>) -> Expr<usize> {
    Expr::FunctionCall(FunctionCall {
        span: None,
        id: Box::new(FunctionID::Builtin {
            name: name.to_string(),
            id: 0,
        }),
        function: scalar_test_function(
            name,
            vec![
                DataType::Number(NumberDataType::UInt64),
                DataType::Number(NumberDataType::UInt64),
            ],
            DataType::Boolean,
        ),
        generics: vec![],
        args: vec![left, right],
        return_type: DataType::Boolean,
    })
}

fn and_filters_expr(args: Vec<Expr<usize>>) -> Expr<usize> {
    Expr::FunctionCall(FunctionCall {
        span: None,
        id: Box::new(FunctionID::Builtin {
            name: "and_filters".to_string(),
            id: 0,
        }),
        function: scalar_test_function(
            "and_filters",
            vec![DataType::Boolean; args.len()],
            DataType::Boolean,
        ),
        generics: vec![],
        args,
        return_type: DataType::Boolean,
    })
}

fn if_expr(args: Vec<Expr<usize>>) -> Expr<usize> {
    Expr::FunctionCall(FunctionCall {
        span: None,
        id: Box::new(FunctionID::Builtin {
            name: "if".to_string(),
            id: 0,
        }),
        function: if_test_function(vec![], DataType::Boolean),
        generics: vec![],
        args,
        return_type: DataType::Boolean,
    })
}

fn fold_with_registry(expr: &Expr<usize>, registry: &FunctionRegistry) -> Expr<usize> {
    ConstantFolder::fold(expr, &FunctionContext::default(), registry).0
}

fn fold(expr: &Expr<usize>) -> Expr<usize> {
    fold_with_registry(expr, &if_test_registry())
}

#[test]
fn test_monotonic_nullable_domain_rejects_boundary_probe() {
    let mut registry = FunctionRegistry::empty();
    registry.register_passthrough_nullable_1_arg::<UInt64Type, UInt64Type, _>(
        "identity",
        |_, _| FunctionDomain::Full,
        |value, _| value,
    );
    registry.properties.insert(
        "identity".to_string(),
        FunctionProperty::default().monotonicity(),
    );

    let data_type = DataType::Number(NumberDataType::UInt64).wrap_nullable();
    let expr = databend_common_expression::type_check::check_function(
        None,
        "identity",
        &[],
        &[Expr::ColumnRef(ColumnRef {
            span: None,
            id: 0,
            data_type,
            display_name: "a".to_string(),
        })],
        &registry,
    )
    .unwrap();
    let input_domain = Domain::Nullable(NullableDomain {
        has_null: true,
        value: Some(Box::new(Domain::Number(NumberDomain::UInt64(
            SimpleDomain { min: 10, max: 20 },
        )))),
    });

    let (folded, output_domain) = ConstantFolder::fold_with_domain(
        &expr,
        &HashMap::from([(0, input_domain)]),
        &FunctionContext::default(),
        &registry,
    );

    assert_eq!(folded, expr);
    assert_eq!(output_domain, None);
}

#[test]
fn test_fold_and_filters_combined_constraints_to_false() {
    let column = uint_column(0, "a");
    let folded = fold_with_registry(
        &and_filters_expr(vec![
            comparison_expr("noteq", column.clone(), uint_constant(5)),
            comparison_expr("gte", column.clone(), uint_constant(5)),
            comparison_expr("lte", column, uint_constant(5)),
        ]),
        &FunctionRegistry::empty(),
    );

    assert_eq!(
        folded,
        Expr::Constant(Constant {
            span: None,
            scalar: Scalar::Boolean(false),
            data_type: DataType::Boolean,
        })
    );
}

#[test]
fn test_fold_if_constant_condition_to_selected_branch() {
    let then_expr = bool_column(0, "then_expr");
    let else_expr = bool_column(1, "else_expr");

    assert_eq!(
        fold(&if_expr(vec![
            bool_condition(Scalar::Boolean(true)),
            then_expr.clone(),
            else_expr.clone(),
        ])),
        then_expr
    );
    assert_eq!(
        fold(&if_expr(vec![
            bool_condition(Scalar::Boolean(false)),
            then_expr.clone(),
            else_expr.clone(),
        ])),
        else_expr
    );
    assert_eq!(
        fold(&if_expr(vec![
            bool_condition(Scalar::Null),
            then_expr,
            else_expr.clone(),
        ])),
        else_expr
    );
}

#[test]
fn test_fold_if_removes_unreachable_multi_branch_conditions() {
    let dynamic_cond = nullable_bool_column(0, "dynamic_cond");
    let dynamic_then = bool_column(1, "dynamic_then");
    let false_then = bool_column(2, "false_then");
    let true_then = bool_column(3, "true_then");
    let unreachable_else = bool_column(4, "unreachable_else");

    let folded = fold(&if_expr(vec![
        dynamic_cond.clone(),
        dynamic_then.clone(),
        bool_condition(Scalar::Boolean(false)),
        false_then,
        bool_condition(Scalar::Boolean(true)),
        true_then.clone(),
        unreachable_else,
    ]));

    match folded {
        Expr::FunctionCall(FunctionCall { function, args, .. }) => {
            assert_eq!(function.signature.args_type.len(), 3);
            assert_eq!(args, vec![dynamic_cond, dynamic_then, true_then]);
        }
        expr => panic!("expected folded if expression, got {expr:?}"),
    }
}

#[test]
fn test_fold_if_keeps_original_call_when_recheck_fails() {
    let dynamic_cond = nullable_bool_column(0, "dynamic_cond");
    let dynamic_then = bool_column(1, "dynamic_then");
    let skipped_then = bool_column(2, "skipped_then");
    let else_expr = bool_column(3, "else_expr");

    let folded = fold_with_registry(
        &if_expr(vec![
            dynamic_cond.clone(),
            dynamic_then.clone(),
            bool_condition(Scalar::Boolean(false)),
            skipped_then.clone(),
            else_expr.clone(),
        ]),
        &FunctionRegistry::empty(),
    );

    match folded {
        Expr::FunctionCall(FunctionCall { args, .. }) => {
            assert_eq!(args, vec![
                dynamic_cond,
                dynamic_then,
                bool_condition(Scalar::Boolean(false)),
                skipped_then,
                else_expr
            ]);
        }
        expr => panic!("expected original if expression, got {expr:?}"),
    }
}

#[test]
fn test_fold_if_ignores_malformed_call() {
    let folded = fold_with_registry(&if_expr(vec![]), &FunctionRegistry::empty());

    match folded {
        Expr::FunctionCall(FunctionCall { args, .. }) => assert!(args.is_empty()),
        expr => panic!("expected malformed if expression to remain a function call, got {expr:?}"),
    }
}

#[test]
fn test_fold_if_keeps_non_constant_conditions_unchanged() {
    let cond = nullable_bool_column(0, "cond");
    let then_expr = bool_column(1, "then_expr");
    let else_expr = bool_column(2, "else_expr");

    let folded = fold(&if_expr(vec![
        cond.clone(),
        then_expr.clone(),
        else_expr.clone(),
    ]));

    match folded {
        Expr::FunctionCall(FunctionCall { args, .. }) => {
            assert_eq!(args, vec![cond, then_expr, else_expr]);
        }
        expr => panic!("expected unchanged if expression, got {expr:?}"),
    }
}

#[test]
fn test_fold_nested_if_constant_conditions() {
    let selected = bool_column(0, "selected");
    let dead = bool_column(1, "dead");

    let inner = if_expr(vec![
        bool_condition(Scalar::Boolean(true)),
        selected.clone(),
        dead.clone(),
    ]);
    let outer = if_expr(vec![bool_condition(Scalar::Null), dead, inner]);

    assert_eq!(fold(&outer), selected);
}
