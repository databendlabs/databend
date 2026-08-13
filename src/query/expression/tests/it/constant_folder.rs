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
use std::io::Write;
use std::sync::Arc;

use databend_common_expression::ConstantFolder;
use databend_common_expression::Domain;
use databend_common_expression::Function;
use databend_common_expression::FunctionContext;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionID;
use databend_common_expression::FunctionProperty;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::RangeConstraint;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_expression::domain_evaluator;
use databend_common_expression::expr::Cast;
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
use databend_common_expression::types::UInt8Type;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::nullable::NullableDomain;
use databend_common_expression_test_support::parse_raw_expr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use goldenfile::Mint;

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

fn run_fold_case(
    file: &mut impl Write,
    text: &str,
    columns: &[(&str, DataType)],
    domain_overrides: &[(&str, Domain)],
    registry: &FunctionRegistry,
) {
    let raw_expr = parse_raw_expr(text, columns, registry);
    let expr = databend_common_expression::type_check::check(&raw_expr, registry).unwrap();
    let input_domains = columns
        .iter()
        .enumerate()
        .map(|(index, (name, data_type))| {
            let domain = domain_overrides
                .iter()
                .find(|(domain_name, _)| domain_name == name)
                .map(|(_, domain)| domain.clone())
                .unwrap_or_else(|| Domain::full(data_type));
            (index, domain)
        })
        .collect::<HashMap<_, _>>();
    let (folded, output_domain) = ConstantFolder::fold_with_domain(
        &expr,
        &input_domains,
        &FunctionContext::default(),
        registry,
    );

    writeln!(file, "expression: {text}").unwrap();
    let mut used_columns = raw_expr.column_refs().keys().copied().collect::<Vec<_>>();
    used_columns.sort_unstable();
    writeln!(
        file,
        "inputs:     {}",
        used_columns
            .iter()
            .map(|index| format!("{}: {}", columns[*index].0, input_domains[index]))
            .collect::<Vec<_>>()
            .join(", ")
    )
    .unwrap();
    writeln!(file, "checked:    {}", expr.sql_display()).unwrap();
    writeln!(file, "folded:     {}", folded.sql_display()).unwrap();
    writeln!(
        file,
        "domain:     {}\n",
        output_domain
            .map(|domain| domain.to_string())
            .unwrap_or_else(|| "Unknown".to_string())
    )
    .unwrap();
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

#[test]
fn test_range_constraint_unwraps_only_nullable_constant_cast() {
    let data_type = DataType::Number(NumberDataType::UInt64);
    let nullable_type = data_type.clone().wrap_nullable();
    let column = Expr::ColumnRef(ColumnRef {
        span: None,
        id: 0,
        data_type: nullable_type.clone(),
        display_name: "a".to_string(),
    });
    let constant = uint_constant(7);
    let nullable_constant = Expr::Cast(Cast {
        span: None,
        is_try: false,
        expr: Box::new(constant.clone()),
        dest_type: nullable_type,
    });

    let constraint =
        RangeConstraint::try_from_expr(&comparison_expr("gte", column.clone(), nullable_constant))
            .unwrap();
    assert_eq!(constraint.column_id, 0);
    assert_eq!(constraint.operator, "gte");
    assert_eq!(constraint.constant, Scalar::Number(NumberScalar::UInt64(7)));

    let converted_constant = Expr::Cast(Cast {
        span: None,
        is_try: false,
        expr: Box::new(constant.clone()),
        dest_type: DataType::Number(NumberDataType::Int64).wrap_nullable(),
    });
    assert!(
        RangeConstraint::try_from_expr(
            &comparison_expr("gte", column.clone(), converted_constant,)
        )
        .is_none()
    );

    let try_cast_constant = Expr::Cast(Cast {
        span: None,
        is_try: true,
        expr: Box::new(constant),
        dest_type: data_type.wrap_nullable(),
    });
    assert!(
        RangeConstraint::try_from_expr(&comparison_expr("gte", column, try_cast_constant))
            .is_none()
    );
}

#[test]
fn test_constant_folder_golden() {
    let mut mint = Mint::new("tests/it/testdata");
    let mut file = mint.new_goldenfile("constant_folder.txt").unwrap();
    let columns = [
        ("a", DataType::Number(NumberDataType::UInt8)),
        ("then_expr", DataType::Boolean),
        ("else_expr", DataType::Boolean),
        ("dynamic_cond", DataType::Boolean.wrap_nullable()),
        ("dynamic_then", DataType::Boolean),
        ("false_then", DataType::Boolean),
        ("true_then", DataType::Boolean),
        ("unreachable_else", DataType::Boolean),
        ("cond", DataType::Boolean.wrap_nullable()),
        ("selected", DataType::Boolean),
        ("dead", DataType::Boolean),
    ];

    for expression in [
        "and_filters(noteq(a, 5), gte(a, 5), lte(a, 5))",
        "if(true, then_expr, else_expr)",
        "if(false, then_expr, else_expr)",
        "if(null, then_expr, else_expr)",
        "if(dynamic_cond, dynamic_then, false, false_then, true, true_then, unreachable_else)",
        "if(cond, then_expr, else_expr)",
        "if(null, dead, if(true, selected, dead))",
    ] {
        run_fold_case(&mut file, expression, &columns, &[], &BUILTIN_FUNCTIONS);
    }

    let mut registry = FunctionRegistry::empty();
    registry.register_passthrough_nullable_1_arg::<UInt64Type, UInt64Type, _>(
        "identity",
        |_, _| FunctionDomain::Full,
        |value, _| value,
    );
    registry.properties.insert(
        "identity".to_string(),
        FunctionProperty::default()
            .monotonicity_type(DataType::Number(NumberDataType::UInt64).wrap_nullable()),
    );
    run_fold_case(
        &mut file,
        "identity(value)",
        &[(
            "value",
            DataType::Number(NumberDataType::UInt64).wrap_nullable(),
        )],
        &[(
            "value",
            Domain::Nullable(NullableDomain {
                has_null: true,
                value: Some(Box::new(Domain::Number(NumberDomain::UInt64(
                    SimpleDomain { min: 10, max: 20 },
                )))),
            }),
        )],
        &registry,
    );

    let mut registry = FunctionRegistry::empty();
    registry.register_1_arg::<UInt64Type, UInt64Type, _>(
        "fallible_identity",
        |_, _| FunctionDomain::Full,
        |value, ctx| {
            if value == 10 {
                ctx.set_error(0, "lower boundary failed");
            }
            value
        },
    );
    registry.properties.insert(
        "fallible_identity".to_string(),
        FunctionProperty::default().monotonicity(),
    );
    run_fold_case(
        &mut file,
        "fallible_identity(value)",
        &[(
            "value",
            DataType::Number(NumberDataType::UInt64).wrap_nullable(),
        )],
        &[(
            "value",
            Domain::Nullable(NullableDomain {
                has_null: false,
                value: Some(Box::new(Domain::Number(NumberDomain::UInt64(
                    SimpleDomain { min: 10, max: 20 },
                )))),
            }),
        )],
        &registry,
    );

    fn below_100(_ctx: &FunctionContext, args: &[Domain]) -> Option<usize> {
        match args {
            [Domain::Number(NumberDomain::UInt64(domain))] if domain.max < 100 => Some(0),
            _ => None,
        }
    }
    let mut registry = FunctionRegistry::empty();
    registry.register_1_arg::<UInt64Type, UInt64Type, _>(
        "range_identity",
        |_, _| FunctionDomain::Full,
        |value, _| value,
    );
    registry.properties.insert(
        "range_identity".to_string(),
        FunctionProperty::default().monotonicity_check(below_100),
    );
    for domain in [SimpleDomain { min: 10, max: 20 }, SimpleDomain {
        min: 10,
        max: 200,
    }] {
        run_fold_case(
            &mut file,
            "range_identity(value)",
            &[("value", DataType::Number(NumberDataType::UInt64))],
            &[("value", Domain::Number(NumberDomain::UInt64(domain)))],
            &registry,
        );
    }

    fn second_argument_if_first_is_constant(
        _ctx: &FunctionContext,
        args: &[Domain],
    ) -> Option<usize> {
        match args {
            [first, _] if first.as_singleton().is_some() => Some(1),
            _ => None,
        }
    }
    let mut registry = FunctionRegistry::empty();
    registry.register_2_arg::<UInt8Type, UInt8Type, UInt8Type, _>(
        "select_second",
        |_, _, _| FunctionDomain::Full,
        |_, value, _| value,
    );
    registry.properties.insert(
        "select_second".to_string(),
        FunctionProperty::default().monotonicity_check(second_argument_if_first_is_constant),
    );
    run_fold_case(
        &mut file,
        "select_second(7, value)",
        &[("value", DataType::Number(NumberDataType::UInt8))],
        &[(
            "value",
            Domain::Number(NumberDomain::UInt8(SimpleDomain { min: 10, max: 20 })),
        )],
        &registry,
    );
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
