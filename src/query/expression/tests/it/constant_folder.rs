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

use std::borrow::Cow;
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;

use databend_common_expression::BlockEntry;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataBlock;
use databend_common_expression::Domain;
use databend_common_expression::Evaluator;
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
use databend_common_expression::types::DecimalScalar;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::F32;
use databend_common_expression::types::F64;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberDomain;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::SimpleDomain;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::i256;
use databend_common_expression::types::nullable::NullableDomain;
use databend_common_expression::types::string::StringDomain;
use databend_common_expression_test_support::parse_raw_expr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use goldenfile::Mint;
use itertools::Itertools;

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
    ConstantFolder::fold(Cow::Borrowed(expr), &FunctionContext::default(), registry)
        .0
        .into_owned()
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
        Cow::Borrowed(&expr),
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

#[test]
fn test_fold_uses_supplied_context() {
    let mut registry = FunctionRegistry::empty();
    // Assert context forwarding in both callbacks without making the identity
    // function's result or domain depend on the context.
    registry.register_passthrough_nullable_1_arg::<UInt64Type, UInt64Type, _>(
        "identity",
        |ctx, _| {
            assert_eq!(ctx.week_start, 1);
            FunctionDomain::Full
        },
        |value, ctx| {
            assert_eq!(ctx.func_ctx.week_start, 1);
            value
        },
    );
    let expr = databend_common_expression::type_check::check_function(
        None,
        "identity",
        &[],
        &[Expr::<usize>::Constant(Constant {
            span: None,
            scalar: Scalar::Number(NumberScalar::UInt64(42)),
            data_type: DataType::Number(NumberDataType::UInt64),
        })],
        &registry,
    )
    .unwrap();
    let context = FunctionContext {
        week_start: 1,
        ..Default::default()
    };
    let (folded, _) = ConstantFolder::fold(Cow::Owned(expr), &context, &registry);
    assert_eq!(
        folded.as_constant().unwrap().scalar,
        Scalar::Number(NumberScalar::UInt64(42))
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

    let timestamp_columns = &[("value", DataType::String.wrap_nullable())];

    for has_null in [false, true] {
        let domains = &[(
            "value",
            Domain::Nullable(NullableDomain {
                has_null,
                value: Some(Box::new(Domain::String(StringDomain {
                    min: "2007-01-01".to_string(),
                    max: Some("2007-01-02".to_string()),
                }))),
            }),
        )];

        run_fold_case(
            &mut file,
            "to_timestamp(value)",
            timestamp_columns,
            domains,
            &BUILTIN_FUNCTIONS,
        );
        run_fold_case(
            &mut file,
            "CAST(value AS TIMESTAMP NULL)",
            timestamp_columns,
            domains,
            &BUILTIN_FUNCTIONS,
        );
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

    // Range-sensitive rule: monotonic only when the whole range lies below 100.
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

fn builtin_call(name: &str, args: &[Expr<usize>]) -> Expr<usize> {
    databend_common_expression::type_check::check_function(
        None,
        name,
        &[],
        args,
        &BUILTIN_FUNCTIONS,
    )
    .unwrap()
}

fn range_comparison(
    data_type: &DataType,
    operator: &str,
    value: &Scalar,
    flipped: bool,
) -> Expr<usize> {
    let column = Expr::ColumnRef(ColumnRef {
        span: None,
        id: 0,
        data_type: data_type.clone(),
        display_name: "x".to_string(),
    });
    let constant = Expr::Constant(Constant {
        span: None,
        scalar: value.clone(),
        data_type: value.as_ref().infer_data_type(),
    });
    if flipped {
        let operator = match operator {
            "gt" => "lt",
            "gte" => "lte",
            "lt" => "gt",
            "lte" => "gte",
            operator => operator,
        };
        builtin_call(operator, &[constant, column])
    } else {
        builtin_call(operator, &[column, constant])
    }
}

fn assert_range_fold(
    conjunction: &str,
    data_type: &DataType,
    constraints: &[(&str, Scalar)],
    candidates: &[Scalar],
    mutually_exclusive: bool,
) {
    let context = FunctionContext::default();
    // Check exclusions before/after bounds and equalities, and constants on
    // either side of the comparisons, using actual registered SQL functions.
    for permutation in constraints.iter().permutations(constraints.len()) {
        for flipped in [false, true] {
            let args = permutation
                .iter()
                .map(|(operator, value)| range_comparison(data_type, operator, value, flipped))
                .collect::<Vec<_>>();
            let expr = if conjunction == "and" {
                args.into_iter()
                    .reduce(|left, right| builtin_call("and", &[left, right]))
                    .unwrap()
            } else {
                builtin_call(conjunction, &args)
            };
            let folded = fold_with_registry(&expr, &BUILTIN_FUNCTIONS);
            assert_eq!(
                folded
                    .as_constant()
                    .is_some_and(|constant| constant.scalar == Scalar::Boolean(false)),
                mutually_exclusive,
                "{data_type:?}: {} -> {}",
                expr.sql_display(),
                folded.sql_display(),
            );
            for candidate in candidates {
                let block = DataBlock::new(
                    vec![BlockEntry::new_const_column(
                        data_type.clone(),
                        candidate.clone(),
                        1,
                    )],
                    1,
                );
                let evaluator = Evaluator::new(&block, &context, &BUILTIN_FUNCTIONS);
                let original = evaluator.run(&expr).unwrap();
                let optimized = evaluator.run(&folded).unwrap();
                assert_eq!(
                    original.convert_to_full_column(expr.data_type(), 1),
                    optimized.convert_to_full_column(folded.data_type(), 1),
                    "{} at {candidate:?}",
                    expr.sql_display(),
                );
            }
        }
    }
}

#[test]
fn test_range_constraints_bounds_equality_and_decimals() {
    let decimal64 = DecimalSize::new_unchecked(9, 2);
    let decimal128 = DecimalSize::new_unchecked(25, 4);
    let decimal256 = DecimalSize::new_unchecked(50, 6);
    let values = [0, 5, 10];
    let scalar_sets = [
        values.map(|value| Scalar::Number(NumberScalar::UInt64(value))),
        values.map(|value| Scalar::Decimal(DecimalScalar::Decimal64(value as i64, decimal64))),
        values.map(|value| Scalar::Decimal(DecimalScalar::Decimal128(value as i128, decimal128))),
        values.map(|value| {
            Scalar::Decimal(DecimalScalar::Decimal256(
                i256::from(value as i64),
                decimal256,
            ))
        }),
    ];
    // Values are indices into each typed scalar set: 1 = 5, 2 = 10.
    let cases: &[(&[(&str, usize)], bool)] = &[
        (&[("gte", 1), ("lte", 1)], false),
        (&[("gt", 1), ("lte", 1)], true),
        (&[("gte", 1), ("lt", 1)], true),
        (&[("gt", 1), ("lt", 1)], true),
        (&[("gte", 2), ("lte", 1)], true),
        (&[("gte", 1), ("gte", 2), ("lte", 1)], true),
        (&[("gte", 1), ("gt", 1), ("lte", 1)], true),
        (&[("lte", 2), ("lt", 2), ("gte", 2)], true),
        (&[("eq", 1), ("gte", 1), ("lte", 1)], false),
        (&[("eq", 1), ("gt", 1)], true),
        (&[("eq", 1), ("lt", 1)], true),
        (&[("eq", 1), ("gte", 2)], true),
        (&[("eq", 2), ("lte", 1)], true),
        (&[("eq", 1), ("eq", 2)], true),
        (&[("eq", 1), ("eq", 1)], false),
        (&[("noteq", 1), ("eq", 1)], true),
        (&[("noteq", 2), ("eq", 1)], false),
        (&[("noteq", 1), ("gte", 1), ("lte", 1)], true),
        (&[("noteq", 2), ("gte", 1), ("lte", 1)], false),
        (&[("noteq", 1), ("gte", 1), ("lte", 2)], false),
    ];
    for scalars in scalar_sets {
        let data_type = scalars[0].as_ref().infer_data_type();
        for &(case, mutually_exclusive) in cases {
            let constraints = case
                .iter()
                .map(|&(operator, index)| (operator, scalars[index].clone()))
                .collect::<Vec<_>>();
            assert_range_fold(
                "and_filters",
                &data_type,
                &constraints,
                &scalars,
                mutually_exclusive,
            );
            if constraints.len() == 2 {
                assert_range_fold(
                    "and",
                    &data_type,
                    &constraints,
                    &scalars,
                    mutually_exclusive,
                );
            }
        }
    }
}

#[test]
fn test_range_constraints_float_nan_infinity_and_signed_zero() {
    // SQL comparisons use OrderedFloat: NaN equals NaN and sorts above infinity.
    for scalars in [
        [f64::NEG_INFINITY, -0.0, 0.0, 5.0, f64::INFINITY, f64::NAN]
            .map(|value| Scalar::Number(NumberScalar::Float64(F64::from(value)))),
        [f32::NEG_INFINITY, -0.0, 0.0, 5.0, f32::INFINITY, f32::NAN]
            .map(|value| Scalar::Number(NumberScalar::Float32(F32::from(value)))),
    ] {
        let data_type = scalars[0].as_ref().infer_data_type();
        let cases: &[(&[(&str, usize)], bool)] = &[
            (&[("gte", 5), ("lte", 5)], false),
            (&[("gte", 5), ("lte", 4)], true),
            (&[("gte", 4), ("lte", 5)], false),
            (&[("eq", 5), ("eq", 5)], false),
            (&[("eq", 5), ("eq", 3)], true),
            // Domain folding removes x <= NaN as always true, so the remaining
            // lower bound/exclusion do not prove a singleton in this rule.
            (&[("noteq", 5), ("gte", 5), ("lte", 5)], false),
            (&[("noteq", 3), ("gte", 5), ("lte", 5)], false),
            (&[("gt", 0), ("lte", 0)], true),
            (&[("gte", 4), ("lte", 4)], false),
            (&[("eq", 1), ("eq", 2)], false),
            (&[("gte", 1), ("lt", 2)], true),
            (&[("noteq", 1), ("gte", 2), ("lte", 2)], true),
        ];
        for &(case, mutually_exclusive) in cases {
            let constraints = case
                .iter()
                .map(|&(operator, index)| (operator, scalars[index].clone()))
                .collect::<Vec<_>>();
            assert_range_fold(
                "and_filters",
                &data_type,
                &constraints,
                &scalars,
                mutually_exclusive,
            );
        }
    }
}

#[test]
fn test_range_constraints_preserve_nullable_and() {
    let data_type = DataType::Number(NumberDataType::UInt64).wrap_nullable();
    let value = Scalar::Number(NumberScalar::UInt64(5));
    let candidates = [Scalar::Null, value.clone()];
    for constraints in [vec![("gte", value.clone()), ("lt", value.clone())], vec![
        ("noteq", value.clone()),
        ("gte", value.clone()),
        ("lte", value.clone()),
    ]] {
        assert_range_fold("and_filters", &data_type, &constraints, &candidates, true);
        assert_range_fold("and", &data_type, &constraints, &candidates, false);
    }
    let constraints = [("gte", value), ("lte", Scalar::Null)];
    assert_range_fold("and_filters", &data_type, &constraints, &candidates, true);
    assert_range_fold("and", &data_type, &constraints, &candidates, false);
}

#[test]
fn test_range_constraints_keep_columns_and_types_separate() {
    // Identical values/bounds on different columns must not be merged.
    for data_type in [
        DataType::Number(NumberDataType::UInt64),
        DataType::Number(NumberDataType::UInt64).wrap_nullable(),
    ] {
        let column = Expr::ColumnRef(ColumnRef {
            span: None,
            id: 1,
            data_type,
            display_name: "other".to_string(),
        });
        let args = [
            range_comparison(
                &DataType::Number(NumberDataType::UInt64),
                "gte",
                &Scalar::Number(NumberScalar::UInt64(10)),
                false,
            ),
            builtin_call("lte", &[column, uint_constant(5)]),
        ];
        assert!(
            fold_with_registry(&builtin_call("and_filters", &args), &BUILTIN_FUNCTIONS)
                .as_constant()
                .is_none()
        );
    }

    // String/number comparison functions use SQL conversion semantics, not
    // Scalar ordering. In particular, these two equalities are compatible.
    let columns = [("x", DataType::String)];
    for text in [
        "and_filters(eq(x, 1), eq(x, '1'))",
        "and_filters(eq(x, '1'), eq(x, 1))",
    ] {
        let raw = parse_raw_expr(text, &columns, &BUILTIN_FUNCTIONS);
        let expr = databend_common_expression::type_check::check(&raw, &BUILTIN_FUNCTIONS).unwrap();
        let folded = fold_with_registry(&expr, &BUILTIN_FUNCTIONS);
        assert!(folded.as_constant().is_none(), "{text}");
        let block = DataBlock::new(
            vec![BlockEntry::new_const_column(
                DataType::String,
                Scalar::String("1".to_string()),
                1,
            )],
            1,
        );
        let context = FunctionContext::default();
        let evaluator = Evaluator::new(&block, &context, &BUILTIN_FUNCTIONS);
        assert_eq!(
            evaluator.run(&folded).unwrap(),
            Value::Scalar(Scalar::Boolean(true))
        );
    }
}

#[test]
fn test_range_constraints_many_conditions() {
    let data_type = DataType::Number(NumberDataType::UInt64);
    let args = (0..3000)
        .map(|value| {
            range_comparison(
                &data_type,
                "noteq",
                &Scalar::Number(NumberScalar::UInt64(value)),
                false,
            )
        })
        .collect::<Vec<_>>();
    let satisfiable = builtin_call("and_filters", &args);
    assert!(
        fold_with_registry(&satisfiable, &BUILTIN_FUNCTIONS)
            .as_constant()
            .is_none()
    );

    // The excluded value is encountered well before the final singleton bounds.
    let value = Scalar::Number(NumberScalar::UInt64(1500));
    let mut args = args;
    args.push(range_comparison(&data_type, "gte", &value, false));
    args.push(range_comparison(&data_type, "lte", &value, false));
    let contradictory = builtin_call("and_filters", &args);
    assert_eq!(
        fold_with_registry(&contradictory, &BUILTIN_FUNCTIONS)
            .as_constant()
            .unwrap()
            .scalar,
        Scalar::Boolean(false),
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
