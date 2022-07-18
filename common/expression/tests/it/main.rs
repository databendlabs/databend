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

#![feature(box_patterns)]
#![feature(try_blocks)]

mod parser;

use std::io::Write;
use std::iter::once;
use std::sync::Arc;

use comfy_table::Table;
use common_ast::DisplayError;
use common_expression::chunk::Chunk;
use common_expression::evaluator::DomainCalculator;
use common_expression::evaluator::Evaluator;
use common_expression::function::vectorize_2_arg;
use common_expression::function::Function;
use common_expression::function::FunctionContext;
use common_expression::function::FunctionRegistry;
use common_expression::function::FunctionSignature;
use common_expression::property::BooleanDomain;
use common_expression::property::Domain;
use common_expression::property::FunctionProperty;
use common_expression::property::IntDomain;
use common_expression::property::NullableDomain;
use common_expression::property::StringDomain;
use common_expression::property::UIntDomain;
use common_expression::type_check;
use common_expression::types::ArrayType;
use common_expression::types::DataType;
use common_expression::types::*;
use common_expression::values::Column;
use common_expression::values::ColumnBuilder;
use common_expression::values::Scalar;
use common_expression::values::Value;
use common_expression::values::ValueRef;
use goldenfile::Mint;
use parser::parse_raw_expr;

#[test]
pub fn test_pass() {
    let mut mint = Mint::new("tests/it/testdata");
    let mut file = mint.new_goldenfile("run-pass.txt").unwrap();

    run_ast(&mut file, "bool_and(true, false)", &[]);
    run_ast(&mut file, "bool_and(null, false)", &[]);
    run_ast(&mut file, "plus(a, 10)", &[(
        "a",
        DataType::Nullable(Box::new(DataType::UInt8)),
        Domain::Nullable(NullableDomain {
            has_null: true,
            value: Some(Box::new(Domain::UInt(UIntDomain { min: 10, max: 12 }))),
        }),
        Column::Nullable {
            column: Box::new(Column::UInt8(vec![10, 11, 12].into())),
            validity: vec![false, true, false].into(),
        },
    )]);
    run_ast(&mut file, "plus(a, b)", &[
        (
            "a",
            DataType::Nullable(Box::new(DataType::UInt8)),
            Domain::Nullable(NullableDomain {
                has_null: true,
                value: Some(Box::new(Domain::UInt(UIntDomain { min: 10, max: 12 }))),
            }),
            Column::Nullable {
                column: Box::new(Column::UInt8(vec![10, 11, 12].into())),
                validity: vec![false, true, false].into(),
            },
        ),
        (
            "b",
            DataType::Nullable(Box::new(DataType::UInt8)),
            Domain::Nullable(NullableDomain {
                has_null: true,
                value: Some(Box::new(Domain::UInt(UIntDomain { min: 1, max: 3 }))),
            }),
            Column::Nullable {
                column: Box::new(Column::UInt8(vec![1, 2, 3].into())),
                validity: vec![false, true, true].into(),
            },
        ),
    ]);
    run_ast(&mut file, "plus(a, b)", &[
        (
            "a",
            DataType::Nullable(Box::new(DataType::UInt8)),
            Domain::Nullable(NullableDomain {
                has_null: true,
                value: Some(Box::new(Domain::UInt(UIntDomain { min: 10, max: 12 }))),
            }),
            Column::Nullable {
                column: Box::new(Column::UInt8(vec![10, 11, 12].into())),
                validity: vec![false, true, false].into(),
            },
        ),
        (
            "b",
            DataType::Null,
            Domain::Nullable(NullableDomain {
                has_null: true,
                value: None,
            }),
            Column::Null { len: 3 },
        ),
    ]);
    run_ast(&mut file, "bool_not(a)", &[(
        "a",
        DataType::Nullable(Box::new(DataType::Boolean)),
        Domain::Nullable(NullableDomain {
            has_null: true,
            value: Some(Box::new(Domain::Boolean(BooleanDomain {
                has_false: true,
                has_true: true,
            }))),
        }),
        Column::Nullable {
            column: Box::new(Column::Boolean(vec![true, false, true].into())),
            validity: vec![false, true, false].into(),
        },
    )]);
    run_ast(&mut file, "bool_not(a)", &[(
        "a",
        DataType::Null,
        Domain::Nullable(NullableDomain {
            has_null: true,
            value: None,
        }),
        Column::Null { len: 5 },
    )]);
    run_ast(&mut file, "least(10, 20, 30, 40)", &[]);
    run_ast(&mut file, "create_tuple(null, true)", &[]);
    run_ast(&mut file, "get_tuple(1)(create_tuple(a, b))", &[
        (
            "a",
            DataType::Int16,
            Domain::Int(IntDomain { min: 0, max: 4 }),
            Column::Int16(vec![0, 1, 2, 3, 4].into()),
        ),
        (
            "b",
            DataType::Nullable(Box::new(DataType::String)),
            Domain::Nullable(NullableDomain {
                has_null: true,
                value: Some(Box::new(Domain::String(StringDomain {
                    min: "a".as_bytes().to_vec(),
                    max: Some("e".as_bytes().to_vec()),
                }))),
            }),
            Column::Nullable {
                column: Box::new(Column::String {
                    data: "abcde".as_bytes().to_vec().into(),
                    offsets: vec![0, 1, 2, 3, 4, 5].into(),
                }),
                validity: vec![true, true, false, false, false].into(),
            },
        ),
    ]);
    run_ast(&mut file, "get_tuple(1)(create_tuple(a, b))", &[
        (
            "a",
            DataType::Nullable(Box::new(DataType::Boolean)),
            Domain::Nullable(NullableDomain {
                has_null: true,
                value: Some(Box::new(Domain::Boolean(BooleanDomain {
                    has_false: true,
                    has_true: false,
                }))),
            }),
            Column::Nullable {
                column: Box::new(Column::Boolean(vec![false; 5].into())),
                validity: vec![true, true, false, false, false].into(),
            },
        ),
        (
            "b",
            DataType::Nullable(Box::new(DataType::String)),
            Domain::Nullable(NullableDomain {
                has_null: true,
                value: Some(Box::new(Domain::String(StringDomain {
                    min: "a".as_bytes().to_vec(),
                    max: Some("e".as_bytes().to_vec()),
                }))),
            }),
            Column::Nullable {
                column: Box::new(Column::String {
                    data: "abcde".as_bytes().to_vec().into(),
                    offsets: vec![0, 1, 2, 3, 4, 5].into(),
                }),
                validity: vec![true, true, false, false, false].into(),
            },
        ),
    ]);
    run_ast(&mut file, "create_array()", &[]);
    run_ast(&mut file, "create_array(null, true)", &[]);
    run_ast(&mut file, "create_array(a, b)", &[
        (
            "a",
            DataType::Int16,
            Domain::Int(IntDomain { min: 0, max: 4 }),
            Column::Int16(vec![0, 1, 2, 3, 4].into()),
        ),
        (
            "b",
            DataType::Int16,
            Domain::Int(IntDomain { min: 5, max: 9 }),
            Column::Int16(vec![5, 6, 7, 8, 9].into()),
        ),
    ]);
    run_ast(
        &mut file,
        "create_array(create_array(a, b), null, null)",
        &[
            (
                "a",
                DataType::Int16,
                Domain::Int(IntDomain { min: 0, max: 4 }),
                Column::Int16(vec![0, 1, 2, 3, 4].into()),
            ),
            (
                "b",
                DataType::Int16,
                Domain::Int(IntDomain { min: 5, max: 9 }),
                Column::Int16(vec![5, 6, 7, 8, 9].into()),
            ),
        ],
    );
    run_ast(&mut file, "get(a, b)", &[
        (
            "a",
            DataType::Array(Box::new(DataType::Int16)),
            Domain::Array(Some(Box::new(Domain::Int(IntDomain { min: 0, max: 99 })))),
            Column::Array {
                array: Box::new(Column::Int16((0..100).collect())),
                offsets: vec![0, 20, 40, 60, 80, 100].into(),
            },
        ),
        (
            "b",
            DataType::UInt8,
            Domain::UInt(UIntDomain { min: 0, max: 4 }),
            Column::UInt8(vec![0, 1, 2, 3, 4].into()),
        ),
    ]);
    run_ast(&mut file, "get(a, b)", &[
        (
            "a",
            DataType::Array(Box::new(DataType::Array(Box::new(DataType::Int16)))),
            Domain::Array(Some(Box::new(Domain::Array(Some(Box::new(Domain::Int(
                IntDomain { min: 0, max: 99 },
            ))))))),
            Column::Array {
                array: Box::new(Column::Array {
                    array: Box::new(Column::Int16((0..100).collect())),
                    offsets: vec![
                        0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90,
                        95, 100,
                    ]
                    .into(),
                }),
                offsets: vec![0, 4, 8, 11, 15, 20].into(),
            },
        ),
        (
            "b",
            DataType::UInt8,
            Domain::UInt(UIntDomain { min: 0, max: 4 }),
            Column::UInt8(vec![0, 1, 2, 3, 4].into()),
        ),
    ]);
}

#[test]
pub fn test_tyck_fail() {
    let mut mint = Mint::new("tests/it/testdata");
    let mut file = mint.new_goldenfile("tyck-fail.txt").unwrap();

    run_ast(&mut file, "bool_and(true, 1)", &[]);
    run_ast(&mut file, "bool_and()", &[]);
    run_ast(&mut file, "bool_not(bool_not('a'))", &[]);
    run_ast(&mut file, "least(1, 2, 3, a)", &[(
        "a",
        DataType::Boolean,
        Domain::Boolean(BooleanDomain {
            has_false: true,
            has_true: false,
        }),
        Column::Boolean(vec![false; 3].into()),
    )]);
    run_ast(&mut file, "create_array('a', 1)", &[]);
    run_ast(&mut file, "create_array('a', null, 'b', true)", &[]);
    run_ast(&mut file, "get(create_array(1, 2), 'a')", &[]);
    run_ast(&mut file, "get_tuple(1)(create_tuple(true))", &[]);
}

#[test]
pub fn test_eval_fail() {
    let mut mint = Mint::new("tests/it/testdata");
    let mut file = mint.new_goldenfile("eval-fail.txt").unwrap();

    run_ast(&mut file, "get(create_array(1, 2), 2)", &[]);
    run_ast(&mut file, "get(create_array(a, b), idx)", &[
        (
            "a",
            DataType::Int16,
            Domain::Int(IntDomain { min: 0, max: 4 }),
            Column::Int16(vec![0, 1, 2, 3, 4].into()),
        ),
        (
            "b",
            DataType::Int16,
            Domain::Int(IntDomain { min: 5, max: 9 }),
            Column::Int16(vec![5, 6, 7, 8, 9].into()),
        ),
        (
            "idx",
            DataType::Int16,
            Domain::Int(IntDomain { min: 0, max: 4 }),
            Column::Int16(vec![0, 1, 2, 3, 4].into()),
        ),
    ]);
}

fn builtin_functions() -> FunctionRegistry {
    let mut registry = FunctionRegistry::default();

    registry.register_2_arg::<BooleanType, BooleanType, BooleanType, _, _>(
        "bool_and",
        FunctionProperty::default(),
        |lhs, rhs| {
            Some(BooleanDomain {
                has_false: lhs.has_false || rhs.has_false,
                has_true: lhs.has_true && rhs.has_true,
            })
        },
        |lhs, rhs| lhs && rhs,
    );

    registry.register_2_arg::<NumberType<i16>, NumberType<i16>, NumberType<i16>, _, _>(
        "plus",
        FunctionProperty::default(),
        |lhs, rhs| {
            Some(IntDomain {
                min: lhs.min.checked_add(rhs.min).unwrap_or(i16::MAX as i64),
                max: lhs.max.checked_add(rhs.max).unwrap_or(i16::MAX as i64),
            })
        },
        |lhs, rhs| lhs + rhs,
    );

    registry.register_1_arg::<BooleanType, BooleanType, _, _>(
        "bool_not",
        FunctionProperty::default(),
        |arg| {
            Some(BooleanDomain {
                has_false: arg.has_true,
                has_true: arg.has_false,
            })
        },
        |val| !val,
    );

    registry.register_function_factory("least", |_, args_type| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "least",
                args_type: vec![DataType::Int16; args_type.len()],
                return_type: DataType::Int16,
                property: FunctionProperty::default().commutative(true),
            },
            calc_domain: Box::new(|args_domain, _| {
                let min = args_domain
                    .iter()
                    .map(|domain| domain.as_int().unwrap().min)
                    .min()
                    .unwrap_or(0);
                let max = args_domain
                    .iter()
                    .map(|domain| domain.as_int().unwrap().max)
                    .min()
                    .unwrap_or(0);
                Domain::Int(IntDomain { min, max })
            }),
            eval: Box::new(|args, generics| {
                if args.is_empty() {
                    Ok(Value::Scalar(Scalar::Int16(0)))
                } else if args.len() == 1 {
                    Ok(args[0].clone().to_owned())
                } else {
                    let mut min =
                        vectorize_2_arg::<NumberType<i16>, NumberType<i16>, NumberType<i16>>(
                            |lhs, rhs| lhs.min(rhs),
                        )(
                            args[0].try_downcast().unwrap(),
                            args[1].try_downcast().unwrap(),
                            generics,
                        )?;
                    for arg in &args[2..] {
                        min = vectorize_2_arg::<NumberType<i16>, NumberType<i16>, NumberType<i16>>(
                            |lhs, rhs| lhs.min(rhs),
                        )(
                            min.as_ref(), arg.try_downcast().unwrap(), generics
                        )?;
                    }
                    Ok(min.upcast())
                }
            }),
        }))
    });

    registry.register_0_arg_core::<EmptyArrayType, _, _>(
        "create_array",
        FunctionProperty::default(),
        || None,
        |_| Ok(Value::Scalar(())),
    );

    registry.register_function_factory("create_array", |_, args_type| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "create_array",
                args_type: vec![DataType::Generic(0); args_type.len()],
                return_type: DataType::Array(Box::new(DataType::Generic(0))),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|args_domain, _| {
                args_domain.iter().fold(Domain::Array(None), |acc, x| {
                    acc.merge(&Domain::Array(Some(Box::new(x.clone()))))
                })
            }),
            eval: Box::new(|args, generics| {
                let len = args.iter().find_map(|arg| match arg {
                    ValueRef::Column(col) => Some(col.len()),
                    _ => None,
                });
                if let Some(len) = len {
                    let mut array_builder = ColumnBuilder::with_capacity(&generics[0], 0);
                    for idx in 0..len {
                        for arg in args {
                            match arg {
                                ValueRef::Scalar(scalar) => {
                                    array_builder.push(scalar.as_ref());
                                }
                                ValueRef::Column(col) => {
                                    array_builder.push(col.index(idx).unwrap());
                                }
                            }
                        }
                    }
                    let offsets = once(0)
                        .chain((0..len).map(|row| (args.len() * (row + 1)) as u64))
                        .collect();
                    Ok(Value::Column(Column::Array {
                        array: Box::new(array_builder.build()),
                        offsets,
                    }))
                } else {
                    // All args are scalars, so we return a scalar as result
                    let mut array = ColumnBuilder::with_capacity(&generics[0], 0);
                    for arg in args {
                        match arg {
                            ValueRef::Scalar(scalar) => {
                                array.push(scalar.as_ref());
                            }
                            ValueRef::Column(_) => unreachable!(),
                        }
                    }
                    Ok(Value::Scalar(Scalar::Array(array.build())))
                }
            }),
        }))
    });

    registry.register_with_writer_2_arg::<ArrayType<GenericType<0>>, NumberType<i16>, GenericType<0>,_, _>(
        "get",
        FunctionProperty::default(),
        |item_domain, _| Some(item_domain.clone()),
        |array, idx, output| {
            let item = array
                .index(idx as usize)
                .ok_or_else(|| format!("index out of bounds: the len is {} but the index is {}", array.len(), idx))?;
            output.push(item);
            Ok(())
        },
    );

    registry.register_function_factory("create_tuple", |_, args_type| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "create_tuple",
                args_type: args_type.to_vec(),
                return_type: DataType::Tuple(args_type.to_vec()),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|args_domain, _| Domain::Tuple(args_domain.to_vec())),
            eval: Box::new(move |args, _generics| {
                let len = args.iter().find_map(|arg| match arg {
                    ValueRef::Column(col) => Some(col.len()),
                    _ => None,
                });
                if let Some(len) = len {
                    let fields = args
                        .iter()
                        .map(|arg| match arg {
                            ValueRef::Scalar(scalar) => scalar.as_ref().repeat(len).build(),
                            ValueRef::Column(col) => col.clone(),
                        })
                        .collect();
                    Ok(Value::Column(Column::Tuple { fields, len }))
                } else {
                    // All args are scalars, so we return a scalar as result
                    let fields = args
                        .iter()
                        .map(|arg| match arg {
                            ValueRef::Scalar(scalar) => (*scalar).to_owned(),
                            ValueRef::Column(_) => unreachable!(),
                        })
                        .collect();
                    Ok(Value::Scalar(Scalar::Tuple(fields)))
                }
            }),
        }))
    });

    registry.register_function_factory("get_tuple", |params, args_type| {
        let idx = *params.first()?;
        let tuple_tys = match args_type.get(0) {
            Some(DataType::Tuple(tys)) => tys,
            _ => return None,
        };
        if idx >= tuple_tys.len() {
            return None;
        }

        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "get_tuple",
                args_type: vec![DataType::Tuple(tuple_tys.to_vec())],
                return_type: tuple_tys[idx].clone(),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(move |args_domain, _| {
                args_domain[0].as_tuple().unwrap()[idx].clone()
            }),
            eval: Box::new(move |args, _| match &args[0] {
                ValueRef::Scalar(Scalar::Tuple(fields)) => {
                    Ok(Value::Scalar(fields[idx].to_owned()))
                }
                ValueRef::Column(Column::Tuple { fields, .. }) => {
                    Ok(Value::Column(fields[idx].to_owned()))
                }
                _ => unreachable!(),
            }),
        }))
    });

    registry.register_function_factory("get_tuple", |params, args_type| {
        let idx = *params.first()?;
        let tuple_tys = match args_type.get(0) {
            Some(DataType::Nullable(box DataType::Tuple(tys))) => tys,
            _ => return None,
        };
        if idx >= tuple_tys.len() {
            return None;
        }

        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "get_tuple",
                args_type: vec![DataType::Nullable(Box::new(DataType::Tuple(
                    tuple_tys.to_vec(),
                )))],
                return_type: DataType::Nullable(Box::new(tuple_tys[idx].clone())),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(move |args_domain, _| {
                let NullableDomain { has_null, value } = args_domain[0].as_nullable().unwrap();
                let value = value.as_ref().map(|value| {
                    let fields = value.as_tuple().unwrap();
                    Box::new(fields[idx].clone())
                });
                Domain::Nullable(NullableDomain {
                    has_null: *has_null,
                    value,
                })
            }),
            eval: Box::new(move |args, _| match &args[0] {
                ValueRef::Scalar(Scalar::Null) => Ok(Value::Scalar(Scalar::Null)),
                ValueRef::Scalar(Scalar::Tuple(fields)) => {
                    Ok(Value::Scalar(fields[idx].to_owned()))
                }
                ValueRef::Column(Column::Nullable {
                    column: box Column::Tuple { fields, .. },
                    validity,
                }) => Ok(Value::Column(Column::Nullable {
                    column: Box::new(fields[idx].to_owned()),
                    validity: validity.clone(),
                })),
                _ => unreachable!(),
            }),
        }))
    });

    registry
}

fn run_ast(file: &mut impl Write, text: &str, columns: &[(&str, DataType, Domain, Column)]) {
    let result = try {
        let raw_expr = parse_raw_expr(
            text,
            &columns
                .iter()
                .map(|(name, ty, _, _)| (*name, ty.clone()))
                .collect::<Vec<_>>(),
        );

        let fn_registry = builtin_functions();
        let (expr, output_ty) = type_check::check(&raw_expr, &fn_registry)?;

        let domain_calculator = DomainCalculator {
            input_domains: columns
                .iter()
                .map(|(_, _, domain, _)| domain.clone())
                .collect::<Vec<_>>(),
        };
        let output_domain = domain_calculator.calculate(&expr)?;

        let chunk = Chunk::new(
            columns
                .iter()
                .map(|(_, _, _, col)| col.clone())
                .collect::<Vec<_>>(),
        );
        let evaluator = Evaluator {
            input_columns: chunk,
            context: FunctionContext::default(),
        };
        let result = evaluator.run(&expr)?;

        (raw_expr, expr, output_ty, output_domain, result)
    };

    match result {
        Ok((raw_expr, expr, output_ty, output_domain, result)) => {
            writeln!(file, "ast            : {text}").unwrap();
            writeln!(file, "raw expr       : {raw_expr}").unwrap();
            writeln!(file, "checked expr   : {expr}").unwrap();

            match result {
                Value::Scalar(output_scalar) => {
                    writeln!(file, "output type    : {output_ty}").unwrap();
                    writeln!(file, "output domain  : {output_domain}").unwrap();
                    writeln!(file, "output         : {}", output_scalar.as_ref()).unwrap();
                }
                Value::Column(output_col) => {
                    let mut table = Table::new();
                    table.load_preset("||--+-++|    ++++++");

                    let mut header = vec!["".to_string()];
                    header.extend(columns.iter().map(|(name, _, _, _)| name.to_string()));
                    header.push("Output".to_string());
                    table.set_header(header);

                    let mut type_row = vec!["Type".to_string()];
                    type_row.extend(columns.iter().map(|(_, ty, _, _)| ty.to_string()));
                    type_row.push(output_ty.to_string());
                    table.add_row(type_row);

                    let mut domain_row = vec!["Domain".to_string()];
                    domain_row.extend(columns.iter().map(|(_, _, domain, _)| domain.to_string()));
                    domain_row.push(output_domain.to_string());
                    table.add_row(domain_row);

                    for i in 0..output_col.len() {
                        let mut row = vec![format!("Row {i}")];
                        for (_, _, _, col) in columns.iter() {
                            let value = col.index(i).unwrap();
                            row.push(format!("{}", value));
                        }
                        row.push(format!("{}", output_col.index(i).unwrap()));
                        table.add_row(row);
                    }

                    writeln!(file, "evaluation:\n{table}").unwrap();

                    let mut table = Table::new();
                    table.load_preset("||--+-++|    ++++++");

                    table.set_header(&["Column", "Data"]);

                    for (name, _, _, col) in columns.iter() {
                        table.add_row(&[name.to_string(), format!("{col:?}")]);
                    }

                    table.add_row(["Output".to_string(), format!("{output_col:?}")]);

                    writeln!(file, "evaluation (internal):\n{table}").unwrap();
                }
            }
            write!(file, "\n\n").unwrap();
        }
        Err((Some(span), msg)) => {
            writeln!(file, "{}\n", span.display_error((text.to_string(), msg))).unwrap();
        }
        Err((None, msg)) => {
            writeln!(file, "error: {}\n", msg).unwrap();
        }
    }
}
