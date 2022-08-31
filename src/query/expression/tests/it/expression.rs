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

use std::io::Write;
use std::iter::once;
use std::sync::Arc;

use comfy_table::Table;
use common_ast::DisplayError;
use common_expression::type_check;
use common_expression::types::array::ArrayColumn;
use common_expression::types::nullable::NullableColumn;
use common_expression::types::ArrayType;
use common_expression::types::DataType;
use common_expression::types::*;
use common_expression::vectorize_2_arg;
use common_expression::vectorize_with_builder_2_arg;
use common_expression::BooleanDomain;
use common_expression::Chunk;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::ColumnFrom;
use common_expression::ConstantFolder;
use common_expression::Domain;
use common_expression::Evaluator;
use common_expression::Function;
use common_expression::FunctionContext;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;
use common_expression::FunctionSignature;
use common_expression::NullableDomain;
use common_expression::NumberDomain;
use common_expression::RemoteExpr;
use common_expression::Scalar;
use common_expression::ScalarRef;
use common_expression::Value;
use common_expression::ValueRef;
use goldenfile::Mint;

use crate::parser::parse_raw_expr;

// Deprecate: move tests to `common_function_v2`
#[test]
pub fn test_pass() {
    let mut mint = Mint::new("tests/it/testdata");
    let mut file = mint.new_goldenfile("run-pass.txt").unwrap();

    run_ast(&mut file, "true AND false", &[]);
    run_ast(&mut file, "CAST(false AS BOOLEAN NULL)", &[]);
    run_ast(&mut file, "null AND false", &[]);
    run_ast(&mut file, "plus(a, 10)", &[(
        "a",
        DataType::Nullable(Box::new(DataType::UInt8)),
        Column::from_data_with_validity(vec![10u8, 11, 12], vec![false, true, false]),
    )]);
    run_ast(&mut file, "plus(a, b)", &[
        (
            "a",
            DataType::Nullable(Box::new(DataType::UInt8)),
            Column::from_data_with_validity(vec![10u8, 11, 12], vec![false, true, false]),
        ),
        (
            "b",
            DataType::Nullable(Box::new(DataType::UInt8)),
            Column::from_data_with_validity(vec![1u8, 2, 3], vec![false, true, true]),
        ),
    ]);
    run_ast(&mut file, "plus(a, b)", &[
        (
            "a",
            DataType::Nullable(Box::new(DataType::UInt8)),
            Column::from_data_with_validity(vec![10u8, 11, 12], vec![false, true, false]),
        ),
        ("b", DataType::Null, Column::Null { len: 3 }),
    ]);

    run_ast(&mut file, "minus(a, 10)", &[(
        "a",
        DataType::Nullable(Box::new(DataType::UInt8)),
        Column::from_data_with_validity(vec![10u8, 11, 12], vec![false, true, false]),
    )]);

    run_ast(&mut file, "minus(a, b)", &[
        (
            "a",
            DataType::Nullable(Box::new(DataType::UInt16)),
            Column::from_data_with_validity(vec![10u16, 11, 12], vec![false, true, false]),
        ),
        (
            "b",
            DataType::Nullable(Box::new(DataType::Int16)),
            Column::from_data_with_validity(vec![1i16, 2, 3], vec![false, true, true]),
        ),
    ]);

    run_ast(&mut file, "minus(a, b)", &[
        (
            "a",
            DataType::Nullable(Box::new(DataType::UInt8)),
            Column::from_data_with_validity(vec![10u16, 11, 12], vec![false, true, false]),
        ),
        ("b", DataType::Null, Column::Null { len: 3 }),
    ]);

    run_ast(&mut file, "multiply(a, 10)", &[(
        "a",
        DataType::Nullable(Box::new(DataType::UInt8)),
        Column::from_data_with_validity(vec![10u8, 11, 12], vec![false, true, false]),
    )]);

    run_ast(&mut file, "multiply(a, b)", &[
        (
            "a",
            DataType::Nullable(Box::new(DataType::UInt16)),
            Column::from_data_with_validity(vec![10u16, 11, 12], vec![false, true, false]),
        ),
        (
            "b",
            DataType::Nullable(Box::new(DataType::Int16)),
            Column::from_data_with_validity(vec![1i16, 2, 3], vec![false, true, true]),
        ),
    ]);

    run_ast(&mut file, "multiply(a, b)", &[
        (
            "a",
            DataType::Nullable(Box::new(DataType::UInt32)),
            Column::from_data_with_validity(vec![10u32, 11, 12], vec![false, true, false]),
        ),
        (
            "b",
            DataType::Nullable(Box::new(DataType::Int32)),
            Column::from_data_with_validity(vec![1i32, 2, 3], vec![false, true, true]),
        ),
    ]);

    run_ast(&mut file, "multiply(a, b)", &[
        (
            "a",
            DataType::Nullable(Box::new(DataType::UInt8)),
            Column::from_data_with_validity(vec![10u8, 11, 12], vec![false, true, false]),
        ),
        ("b", DataType::Null, Column::Null { len: 3 }),
    ]);

    run_ast(&mut file, "divide(a, 10)", &[(
        "a",
        DataType::Nullable(Box::new(DataType::UInt8)),
        Column::from_data_with_validity(vec![10u8, 11, 12], vec![false, true, false]),
    )]);

    run_ast(&mut file, "divide(a, b)", &[
        (
            "a",
            DataType::Nullable(Box::new(DataType::UInt16)),
            Column::from_data_with_validity(vec![10u16, 11, 12], vec![false, true, false]),
        ),
        (
            "b",
            DataType::Nullable(Box::new(DataType::Int16)),
            Column::from_data_with_validity(vec![1i16, 2, 3], vec![false, true, true]),
        ),
    ]);

    run_ast(&mut file, "divide(a, b)", &[
        (
            "a",
            DataType::Nullable(Box::new(DataType::UInt8)),
            Column::from_data_with_validity(vec![10u8, 11, 12], vec![false, true, false]),
        ),
        ("b", DataType::Null, Column::Null { len: 3 }),
    ]);

    run_ast(&mut file, "avg(a, 10)", &[(
        "a",
        DataType::Nullable(Box::new(DataType::UInt8)),
        Column::from_data_with_validity(vec![10u8, 11, 12], vec![false, true, false]),
    )]);

    run_ast(&mut file, "avg(a, b)", &[
        (
            "a",
            DataType::Nullable(Box::new(DataType::UInt16)),
            Column::from_data_with_validity(vec![10u16, 11, 12], vec![false, true, false]),
        ),
        (
            "b",
            DataType::Nullable(Box::new(DataType::Int16)),
            Column::from_data_with_validity(vec![1i16, 2, 3], vec![false, true, true]),
        ),
    ]);

    run_ast(&mut file, "avg(a, b)", &[
        (
            "a",
            DataType::Nullable(Box::new(DataType::UInt32)),
            Column::from_data_with_validity(vec![10u32, 11, 12], vec![false, true, false]),
        ),
        (
            "b",
            DataType::Nullable(Box::new(DataType::Int32)),
            Column::from_data_with_validity(vec![1i32, 2, 3], vec![false, true, true]),
        ),
    ]);

    run_ast(&mut file, "avg(a, b)", &[
        (
            "a",
            DataType::Nullable(Box::new(DataType::Float32)),
            Column::from_data_with_validity(vec![10f32, 11f32, 12f32], vec![false, true, false]),
        ),
        (
            "b",
            DataType::Nullable(Box::new(DataType::Int32)),
            Column::from_data_with_validity(vec![1i32, 2, 3], vec![false, true, true]),
        ),
    ]);

    run_ast(&mut file, "avg(a, b)", &[
        (
            "a",
            DataType::Nullable(Box::new(DataType::Float32)),
            Column::from_data_with_validity(vec![10f32, 11f32, 12f32], vec![false, true, false]),
        ),
        (
            "b",
            DataType::Nullable(Box::new(DataType::Float64)),
            Column::from_data_with_validity(vec![1f64, 2f64, 3f64], vec![false, true, true]),
        ),
    ]);

    run_ast(&mut file, "multiply(a, b)", &[
        (
            "a",
            DataType::Nullable(Box::new(DataType::Int8)),
            Column::from_data_with_validity(vec![10, 11, 12], vec![false, true, false]),
        ),
        ("b", DataType::Null, Column::Null { len: 3 }),
    ]);

    run_ast(&mut file, "NOT a", &[(
        "a",
        DataType::Nullable(Box::new(DataType::Boolean)),
        Column::from_data_with_validity(vec![true, false, true], vec![false, true, false]),
    )]);

    run_ast(&mut file, "NOT a", &[("a", DataType::Null, Column::Null {
        len: 5,
    })]);
    run_ast(&mut file, "least(10, CAST(20 as Int8), 30, 40)", &[]);
    run_ast(&mut file, "create_tuple(null, true)", &[]);
    run_ast(&mut file, "get_tuple(1)(create_tuple(a, b))", &[
        (
            "a",
            DataType::Int16,
            Column::from_data(vec![0i16, 1, 2, 3, 4]),
        ),
        (
            "b",
            DataType::Nullable(Box::new(DataType::String)),
            Column::from_data_with_validity(vec!["a", "b", "c", "d", "e"], vec![
                true, true, false, false, false,
            ]),
        ),
    ]);
    run_ast(&mut file, "get_tuple(1)(create_tuple(a, b))", &[
        (
            "a",
            DataType::Nullable(Box::new(DataType::Boolean)),
            Column::from_data_with_validity(vec![false; 5], vec![true, true, false, false, false]),
        ),
        (
            "b",
            DataType::Nullable(Box::new(DataType::String)),
            Column::from_data_with_validity(vec!["a", "b", "c", "d", "e"], vec![
                true, true, false, false, false,
            ]),
        ),
    ]);
    run_ast(&mut file, "create_array()", &[]);
    run_ast(&mut file, "create_array(null, true)", &[]);
    run_ast(&mut file, "create_array(a, b)", &[
        (
            "a",
            DataType::Int16,
            Column::from_data(vec![0i16, 1, 2, 3, 4]),
        ),
        (
            "b",
            DataType::Int16,
            Column::from_data(vec![5i16, 6, 7, 8, 9]),
        ),
    ]);
    run_ast(
        &mut file,
        "create_array(create_array(a, b), null, null)",
        &[
            (
                "a",
                DataType::Int16,
                Column::from_data(vec![0i16, 1, 2, 3, 4]),
            ),
            (
                "b",
                DataType::Int16,
                Column::from_data(vec![5i16, 6, 7, 8, 9]),
            ),
        ],
    );
    run_ast(&mut file, "get(a, b)", &[
        (
            "a",
            DataType::Array(Box::new(DataType::Int16)),
            Column::Array(Box::new(ArrayColumn {
                values: Column::Int16((0..100).collect()),
                offsets: vec![0, 20, 40, 60, 80, 100].into(),
            })),
        ),
        (
            "b",
            DataType::UInt8,
            Column::from_data(vec![0u8, 1, 2, 3, 4]),
        ),
    ]);
    run_ast(&mut file, "get(a, b)", &[
        (
            "a",
            DataType::Array(Box::new(DataType::Array(Box::new(DataType::Int16)))),
            Column::Array(Box::new(ArrayColumn {
                values: Column::Array(Box::new(ArrayColumn {
                    values: Column::Int16((0..100).collect()),
                    offsets: vec![
                        0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90,
                        95, 100,
                    ]
                    .into(),
                })),
                offsets: vec![0, 4, 8, 11, 15, 20].into(),
            })),
        ),
        (
            "b",
            DataType::UInt8,
            Column::from_data(vec![0u8, 1, 2, 3, 4]),
        ),
    ]);
    run_ast(&mut file, "TRY_CAST(a AS UINT8)", &[(
        "a",
        DataType::UInt16,
        Column::from_data(vec![0u16, 64, 255, 512, 1024]),
    )]);
    run_ast(&mut file, "TRY_CAST(a AS UINT16)", &[(
        "a",
        DataType::Int16,
        Column::from_data(vec![0i16, 1, 2, 3, -4]),
    )]);
    run_ast(&mut file, "TRY_CAST(a AS INT64)", &[(
        "a",
        DataType::Int16,
        Column::from_data(vec![0i16, 1, 2, 3, -4]),
    )]);
    run_ast(
        &mut file,
        "create_tuple(TRY_CAST(a AS FLOAT32), TRY_CAST(a AS INT32), TRY_CAST(b AS FLOAT32), TRY_CAST(b AS INT32))",
        &[
            (
                "a",
                DataType::UInt64,
                Column::from_data(vec![
                    0,
                    1,
                    u8::MAX as u64,
                    u16::MAX as u64,
                    u32::MAX as u64,
                    u64::MAX,
                ]),
            ),
            (
                "b",
                DataType::Float64,
                Column::from_data(vec![
                    0.0,
                    u32::MAX as f64,
                    u64::MAX as f64,
                    f64::MIN,
                    f64::MAX,
                    f64::INFINITY,
                ]),
            ),
        ],
    );
    run_ast(
        &mut file,
        "TRY_CAST(create_array(create_array(a, b), null, null) AS Array(Array(Int8)))",
        &[
            (
                "a",
                DataType::Int16,
                Column::from_data(vec![0i16, 1, 2, 127, 255]),
            ),
            (
                "b",
                DataType::Int16,
                Column::from_data(vec![0i16, -1, -127, -128, -129]),
            ),
        ],
    );
    run_ast(
        &mut file,
        "TRY_CAST(create_tuple(a, b, NULL) AS TUPLE(Int8, UInt8, Boolean NULL))",
        &[
            (
                "a",
                DataType::Int16,
                Column::from_data(vec![0i16, 1, 2, 127, 256]),
            ),
            (
                "b",
                DataType::Int16,
                Column::from_data(vec![0i16, 1, -127, -128, -129]),
            ),
        ],
    );

    run_ast(&mut file, "CAST(a AS INT16)", &[(
        "a",
        DataType::Float64,
        Column::from_data(vec![0.0f64, 1.1, 2.2, 3.3, -4.4]),
    )]);

    run_ast(&mut file, "CAST(b AS INT16)", &[(
        "b",
        DataType::Int8,
        Column::from_data(vec![0i8, 1, 2, 3, -4]),
    )]);
}

#[test]
pub fn test_tyck_fail() {
    let mut mint = Mint::new("tests/it/testdata");
    let mut file = mint.new_goldenfile("tyck-fail.txt").unwrap();

    run_ast(&mut file, "true AND 1", &[]);
    run_ast(&mut file, "NOT NOT 'a'", &[]);
    run_ast(&mut file, "least(1, 2, 3, a)", &[(
        "a",
        DataType::Boolean,
        Column::from_data(vec![false; 3]),
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
            Column::from_data(vec![0i16, 1, 2, 3, 4]),
        ),
        (
            "b",
            DataType::Int16,
            Column::from_data(vec![5i16, 6, 7, 8, 9]),
        ),
        (
            "idx",
            DataType::Int16,
            Column::from_data(vec![0i16, 1, 2, 3, 4]),
        ),
    ]);
    run_ast(&mut file, "CAST(a AS UINT16)", &[(
        "a",
        DataType::Int16,
        Column::from_data(vec![0i16, 1, 2, 3, -4]),
    )]);

    run_ast(&mut file, "CAST(c AS INT16)", &[(
        "c",
        DataType::Int64,
        Column::from_data(vec![0i64, 11111111111, 2, 3, -4]),
    )]);
}

fn builtin_functions() -> FunctionRegistry {
    let mut registry = FunctionRegistry::default();

    registry.register_2_arg::<BooleanType, BooleanType, BooleanType, _, _>(
        "and",
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
            Some(NumberDomain {
                min: lhs.min.checked_add(rhs.min).unwrap_or(i16::MAX),
                max: lhs.max.checked_add(rhs.max).unwrap_or(i16::MAX),
            })
        },
        |lhs, rhs| lhs + rhs,
    );

    registry.register_2_arg::<NumberType<i32>, NumberType<i32>, NumberType<i32>, _, _>(
        "minus",
        FunctionProperty::default(),
        |lhs, rhs| {
            Some(NumberDomain {
                min: lhs.min.checked_sub(rhs.max).unwrap_or(i32::MAX),
                max: lhs.max.checked_sub(rhs.min).unwrap_or(i32::MAX),
            })
        },
        |lhs, rhs| lhs - rhs,
    );

    registry.register_2_arg::<NumberType<i64>, NumberType<i64>, NumberType<i64>, _, _>(
        "multiply",
        FunctionProperty::default(),
        |_, _| None,
        |lhs, rhs| lhs * rhs,
    );

    registry.register_2_arg::<NumberType<f32>, NumberType<f32>, NumberType<f32>, _, _>(
        "divide",
        FunctionProperty::default(),
        |_, _| None,
        |lhs, rhs| lhs / rhs,
    );

    registry.register_2_arg::<NumberType<f64>, NumberType<f64>, NumberType<f64>, _, _>(
        "avg",
        FunctionProperty::default(),
        |lhs, rhs| {
            Some(NumberDomain {
                min: (lhs.min + rhs.min) / 2.0,
                max: (lhs.max + rhs.max) / 2.0,
            })
        },
        |lhs, rhs| (lhs + rhs) / 2.0,
    );

    registry.register_1_arg::<BooleanType, BooleanType, _, _>(
        "not",
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
                    .map(|domain| domain.as_int16().unwrap().min)
                    .min()
                    .unwrap_or(0);
                let max = args_domain
                    .iter()
                    .map(|domain| domain.as_int16().unwrap().max)
                    .min()
                    .unwrap_or(0);
                Some(Domain::Int16(NumberDomain { min, max }))
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
        || Some(()),
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
                Some(args_domain.iter().fold(Domain::Array(None), |acc, x| {
                    acc.merge(&Domain::Array(Some(Box::new(x.clone()))))
                }))
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
                                    array_builder.push(scalar.clone());
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
                    Ok(Value::Column(Column::Array(Box::new(ArrayColumn {
                        values: array_builder.build(),
                        offsets,
                    }))))
                } else {
                    // All args are scalars, so we return a scalar as result
                    let mut array = ColumnBuilder::with_capacity(&generics[0], 0);
                    for arg in args {
                        match arg {
                            ValueRef::Scalar(scalar) => {
                                array.push(scalar.clone());
                            }
                            ValueRef::Column(_) => unreachable!(),
                        }
                    }
                    Ok(Value::Scalar(Scalar::Array(array.build())))
                }
            }),
        }))
    });

    registry.register_passthrough_nullable_2_arg::<ArrayType<GenericType<0>>, NumberType<i16>, GenericType<0>,_, _>(
        "get",
        FunctionProperty::default(),
        |_, _| None,
        vectorize_with_builder_2_arg::<ArrayType<GenericType<0>>, NumberType<i16>, GenericType<0>>(
            |array, idx, output| {
            let item = array
                .index(idx as usize)
                .ok_or_else(|| format!("index out of bounds: the len is {} but the index is {}", array.len(), idx))?;
            output.push(item);
            Ok(())
        }),
    );

    registry.register_function_factory("create_tuple", |_, args_type| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "create_tuple",
                args_type: args_type.to_vec(),
                return_type: DataType::Tuple(args_type.to_vec()),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|args_domain, _| Some(Domain::Tuple(args_domain.to_vec()))),
            eval: Box::new(move |args, _generics| {
                let len = args.iter().find_map(|arg| match arg {
                    ValueRef::Column(col) => Some(col.len()),
                    _ => None,
                });
                if let Some(len) = len {
                    let fields = args
                        .iter()
                        .map(|arg| match arg {
                            ValueRef::Scalar(scalar) => scalar.clone().repeat(len).build(),
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
                Some(args_domain[0].as_tuple().unwrap()[idx].clone())
            }),
            eval: Box::new(move |args, _| match &args[0] {
                ValueRef::Scalar(ScalarRef::Tuple(fields)) => {
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
                Some(Domain::Nullable(NullableDomain {
                    has_null: *has_null,
                    value,
                }))
            }),
            eval: Box::new(move |args, _| match &args[0] {
                ValueRef::Scalar(ScalarRef::Null) => Ok(Value::Scalar(Scalar::Null)),
                ValueRef::Scalar(ScalarRef::Tuple(fields)) => {
                    Ok(Value::Scalar(fields[idx].to_owned()))
                }
                ValueRef::Column(Column::Nullable(box NullableColumn {
                    column: Column::Tuple { fields, .. },
                    validity,
                })) => Ok(Value::Column(Column::Nullable(Box::new(NullableColumn {
                    column: fields[idx].to_owned(),
                    validity: validity.clone(),
                })))),
                _ => unreachable!(),
            }),
        }))
    });

    registry
}

fn run_ast(file: &mut impl Write, text: &str, columns: &[(&str, DataType, Column)]) {
    let result = try {
        let raw_expr = parse_raw_expr(
            text,
            &columns
                .iter()
                .map(|(name, ty, _)| (*name, ty.clone()))
                .collect::<Vec<_>>(),
        );

        let fn_registry = builtin_functions();
        let (expr, output_ty) = type_check::check(&raw_expr, &fn_registry)?;

        let remote_expr = RemoteExpr::from_expr(expr);
        let expr = remote_expr.into_expr(&fn_registry).unwrap();

        let input_domains = columns
            .iter()
            .map(|(_, _, col)| col.domain())
            .collect::<Vec<_>>();

        let constant_folder = ConstantFolder::new(&input_domains, FunctionContext::default());
        let (optimized_expr, output_domain) = constant_folder.fold(&expr);

        let num_rows = columns.iter().map(|col| col.2.len()).max().unwrap_or(0);
        let chunk = Chunk::new(
            columns
                .iter()
                .map(|(_, _, col)| Value::Column(col.clone()))
                .collect::<Vec<_>>(),
            num_rows,
        );

        columns.iter().for_each(|(_, _, col)| {
            test_arrow_conversion(col);
        });

        let evaluator = Evaluator::new(&chunk, FunctionContext::default());
        let result = evaluator.run(&expr);
        let optimized_result = evaluator.run(&optimized_expr);
        match &result {
            Ok(result) => assert!(
                result
                    .as_ref()
                    .sematically_eq(&optimized_result.unwrap().as_ref())
            ),
            Err(e) => assert_eq!(e, &optimized_result.unwrap_err()),
        }

        (
            raw_expr,
            expr,
            input_domains,
            output_ty,
            optimized_expr,
            output_domain
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "Unknown".to_string()),
            result?,
        )
    };

    match result {
        Ok((raw_expr, expr, input_domains, output_ty, optimized_expr, output_domain, result)) => {
            writeln!(file, "ast            : {text}").unwrap();
            writeln!(file, "raw expr       : {raw_expr}").unwrap();
            writeln!(file, "checked expr   : {expr}").unwrap();
            if optimized_expr != expr {
                writeln!(file, "optimized expr : {optimized_expr}").unwrap();
            }

            match result {
                Value::Scalar(output_scalar) => {
                    writeln!(file, "output type    : {output_ty}").unwrap();
                    writeln!(file, "output domain  : {output_domain}").unwrap();
                    writeln!(file, "output         : {}", output_scalar.as_ref()).unwrap();
                }
                Value::Column(output_col) => {
                    test_arrow_conversion(&output_col);

                    let mut table = Table::new();
                    table.load_preset("||--+-++|    ++++++");

                    let mut header = vec!["".to_string()];
                    header.extend(columns.iter().map(|(name, _, _)| name.to_string()));
                    header.push("Output".to_string());
                    table.set_header(header);

                    let mut type_row = vec!["Type".to_string()];
                    type_row.extend(columns.iter().map(|(_, ty, _)| ty.to_string()));
                    type_row.push(output_ty.to_string());
                    table.add_row(type_row);

                    let mut domain_row = vec!["Domain".to_string()];
                    domain_row.extend(input_domains.iter().map(|domain| domain.to_string()));
                    domain_row.push(output_domain.to_string());
                    table.add_row(domain_row);

                    for i in 0..output_col.len() {
                        let mut row = vec![format!("Row {i}")];
                        for (_, _, col) in columns.iter() {
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

                    for (name, _, col) in columns.iter() {
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

fn test_arrow_conversion(col: &Column) {
    let arrow_col = col.as_arrow();
    let new_col = Column::from_arrow(&*arrow_col);
    assert_eq!(col, &new_col, "arrow conversion went wrong");
}
