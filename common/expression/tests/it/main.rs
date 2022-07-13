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

use std::io::Write;
use std::iter::once;
use std::sync::Arc;

use common_expression::chunk::Chunk;
use common_expression::evaluator::Evaluator;
use common_expression::expression::Literal;
use common_expression::expression::RawExpr;
use common_expression::function::vectorize_2_arg;
use common_expression::function::Function;
use common_expression::function::FunctionContext;
use common_expression::function::FunctionRegistry;
use common_expression::function::FunctionSignature;
use common_expression::property::FunctionProperty;
use common_expression::property::ValueProperty;
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

fn run_ast(file: &mut impl Write, raw: &RawExpr, columns: Vec<Column>) {
    writeln!(file, "raw expr       : {raw}").unwrap();

    let fn_registry = builtin_functions();
    let (expr, ty, prop) = type_check::check(raw, &fn_registry).unwrap();

    writeln!(file, "type           : {ty}").unwrap();
    writeln!(file, "property       : {prop}").unwrap();
    writeln!(file, "checked expr   : {expr}").unwrap();

    let chunk = Chunk::new(columns);
    if chunk.num_columns() > 0 {
        writeln!(file, "input chunk:\n{}", chunk).unwrap();
        writeln!(file, "input chunk (internal):\n{:?}", chunk).unwrap();
    }

    let runtime = Evaluator {
        input_columns: chunk,
        context: FunctionContext::default(),
    };
    let result = runtime.run(&expr);
    match result {
        Value::Scalar(scalar) => writeln!(file, "evaluation result:\n{}", scalar.as_ref()).unwrap(),
        Value::Column(col) => {
            let chunk = Chunk::new(vec![col]);
            writeln!(file, "evaluation result:\n{}", chunk).unwrap();
            writeln!(file, "evaluation result (internal):\n{:?}", chunk).unwrap();
        }
    }

    write!(file, "\n\n").unwrap();
}

#[test]
pub fn test() {
    let mut mint = Mint::new("tests/it/testdata");
    let mut file = mint.new_goldenfile("run-ast.txt").unwrap();

    run_ast(
        &mut file,
        &RawExpr::FunctionCall {
            name: "and".to_string(),
            args: vec![
                RawExpr::Literal(Literal::Boolean(true)),
                RawExpr::Literal(Literal::Boolean(false)),
            ],
            params: vec![],
        },
        vec![],
    );

    run_ast(
        &mut file,
        &RawExpr::FunctionCall {
            name: "and".to_string(),
            args: vec![
                RawExpr::Literal(Literal::Null),
                RawExpr::Literal(Literal::Boolean(false)),
            ],
            params: vec![],
        },
        vec![],
    );

    run_ast(
        &mut file,
        &RawExpr::FunctionCall {
            name: "plus".to_string(),
            args: vec![
                RawExpr::ColumnRef {
                    id: 0,
                    data_type: DataType::Nullable(Box::new(DataType::UInt8)),
                    property: ValueProperty::default(),
                },
                RawExpr::Literal(Literal::Int8(-10)),
            ],
            params: vec![],
        },
        vec![Column::Nullable {
            column: Box::new(Column::UInt8(vec![10, 11, 12].into())),
            validity: vec![false, true, false].into(),
        }],
    );

    run_ast(
        &mut file,
        &RawExpr::FunctionCall {
            name: "plus".to_string(),
            args: vec![
                RawExpr::ColumnRef {
                    id: 0,
                    data_type: DataType::Nullable(Box::new(DataType::UInt8)),
                    property: ValueProperty::default(),
                },
                RawExpr::ColumnRef {
                    id: 1,
                    data_type: DataType::Nullable(Box::new(DataType::UInt8)),
                    property: ValueProperty::default(),
                },
            ],
            params: vec![],
        },
        vec![
            Column::Nullable {
                column: Box::new(Column::UInt8(vec![10, 11, 12].into())),
                validity: vec![false, true, false].into(),
            },
            Column::Nullable {
                column: Box::new(Column::UInt8(vec![1, 2, 3].into())),
                validity: vec![false, true, true].into(),
            },
        ],
    );

    run_ast(
        &mut file,
        &RawExpr::FunctionCall {
            name: "plus".to_string(),
            args: vec![
                RawExpr::ColumnRef {
                    id: 0,
                    data_type: DataType::Nullable(Box::new(DataType::UInt8)),
                    property: ValueProperty::default(),
                },
                RawExpr::ColumnRef {
                    id: 1,
                    data_type: DataType::Null,
                    property: ValueProperty::default(),
                },
            ],
            params: vec![],
        },
        vec![
            Column::Nullable {
                column: Box::new(Column::UInt8(vec![10, 11, 12].into())),
                validity: vec![false, true, false].into(),
            },
            Column::Null { len: 3 },
        ],
    );

    run_ast(
        &mut file,
        &RawExpr::FunctionCall {
            name: "not".to_string(),
            args: vec![RawExpr::ColumnRef {
                id: 0,
                data_type: DataType::Nullable(Box::new(DataType::Boolean)),
                property: ValueProperty::default(),
            }],
            params: vec![],
        },
        vec![Column::Nullable {
            column: Box::new(Column::Boolean(vec![true, false, true].into())),
            validity: vec![false, true, false].into(),
        }],
    );

    run_ast(
        &mut file,
        &RawExpr::FunctionCall {
            name: "not".to_string(),
            args: vec![RawExpr::ColumnRef {
                id: 0,
                data_type: DataType::Null,
                property: ValueProperty::default(),
            }],
            params: vec![],
        },
        vec![Column::Null { len: 10 }],
    );

    run_ast(
        &mut file,
        &RawExpr::FunctionCall {
            name: "least".to_string(),
            args: vec![
                RawExpr::Literal(Literal::UInt8(10)),
                RawExpr::Literal(Literal::UInt8(20)),
                RawExpr::Literal(Literal::UInt8(30)),
                RawExpr::Literal(Literal::UInt8(40)),
            ],
            params: vec![],
        },
        vec![],
    );

    run_ast(
        &mut file,
        &RawExpr::FunctionCall {
            name: "create_tuple".to_string(),
            args: vec![
                RawExpr::Literal(Literal::Null),
                RawExpr::Literal(Literal::Boolean(true)),
            ],
            params: vec![],
        },
        vec![],
    );

    run_ast(
        &mut file,
        &RawExpr::FunctionCall {
            name: "get_tuple".to_string(),
            args: vec![RawExpr::FunctionCall {
                name: "create_tuple".to_string(),
                args: vec![
                    RawExpr::ColumnRef {
                        id: 0,
                        data_type: DataType::Int16,
                        property: ValueProperty::default().not_null(true),
                    },
                    RawExpr::ColumnRef {
                        id: 1,
                        data_type: DataType::Nullable(Box::new(DataType::String)),
                        property: ValueProperty::default(),
                    },
                ],
                params: vec![],
            }],
            params: vec![1],
        },
        vec![
            Column::Int16(vec![0, 1, 2, 3, 4].into()),
            Column::Nullable {
                column: Box::new(Column::String {
                    data: "abcde".as_bytes().to_vec().into(),
                    offsets: vec![0, 1, 2, 3, 4, 5].into(),
                }),
                validity: vec![true, true, false, false, false].into(),
            },
        ],
    );

    run_ast(
        &mut file,
        &RawExpr::FunctionCall {
            name: "get_tuple".to_string(),
            args: vec![RawExpr::ColumnRef {
                id: 0,
                data_type: DataType::Nullable(Box::new(DataType::Tuple(vec![
                    DataType::Boolean,
                    DataType::String,
                ]))),
                property: ValueProperty::default().not_null(true),
            }],
            params: vec![1],
        },
        vec![Column::Nullable {
            column: Box::new(Column::Tuple {
                fields: vec![Column::Boolean(vec![false; 5].into()), Column::String {
                    data: "abcde".as_bytes().to_vec().into(),
                    offsets: vec![0, 1, 2, 3, 4, 5].into(),
                }],
                len: 5,
            }),
            validity: vec![true, true, false, false, false].into(),
        }],
    );

    run_ast(
        &mut file,
        &RawExpr::FunctionCall {
            name: "create_array".to_string(),
            args: vec![],
            params: vec![],
        },
        [].into_iter().collect(),
    );

    run_ast(
        &mut file,
        &RawExpr::FunctionCall {
            name: "create_array".to_string(),
            args: vec![
                RawExpr::Literal(Literal::Null),
                RawExpr::Literal(Literal::Boolean(true)),
            ],
            params: vec![],
        },
        [].into_iter().collect(),
    );

    run_ast(
        &mut file,
        &RawExpr::FunctionCall {
            name: "create_array".to_string(),
            args: vec![
                RawExpr::ColumnRef {
                    id: 0,
                    data_type: DataType::Int16,
                    property: ValueProperty::default().not_null(true),
                },
                RawExpr::ColumnRef {
                    id: 1,
                    data_type: DataType::Int16,
                    property: ValueProperty::default().not_null(true),
                },
            ],
            params: vec![],
        },
        vec![
            Column::Int16(vec![0, 1, 2, 3, 4].into()),
            Column::Int16(vec![5, 6, 7, 8, 9].into()),
        ],
    );

    run_ast(
        &mut file,
        &RawExpr::FunctionCall {
            name: "create_array".to_string(),
            args: vec![
                RawExpr::FunctionCall {
                    name: "create_array".to_string(),
                    args: vec![
                        RawExpr::ColumnRef {
                            id: 0,
                            data_type: DataType::Int16,
                            property: ValueProperty::default().not_null(true),
                        },
                        RawExpr::ColumnRef {
                            id: 1,
                            data_type: DataType::Int16,
                            property: ValueProperty::default().not_null(true),
                        },
                    ],
                    params: vec![],
                },
                RawExpr::Literal(Literal::Null),
                RawExpr::Literal(Literal::Null),
            ],
            params: vec![],
        },
        vec![
            Column::Int16(vec![0, 1, 2, 3, 4].into()),
            Column::Int16(vec![5, 6, 7, 8, 9].into()),
        ],
    );

    run_ast(
        &mut file,
        &RawExpr::FunctionCall {
            name: "get".to_string(),
            args: vec![
                RawExpr::ColumnRef {
                    id: 0,
                    data_type: DataType::Array(Box::new(DataType::Int16)),
                    property: ValueProperty::default().not_null(true),
                },
                RawExpr::ColumnRef {
                    id: 1,
                    data_type: DataType::UInt8,
                    property: ValueProperty::default().not_null(true),
                },
            ],
            params: vec![],
        },
        vec![
            Column::Array {
                array: Box::new(Column::Int16((0..100).collect())),
                offsets: vec![0, 20, 40, 60, 80, 100].into(),
            },
            Column::UInt8(vec![0, 1, 2, 3, 4].into()),
        ],
    );

    run_ast(
        &mut file,
        &RawExpr::FunctionCall {
            name: "get".to_string(),
            args: vec![
                RawExpr::ColumnRef {
                    id: 0,
                    data_type: DataType::Array(Box::new(DataType::Array(Box::new(
                        DataType::Int16,
                    )))),
                    property: ValueProperty::default().not_null(true),
                },
                RawExpr::ColumnRef {
                    id: 1,
                    data_type: DataType::UInt8,
                    property: ValueProperty::default().not_null(true),
                },
            ],
            params: vec![],
        },
        vec![
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
            Column::UInt8(vec![0, 1, 2, 3, 4].into()),
        ],
    );
}

fn builtin_functions() -> FunctionRegistry {
    let mut registry = FunctionRegistry::default();

    registry.register_2_arg::<BooleanType, BooleanType, BooleanType, _>(
        "and",
        FunctionProperty::default(),
        |lhs, rhs| lhs && rhs,
    );

    registry.register_2_arg::<NumberType<i16>, NumberType<i16>, NumberType<i16>, _>(
        "plus",
        FunctionProperty::default(),
        |lhs, rhs| lhs + rhs,
    );

    registry.register_1_arg::<BooleanType, BooleanType, _>(
        "not",
        FunctionProperty::default(),
        |val| !val,
    );

    registry.register_function_factory("least", |_, args_type| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "least",
                args_type: vec![DataType::Int16; args_type.len()],
                return_type: DataType::Int16,
                property: FunctionProperty::default().preserve_not_null(true),
            },
            eval: Box::new(|args, generics| {
                if args.is_empty() {
                    Value::Scalar(Scalar::Int16(0))
                } else if args.len() == 1 {
                    args[0].clone().to_owned()
                } else {
                    let mut min =
                        vectorize_2_arg::<NumberType<i16>, NumberType<i16>, NumberType<i16>>(
                            |lhs, rhs| lhs.min(rhs),
                        )(
                            args[0].try_downcast().unwrap(),
                            args[1].try_downcast().unwrap(),
                            generics,
                        );
                    for arg in &args[2..] {
                        min = vectorize_2_arg::<NumberType<i16>, NumberType<i16>, NumberType<i16>>(
                            |lhs, rhs| lhs.min(rhs),
                        )(
                            min.as_ref(), arg.try_downcast().unwrap(), generics
                        );
                    }
                    min.upcast()
                }
            }),
        }))
    });

    registry.register_0_arg_core::<EmptyArrayType, _>(
        "create_array",
        FunctionProperty::default(),
        |_| Value::Scalar(()),
    );

    registry.register_function_factory("create_array", |_, args_type| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "create_array",
                args_type: vec![DataType::Generic(0); args_type.len()],
                return_type: DataType::Array(Box::new(DataType::Generic(0))),
                property: FunctionProperty::default().preserve_not_null(true),
            },
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
                                    array_builder.push(col.index(idx));
                                }
                            }
                        }
                    }
                    let offsets = once(0)
                        .chain((0..len).map(|row| (args.len() * (row + 1)) as u64))
                        .collect();
                    Value::Column(Column::Array {
                        array: Box::new(array_builder.build()),
                        offsets,
                    })
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
                    Value::Scalar(Scalar::Array(array.build()))
                }
            }),
        }))
    });

    registry.register_with_writer_2_arg::<ArrayType<GenericType<0>>, NumberType<i16>, GenericType<0>, _>(
        "get",
        FunctionProperty::default(),
        |array, idx, output| output.push(array.index(idx as usize)),
    );

    registry.register_function_factory("create_tuple", |_, args_type| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "create_tuple",
                args_type: args_type.to_vec(),
                return_type: DataType::Tuple(args_type.to_vec()),
                property: FunctionProperty::default().preserve_not_null(true),
            },
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
                    Value::Column(Column::Tuple { fields, len })
                } else {
                    // All args are scalars, so we return a scalar as result
                    let fields = args
                        .iter()
                        .map(|arg| match arg {
                            ValueRef::Scalar(scalar) => (*scalar).to_owned(),
                            ValueRef::Column(_) => unreachable!(),
                        })
                        .collect();
                    Value::Scalar(Scalar::Tuple(fields))
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
                property: FunctionProperty::default().preserve_not_null(true),
            },
            eval: Box::new(move |args, _| match &args[0] {
                ValueRef::Scalar(Scalar::Tuple(fields)) => Value::Scalar(fields[idx].to_owned()),
                ValueRef::Column(Column::Tuple { fields, .. }) => {
                    Value::Column(fields[idx].to_owned())
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
                property: FunctionProperty::default().preserve_not_null(true),
            },
            eval: Box::new(move |args, _| match &args[0] {
                ValueRef::Scalar(Scalar::Null) => Value::Scalar(Scalar::Null),
                ValueRef::Scalar(Scalar::Tuple(fields)) => Value::Scalar(fields[idx].to_owned()),
                ValueRef::Column(Column::Nullable {
                    column: box Column::Tuple { fields, .. },
                    validity,
                }) => Value::Column(Column::Nullable {
                    column: Box::new(fields[idx].to_owned()),
                    validity: validity.clone(),
                }),
                _ => unreachable!(),
            }),
        }))
    });

    registry
}
