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

use comfy_table::Table;
use common_expression::chunk::Chunk;
use common_expression::evaluator::DomainCalculator;
use common_expression::evaluator::Evaluator;
use common_expression::expression::Literal;
use common_expression::expression::RawExpr;
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

fn run_ast(
    file: &mut impl Write,
    raw: &RawExpr,
    columns: Vec<Column>,
    columns_domain: Vec<Domain>,
) {
    writeln!(file, "raw expr       : {raw}").unwrap();

    let fn_registry = builtin_functions();
    let (expr, ty) = type_check::check(raw, &fn_registry).unwrap();

    writeln!(file, "checked expr   : {expr}").unwrap();
    writeln!(file, "type           : {ty}").unwrap();

    let domain_calculator = DomainCalculator {
        input_domains: columns_domain.clone(),
    };
    let output_domain = domain_calculator.calculate(&expr);
    writeln!(
        file,
        "domain calculation:\n{}",
        pretty_print_domain_test(&columns_domain, &output_domain)
    )
    .unwrap();

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

fn pretty_print_domain_test(columns_domain: &[Domain], output_domain: &Domain) -> String {
    let mut table = Table::new();
    table.load_preset("||--+-++|    ++++++");

    table.set_header(vec!["Column", "Domain"]);

    for (i, domain) in columns_domain.iter().enumerate() {
        table.add_row(vec![format!("Column {i}"), format!("{domain}")]);
    }

    table.add_row(vec!["Output".to_string(), format!("{output_domain}")]);

    table.to_string()
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
                },
                RawExpr::Literal(Literal::Int8(-10)),
            ],
            params: vec![],
        },
        vec![Column::Nullable {
            column: Box::new(Column::UInt8(vec![10, 11, 12].into())),
            validity: vec![false, true, false].into(),
        }],
        vec![Domain::Nullable(NullableDomain {
            has_null: true,
            value: Some(Box::new(Domain::UInt(UIntDomain { min: 10, max: 12 }))),
        })],
    );

    run_ast(
        &mut file,
        &RawExpr::FunctionCall {
            name: "plus".to_string(),
            args: vec![
                RawExpr::ColumnRef {
                    id: 0,
                    data_type: DataType::Nullable(Box::new(DataType::UInt8)),
                },
                RawExpr::ColumnRef {
                    id: 1,
                    data_type: DataType::Nullable(Box::new(DataType::UInt8)),
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
        vec![
            Domain::Nullable(NullableDomain {
                has_null: true,
                value: Some(Box::new(Domain::UInt(UIntDomain { min: 10, max: 12 }))),
            }),
            Domain::Nullable(NullableDomain {
                has_null: true,
                value: Some(Box::new(Domain::UInt(UIntDomain { min: 1, max: 3 }))),
            }),
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
                },
                RawExpr::ColumnRef {
                    id: 1,
                    data_type: DataType::Null,
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
        vec![
            Domain::Nullable(NullableDomain {
                has_null: true,
                value: Some(Box::new(Domain::UInt(UIntDomain { min: 10, max: 12 }))),
            }),
            Domain::Nullable(NullableDomain {
                has_null: true,
                value: None,
            }),
        ],
    );

    run_ast(
        &mut file,
        &RawExpr::FunctionCall {
            name: "not".to_string(),
            args: vec![RawExpr::ColumnRef {
                id: 0,
                data_type: DataType::Nullable(Box::new(DataType::Boolean)),
            }],
            params: vec![],
        },
        vec![Column::Nullable {
            column: Box::new(Column::Boolean(vec![true, false, true].into())),
            validity: vec![false, true, false].into(),
        }],
        vec![Domain::Nullable(NullableDomain {
            has_null: true,
            value: Some(Box::new(Domain::Boolean(BooleanDomain {
                has_false: true,
                has_true: true,
            }))),
        })],
    );

    run_ast(
        &mut file,
        &RawExpr::FunctionCall {
            name: "not".to_string(),
            args: vec![RawExpr::ColumnRef {
                id: 0,
                data_type: DataType::Null,
            }],
            params: vec![],
        },
        vec![Column::Null { len: 10 }],
        vec![Domain::Nullable(NullableDomain {
            has_null: true,
            value: None,
        })],
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
                    },
                    RawExpr::ColumnRef {
                        id: 1,
                        data_type: DataType::Nullable(Box::new(DataType::String)),
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
        vec![
            Domain::Int(IntDomain { min: 0, max: 4 }),
            Domain::Nullable(NullableDomain {
                has_null: true,
                value: Some(Box::new(Domain::String(StringDomain {
                    min: "a".as_bytes().to_vec(),
                    max: Some("e".as_bytes().to_vec()),
                }))),
            }),
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
        vec![Domain::Nullable(NullableDomain {
            has_null: true,
            value: Some(Box::new(Domain::Tuple(vec![
                Domain::Boolean(BooleanDomain {
                    has_false: true,
                    has_true: false,
                }),
                Domain::String(StringDomain {
                    min: "a".as_bytes().to_vec(),
                    max: Some("e".as_bytes().to_vec()),
                }),
            ]))),
        })],
    );

    run_ast(
        &mut file,
        &RawExpr::FunctionCall {
            name: "create_array".to_string(),
            args: vec![],
            params: vec![],
        },
        vec![],
        vec![],
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
        vec![],
        vec![],
    );

    run_ast(
        &mut file,
        &RawExpr::FunctionCall {
            name: "create_array".to_string(),
            args: vec![
                RawExpr::ColumnRef {
                    id: 0,
                    data_type: DataType::Int16,
                },
                RawExpr::ColumnRef {
                    id: 1,
                    data_type: DataType::Int16,
                },
            ],
            params: vec![],
        },
        vec![
            Column::Int16(vec![0, 1, 2, 3, 4].into()),
            Column::Int16(vec![5, 6, 7, 8, 9].into()),
        ],
        vec![
            Domain::Int(IntDomain { min: 0, max: 4 }),
            Domain::Int(IntDomain { min: 5, max: 9 }),
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
                        },
                        RawExpr::ColumnRef {
                            id: 1,
                            data_type: DataType::Int16,
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
        vec![
            Domain::Int(IntDomain { min: 0, max: 4 }),
            Domain::Int(IntDomain { min: 5, max: 9 }),
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
                },
                RawExpr::ColumnRef {
                    id: 1,
                    data_type: DataType::UInt8,
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
        vec![
            Domain::Array(Some(Box::new(Domain::Int(IntDomain { min: 0, max: 99 })))),
            Domain::UInt(UIntDomain { min: 0, max: 4 }),
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
                },
                RawExpr::ColumnRef {
                    id: 1,
                    data_type: DataType::UInt8,
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
        vec![
            Domain::Array(Some(Box::new(Domain::Array(Some(Box::new(Domain::Int(
                IntDomain { min: 0, max: 99 },
            ))))))),
            Domain::Int(IntDomain { min: 0, max: 4 }),
        ],
    );
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
            Some(IntDomain {
                min: lhs.min.checked_add(rhs.min).unwrap_or(i16::MAX as i64),
                max: lhs.max.checked_add(rhs.max).unwrap_or(i16::MAX as i64),
            })
        },
        |lhs, rhs| lhs + rhs,
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

    registry.register_0_arg_core::<EmptyArrayType, _, _>(
        "create_array",
        FunctionProperty::default(),
        || None,
        |_| Value::Scalar(()),
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

    registry.register_with_writer_2_arg::<ArrayType<GenericType<0>>, NumberType<i16>, GenericType<0>,_, _>(
        "get",
        FunctionProperty::default(),
        |item_domain, _| Some(item_domain.clone()),
        |array, idx, output| output.push(array.index(idx as usize)),
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
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(move |args_domain, _| {
                args_domain[0].as_tuple().unwrap()[idx].clone()
            }),
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
