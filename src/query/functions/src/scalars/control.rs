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

use common_expression::types::boolean::BooleanDomain;
use common_expression::types::nullable::NullableColumn;
use common_expression::types::nullable::NullableDomain;
use common_expression::types::AnyType;
use common_expression::types::BooleanType;
use common_expression::types::DataType;
use common_expression::types::GenericType;
use common_expression::types::NullType;
use common_expression::types::NullableType;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::Domain;
use common_expression::Function;
use common_expression::FunctionDomain;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;
use common_expression::FunctionSignature;
use common_expression::ScalarRef;
use common_expression::Value;
use common_expression::ValueRef;

/// Each result argument of `if` and `multi_if` is a tuple of (value, error).
/// This function is used to split the [`ValueRef`] into the two parts.
#[inline]
fn unwrap_error<'a>(
    val_with_error: &ValueRef<'a, AnyType>,
) -> (ValueRef<'a, AnyType>, ValueRef<'a, AnyType>) {
    match val_with_error {
        ValueRef::Scalar(scalar) => {
            let inner = scalar.as_tuple().unwrap();
            let value = inner.first().unwrap().clone();
            let error = inner.last().unwrap().clone();
            (ValueRef::Scalar(value), ValueRef::Scalar(error))
        }
        ValueRef::Column(col) => {
            let inner_col = col.as_tuple().unwrap();
            let value = ValueRef::Column(inner_col.first().unwrap().clone());
            let error = ValueRef::Column(inner_col.last().unwrap().clone());
            (value, error)
        }
    }
}

#[inline(always)]
fn unwrap_domain(domain: &Domain) -> Domain {
    domain.as_tuple().unwrap()[0].clone()
}

pub fn register(registry: &mut FunctionRegistry) {
    // special case for multi_if to have better performance in fixed size loop
    registry.register_function_factory("if", |_, args_type| {
        if args_type.len() != 3 {
            return None;
        }
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "if".to_string(),
                args_type: vec![
                    DataType::Nullable(Box::new(DataType::Boolean)),
                    DataType::Tuple(vec![
                        DataType::Generic(0),
                        DataType::Nullable(Box::new(DataType::String)),
                    ]),
                    DataType::Tuple(vec![
                        DataType::Generic(0),
                        DataType::Nullable(Box::new(DataType::String)),
                    ]),
                ],
                return_type: DataType::Generic(0),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|args_domain| {
                let (has_true, has_null_or_false) = match &args_domain[0] {
                    Domain::Nullable(NullableDomain {
                        has_null,
                        value:
                            Some(box Domain::Boolean(BooleanDomain {
                                has_true,
                                has_false,
                            })),
                    }) => (*has_true, *has_null || *has_false),
                    Domain::Nullable(NullableDomain { value: None, .. }) => (false, true),
                    _ => unreachable!(),
                };

                let domain = match (has_true, has_null_or_false) {
                    (true, false) => {
                        return FunctionDomain::Domain(unwrap_domain(&args_domain[1]));
                    }
                    (false, true) => None,
                    (true, true) => Some(unwrap_domain(&args_domain[1])),
                    _ => unreachable!(),
                };

                FunctionDomain::Domain(match domain {
                    Some(domain) => domain.merge(&unwrap_domain(args_domain.last().unwrap())),
                    None => unwrap_domain(args_domain.last().unwrap()),
                })
            }),
            eval: Box::new(|args, ctx| {
                let len = args.iter().find_map(|arg| match arg {
                    ValueRef::Column(col) => Some(col.len()),
                    _ => None,
                });

                let mut output_builder =
                    ColumnBuilder::with_capacity(&ctx.generics[0], len.unwrap_or(1));

                let (values1, errors1) = unwrap_error(&args[1]);
                let (values2, errors2) = unwrap_error(&args[2]);

                let values = vec![values1, values2];
                let errors = vec![errors1, errors2];

                for row_idx in 0..(len.unwrap_or(1)) {
                    let flag = match &args[0] {
                        ValueRef::Scalar(ScalarRef::Null) => false,
                        ValueRef::Scalar(ScalarRef::Boolean(cond)) => *cond,
                        ValueRef::Column(Column::Nullable(box NullableColumn {
                            column: Column::Boolean(cond_col),
                            validity,
                        })) => validity.get_bit(row_idx) && cond_col.get_bit(row_idx),
                        _ => unreachable!(),
                    };
                    let result_idx = 1 - flag as usize; // if flag { 0 } else { 1 }
                    if let ValueRef::Column(Column::Nullable(col)) = &errors[result_idx] {
                        if let Some(ScalarRef::String(err)) =
                            unsafe { &col.index_unchecked(row_idx) }
                        {
                            ctx.set_error(row_idx, &String::from_utf8_lossy(err));
                            ctx.set_already_rendered();
                            output_builder.push_default();
                            continue;
                        }
                    }
                    match &values[result_idx] {
                        ValueRef::Scalar(scalar) => {
                            output_builder.push(scalar.clone());
                        }
                        ValueRef::Column(col) => {
                            let inner = unsafe { col.index_unchecked(row_idx) };
                            output_builder.push(inner);
                        }
                    }
                }
                match len {
                    Some(_) => Value::Column(output_builder.build()),
                    None => Value::Scalar(output_builder.build_scalar()),
                }
            }),
        }))
    });

    registry.register_function_factory("multi_if", |_, args_type| {
        if args_type.len() < 3 || args_type.len() % 2 == 0 {
            return None;
        }
        let sig_args_type = (0..(args_type.len() - 1) / 2)
            .flat_map(|_| {
                [
                    DataType::Nullable(Box::new(DataType::Boolean)),
                    DataType::Tuple(vec![
                        DataType::Generic(0),
                        DataType::Nullable(Box::new(DataType::String)),
                    ]),
                ]
            })
            .chain([DataType::Tuple(vec![
                DataType::Generic(0),
                DataType::Nullable(Box::new(DataType::String)),
            ])])
            .collect();

        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "multi_if".to_string(),
                args_type: sig_args_type,
                return_type: DataType::Generic(0),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|args_domain| {
                let mut domain = None;
                for cond_idx in (0..args_domain.len() - 1).step_by(2) {
                    let (has_true, has_null_or_false) = match &args_domain[cond_idx] {
                        Domain::Nullable(NullableDomain {
                            has_null,
                            value:
                                Some(box Domain::Boolean(BooleanDomain {
                                    has_true,
                                    has_false,
                                })),
                        }) => (*has_true, *has_null || *has_false),
                        Domain::Nullable(NullableDomain { value: None, .. }) => (false, true),
                        _ => unreachable!(),
                    };
                    match (&mut domain, has_true, has_null_or_false) {
                        (None, true, false) => {
                            return FunctionDomain::Domain(unwrap_domain(
                                &args_domain[cond_idx + 1],
                            ));
                        }
                        (None, false, true) => {
                            continue;
                        }
                        (None, true, true) => {
                            domain = Some(unwrap_domain(&args_domain[cond_idx + 1]));
                        }
                        (Some(prev_domain), true, false) => {
                            return FunctionDomain::Domain(
                                prev_domain.merge(&unwrap_domain(&args_domain[cond_idx + 1])),
                            );
                        }
                        (Some(_), false, true) => {
                            continue;
                        }
                        (Some(prev_domain), true, true) => {
                            domain =
                                Some(prev_domain.merge(&unwrap_domain(&args_domain[cond_idx + 1])));
                        }
                        (_, false, false) => unreachable!(),
                    }
                }

                FunctionDomain::Domain(match domain {
                    Some(domain) => domain.merge(&unwrap_domain(args_domain.last().unwrap())),
                    None => unwrap_domain(args_domain.last().unwrap()),
                })
            }),
            eval: Box::new(|args, ctx| {
                let len = args.iter().find_map(|arg| match arg {
                    ValueRef::Column(col) => Some(col.len()),
                    _ => None,
                });

                let mut output_builder =
                    ColumnBuilder::with_capacity(&ctx.generics[0], len.unwrap_or(1));

                let (values, errors) = args
                    .iter()
                    .skip(1)
                    .step_by(2)
                    .chain(vec![args.last().unwrap()])
                    .map(unwrap_error)
                    .unzip::<_, _, Vec<_>, Vec<_>>();
                for row_idx in 0..(len.unwrap_or(1)) {
                    let result_idx = (0..args.len() - 1)
                        .step_by(2)
                        .find(|&cond_idx| match &args[cond_idx] {
                            ValueRef::Scalar(ScalarRef::Null) => false,
                            ValueRef::Scalar(ScalarRef::Boolean(cond)) => *cond,
                            ValueRef::Column(Column::Nullable(box NullableColumn {
                                column: Column::Boolean(cond_col),
                                validity,
                            })) => validity.get_bit(row_idx) && cond_col.get_bit(row_idx),
                            _ => unreachable!(),
                        })
                        .map(|idx| {
                            // The next argument of true condition is the value to return.
                            idx / 2
                        })
                        .unwrap_or_else(|| {
                            // If no true condition is found, the last argument is the value to return.
                            values.len() - 1
                        });
                    if let ValueRef::Column(Column::Nullable(col)) = &errors[result_idx] {
                        if let Some(ScalarRef::String(err)) =
                            unsafe { &col.index_unchecked(row_idx) }
                        {
                            ctx.set_error(row_idx, &String::from_utf8_lossy(err));
                            ctx.set_already_rendered();
                            output_builder.push_default();
                            continue;
                        }
                    }
                    match &values[result_idx] {
                        ValueRef::Scalar(scalar) => {
                            output_builder.push(scalar.clone());
                        }
                        ValueRef::Column(col) => {
                            let inner = unsafe { col.index_unchecked(row_idx) };
                            output_builder.push(inner);
                        }
                    }
                }

                match len {
                    Some(_) => Value::Column(output_builder.build()),
                    None => Value::Scalar(output_builder.build_scalar()),
                }
            }),
        }))
    });

    registry.register_1_arg_core::<NullType, BooleanType, _, _>(
        "is_not_null",
        FunctionProperty::default(),
        |_| {
            FunctionDomain::Domain(BooleanDomain {
                has_true: false,
                has_false: true,
            })
        },
        |_, _| Value::Scalar(false),
    );
    registry.register_1_arg_core::<NullableType<GenericType<0>>, BooleanType, _, _>(
        "is_not_null",
        FunctionProperty::default(),
        |NullableDomain { has_null, value }| {
            FunctionDomain::Domain(BooleanDomain {
                has_true: value.is_some(),
                has_false: *has_null,
            })
        },
        |arg, _| match &arg {
            ValueRef::Column(NullableColumn { validity, .. }) => {
                let bitmap = validity.clone();
                Value::Column(bitmap)
            }
            ValueRef::Scalar(None) => Value::Scalar(false),
            ValueRef::Scalar(Some(_)) => Value::Scalar(true),
        },
    );
}
