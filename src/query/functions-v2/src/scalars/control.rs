// Copyright 2021 Datafuse Labs.
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

use common_expression::types::nullable::NullableColumn;
use common_expression::types::BooleanType;
use common_expression::types::DataType;
use common_expression::types::GenericType;
use common_expression::types::NullType;
use common_expression::types::NullableType;
use common_expression::BooleanDomain;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::Domain;
use common_expression::Function;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;
use common_expression::FunctionSignature;
use common_expression::NullableDomain;
use common_expression::ScalarRef;
use common_expression::Value;
use common_expression::ValueRef;

const MULTI_IF: &str = "multi_if";
const IS_NOT_NULL: &str = "is_not_null";

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_function_factory(MULTI_IF, |_, args_type| {
        if args_type.len() < 3 || args_type.len() % 2 == 0 {
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

        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: MULTI_IF,
                args_type: sig_args_type,
                return_type: DataType::Generic(0),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|args_domain, _| {
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
                            return Some(args_domain[cond_idx + 1].clone());
                        }
                        (None, false, true) => {
                            continue;
                        }
                        (None, true, true) => {
                            domain = Some(args_domain[cond_idx + 1].clone());
                        }
                        (Some(prev_domain), true, false) => {
                            return Some(prev_domain.merge(&args_domain[cond_idx + 1]));
                        }
                        (Some(_), false, true) => {
                            continue;
                        }
                        (Some(prev_domain), true, true) => {
                            domain = Some(prev_domain.merge(&args_domain[cond_idx + 1]));
                        }
                        (_, false, false) => unreachable!(),
                    }
                }

                Some(match domain {
                    Some(domain) => domain.merge(args_domain.last().unwrap()),
                    None => args_domain.last().unwrap().clone(),
                })
            }),
            eval: Box::new(|args, generics| {
                let len = args.iter().find_map(|arg| match arg {
                    ValueRef::Column(col) => Some(col.len()),
                    _ => None,
                });

                let mut output_builder =
                    ColumnBuilder::with_capacity(&generics[0], len.unwrap_or(1));
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
                            idx + 1
                        })
                        .unwrap_or_else(|| {
                            // If no true condition is found, the last argument is the value to return.
                            args.len() - 1
                        });

                    match &args[result_idx] {
                        ValueRef::Scalar(scalar) => {
                            output_builder.push(scalar.clone());
                        }
                        ValueRef::Column(col) => {
                            output_builder.push(col.index(row_idx).unwrap());
                        }
                    }
                }

                match len {
                    Some(_) => Ok(Value::Column(output_builder.build())),
                    None => Ok(Value::Scalar(output_builder.build_scalar())),
                }
            }),
        }))
    });
    registry.register_1_arg_core::<NullType, BooleanType, _, _>(
        IS_NOT_NULL,
        FunctionProperty::default(),
        |_| {
            Some(BooleanDomain {
                has_true: false,
                has_false: true,
            })
        },
        |_, _| Ok(Value::Scalar(false)),
    );
    registry.register_1_arg_core::<NullableType<GenericType<0>>, BooleanType, _, _>(
        IS_NOT_NULL,
        FunctionProperty::default(),
        |NullableDomain { has_null, value }| {
            Some(BooleanDomain {
                has_true: value.is_some(),
                has_false: *has_null,
            })
        },
        |arg, _| match &arg {
            ValueRef::Column(NullableColumn { validity, .. }) => {
                let bitmap = validity.clone();
                Ok(Value::Column(bitmap))
            }
            ValueRef::Scalar(None) => Ok(Value::Scalar(false)),
            ValueRef::Scalar(Some(_)) => Ok(Value::Scalar(true)),
        },
    );
}
