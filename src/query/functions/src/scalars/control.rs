// Copyright 2021 Datafuse Labs
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

use databend_common_expression::Domain;
use databend_common_expression::Function;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionFactory;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::ImplByEvaluator;
use databend_common_expression::Value;
use databend_common_expression::domain_evaluator;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::GenericType;
use databend_common_expression::types::NullType;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::boolean::BooleanDomain;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::nullable::NullableDomain;

pub fn register(registry: &mut FunctionRegistry) {
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

        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "if".to_string(),
                args_type: sig_args_type,
                return_type: DataType::Generic(0),
            },
            eval: FunctionEval::Scalar {
                calc_domain: domain_evaluator(|_, args_domain| {
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
                                return FunctionDomain::Domain(args_domain[cond_idx + 1].clone());
                            }
                            (None, false, true) => {
                                continue;
                            }
                            (None, true, true) => {
                                domain = Some(args_domain[cond_idx + 1].clone());
                            }
                            (Some(prev_domain), true, false) => {
                                return FunctionDomain::Domain(
                                    prev_domain.merge(&args_domain[cond_idx + 1]),
                                );
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

                    FunctionDomain::Domain(match domain {
                        Some(domain) => domain.merge(args_domain.last().unwrap()),
                        None => args_domain.last().unwrap().clone(),
                    })
                }),
                eval: Box::new(ImplByEvaluator("if")),
                derive_stat: None,
            },
        }))
    }));
    registry.register_function_factory("if", factory);

    registry.register_1_arg_core::<NullType, BooleanType, _, _>(
        "is_not_null",
        |_, _| {
            FunctionDomain::Domain(BooleanDomain {
                has_true: false,
                has_false: true,
            })
        },
        |_, _| Value::Scalar(false),
    );
    registry.register_1_arg_core::<NullableType<GenericType<0>>, BooleanType, _, _>(
        "is_not_null",
        |_, NullableDomain { has_null, value }| {
            FunctionDomain::Domain(BooleanDomain {
                has_true: value.is_some(),
                has_false: *has_null,
            })
        },
        |arg, _| match &arg {
            Value::Column(NullableColumn { validity, .. }) => {
                let bitmap = validity.clone();
                Value::Column(bitmap)
            }
            Value::Scalar(None) => Value::Scalar(false),
            Value::Scalar(Some(_)) => Value::Scalar(true),
        },
    );

    registry.register_1_arg_core::<GenericType<0>, BooleanType, _, _>(
        "is_not_error",
        |_, _| FunctionDomain::Full,
        |arg, ctx| match ctx.errors.take() {
            Some((bitmap, _)) => match arg {
                Value::Column(_) => Value::Column(bitmap.into()),
                Value::Scalar(_) => Value::Scalar(bitmap.get(0)),
            },
            None => Value::Scalar(true),
        },
    );
}
