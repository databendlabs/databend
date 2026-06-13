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

use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::Domain;
use databend_common_expression::Function;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionFactory;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_expression::domain_evaluator;
use databend_common_expression::scalar_evaluator;
use databend_common_expression::types::DataType;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::nullable::NullableDomain;

pub fn register(registry: &mut FunctionRegistry) {
    let tuple = FunctionFactory::Closure(Box::new(|_, args_type: &[DataType]| {
        if args_type.is_empty() {
            return None;
        }
        let args_type = args_type.to_vec();
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "tuple".to_string(),
                args_type: args_type.clone(),
                return_type: DataType::Tuple(args_type.clone()),
            },
            eval: FunctionEval::Scalar {
                calc_domain: domain_evaluator(|_, args_domain| {
                    FunctionDomain::Domain(Domain::Tuple(args_domain.to_vec()))
                }),
                eval: scalar_evaluator(move |args, _| {
                    let len = args.iter().find_map(|arg| match arg {
                        Value::Column(col) => Some(col.len()),
                        _ => None,
                    });
                    if let Some(len) = len {
                        let fields = args
                            .iter()
                            .zip(&args_type)
                            .map(|(arg, ty)| match arg {
                                Value::Scalar(scalar) => {
                                    ColumnBuilder::repeat(&scalar.as_ref(), len, ty).build()
                                }
                                Value::Column(col) => col.clone(),
                            })
                            .collect();
                        Value::Column(Column::Tuple(fields))
                    } else {
                        // All args are scalars, so we return a scalar as result
                        let fields = args
                            .iter()
                            .map(|arg| match arg {
                                Value::Scalar(scalar) => scalar.clone(),
                                Value::Column(_) => unreachable!(),
                            })
                            .collect();
                        Value::Scalar(Scalar::Tuple(fields))
                    }
                }),
                derive_stat: None,
            },
        }))
    }));
    registry.register_function_factory("tuple", tuple);

    let get = FunctionFactory::Closure(Box::new(|params, args_type: &[DataType]| {
        // Tuple index starts from 1
        let idx = (params.first()?.get_i64()? as usize).checked_sub(1)?;

        let fields_ty = match args_type.first()? {
            DataType::Tuple(tys) => tys,
            _ => return None,
        };
        if idx >= fields_ty.len() {
            return None;
        }

        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "get".to_string(),
                args_type: vec![DataType::Tuple(
                    (0..fields_ty.len()).map(DataType::Generic).collect(),
                )],
                return_type: DataType::Generic(idx),
            },
            eval: FunctionEval::Scalar {
                calc_domain: domain_evaluator(move |_, args_domain| {
                    FunctionDomain::Domain(args_domain[0].as_tuple().unwrap()[idx].clone())
                }),
                eval: scalar_evaluator(move |args, _| match &args[0] {
                    Value::Scalar(Scalar::Tuple(fields)) => Value::Scalar(fields[idx].to_owned()),
                    Value::Column(Column::Tuple(fields)) => Value::Column(fields[idx].to_owned()),
                    _ => unreachable!(),
                }),
                derive_stat: None,
            },
        }))
    }));
    registry.register_function_factory("get", get);

    let get = FunctionFactory::Closure(Box::new(|params, args_type: &[DataType]| {
        // Tuple index starts from 1
        let idx = usize::try_from(params.first()?.get_i64()? - 1).ok()?;
        let fields_ty = match args_type.first()? {
            DataType::Nullable(box DataType::Tuple(tys)) => tys,
            _ => return None,
        };
        if idx >= fields_ty.len() {
            return None;
        }

        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "get".to_string(),
                args_type: vec![DataType::Nullable(Box::new(DataType::Tuple(
                    (0..fields_ty.len())
                        .map(|idx| DataType::Nullable(Box::new(DataType::Generic(idx))))
                        .collect(),
                )))],
                return_type: DataType::Nullable(Box::new(DataType::Generic(idx))),
            },
            eval: FunctionEval::Scalar {
                calc_domain: domain_evaluator(move |_, args_domain| {
                    let NullableDomain { has_null, value } = args_domain[0].as_nullable().unwrap();
                    match value {
                        Some(value) => {
                            let NullableDomain {
                                has_null: field_has_null,
                                value: field_value,
                            } = value.as_tuple().unwrap()[idx].as_nullable().unwrap();
                            FunctionDomain::Domain(Domain::Nullable(NullableDomain {
                                has_null: *has_null || *field_has_null,
                                value: field_value.clone(),
                            }))
                        }
                        None => FunctionDomain::Domain(Domain::Nullable(NullableDomain {
                            has_null: true,
                            value: None,
                        })),
                    }
                }),
                eval: scalar_evaluator(move |args, _| match &args[0] {
                    Value::Scalar(Scalar::Null) => Value::Scalar(Scalar::Null),
                    Value::Scalar(Scalar::Tuple(fields)) => Value::Scalar(fields[idx].to_owned()),
                    Value::Column(Column::Nullable(box NullableColumn {
                        column: Column::Tuple(fields),
                        validity,
                    })) => {
                        let field_col = fields[idx].as_nullable().unwrap();
                        Value::Column(NullableColumn::new_column(
                            field_col.column.clone(),
                            (&field_col.validity) & validity,
                        ))
                    }
                    _ => unreachable!(),
                }),
                derive_stat: None,
            },
        }))
    }));
    registry.register_function_factory("get", get);

    let get = FunctionFactory::Closure(Box::new(|params, args_type: &[DataType]| {
        // Tuple index starts from 1
        let idx = usize::try_from(params.first()?.get_i64()? - 1).ok()?;
        let fields_ty = match args_type.first()? {
            DataType::Nullable(box DataType::Tuple(tys)) => tys,
            _ => return None,
        };
        if idx >= fields_ty.len() {
            return None;
        }

        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "get".to_string(),
                args_type: vec![DataType::Nullable(Box::new(DataType::Tuple(
                    (0..fields_ty.len())
                        .map(|i| {
                            if i == idx {
                                DataType::Null
                            } else {
                                DataType::Generic(i)
                            }
                        })
                        .collect(),
                )))],
                return_type: DataType::Null,
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(FunctionDomain::Full),
                eval: scalar_evaluator(move |_, _| Value::Scalar(Scalar::Null)),
                derive_stat: None,
            },
        }))
    }));
    registry.register_function_factory("get", get);
}
