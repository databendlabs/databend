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

use common_expression::types::nullable::NullableColumn;
use common_expression::types::nullable::NullableDomain;
use common_expression::types::DataType;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::Domain;
use common_expression::Function;
use common_expression::FunctionDomain;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;
use common_expression::FunctionSignature;
use common_expression::Scalar;
use common_expression::ScalarRef;
use common_expression::Value;
use common_expression::ValueRef;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_function_factory("tuple", |_, args_type| {
        if args_type.is_empty() {
            return None;
        }
        let args_type = args_type.to_vec();
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "tuple".to_string(),
                args_type: args_type.clone(),
                return_type: DataType::Tuple(args_type.clone()),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|args_domain| {
                FunctionDomain::Domain(Domain::Tuple(args_domain.to_vec()))
            }),
            eval: Box::new(move |args, _| {
                let len = args.iter().find_map(|arg| match arg {
                    ValueRef::Column(col) => Some(col.len()),
                    _ => None,
                });
                if let Some(len) = len {
                    let fields = args
                        .iter()
                        .zip(&args_type)
                        .map(|(arg, ty)| match arg {
                            ValueRef::Scalar(scalar) => {
                                ColumnBuilder::repeat(scalar, len, ty).build()
                            }
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

    registry.register_function_factory("get", |params, args_type| {
        // Tuple index starts from 1
        let idx = params.first()?.checked_sub(1)?;
        let fields_ty = match args_type.get(0)? {
            DataType::Tuple(tys) => tys,
            _ => return None,
        };
        if idx >= fields_ty.len() {
            return None;
        }

        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "get".to_string(),
                args_type: vec![DataType::Tuple(fields_ty.to_vec())],
                return_type: fields_ty[idx].clone(),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(move |args_domain| {
                FunctionDomain::Domain(args_domain[0].as_tuple().unwrap()[idx].clone())
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

    registry.register_function_factory("get", |params, args_type| {
        // Tuple index starts from 1
        let idx = params.first()?.checked_sub(1)?;
        let fields_ty = match args_type.get(0)? {
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
                    fields_ty.to_vec(),
                )))],
                return_type: DataType::Nullable(Box::new(fields_ty[idx].clone())),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(move |args_domain| {
                let NullableDomain { has_null, value } = args_domain[0].as_nullable().unwrap();
                let value = value.as_ref().map(|value| {
                    let fields = value.as_tuple().unwrap();
                    Box::new(fields[idx].clone())
                });
                FunctionDomain::Domain(Domain::Nullable(NullableDomain {
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
}
