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

use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::nullable::NullableDomain;
use databend_common_expression::types::ArrayColumn;
use databend_common_expression::types::DataType;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::Domain;
use databend_common_expression::Function;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::Value;

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
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, args_domain| {
                    FunctionDomain::Domain(Domain::Tuple(args_domain.to_vec()))
                }),
                eval: Box::new(move |args, _| {
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
            },
        }))
    });

    registry.register_function_factory("array_tuple", |_, args_type| {
        if args_type.is_empty() {
            return None;
        }
        let args_type = args_type.to_vec();

        let inner_types: Vec<DataType> = args_type
            .iter()
            .map(|arg_type| {
                let is_nullable = arg_type.is_nullable();
                match arg_type.remove_nullable() {
                    DataType::Array(box inner_type) => {
                        if is_nullable {
                            inner_type.wrap_nullable()
                        } else {
                            inner_type.clone()
                        }
                    }
                    _ => arg_type.clone(),
                }
            })
            .collect();
        let return_type = DataType::Array(Box::new(DataType::Tuple(inner_types.clone())));
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "array_tuple".to_string(),
                args_type: args_type.clone(),
                return_type,
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, args_domain| {
                    let inner_domains = args_domain
                        .iter()
                        .map(|arg_domain| match arg_domain {
                            Domain::Nullable(nullable_domain) => match &nullable_domain.value {
                                Some(box Domain::Array(Some(inner_domain))) => {
                                    Domain::Nullable(NullableDomain {
                                        has_null: nullable_domain.has_null,
                                        value: Some(Box::new(*inner_domain.clone())),
                                    })
                                }
                                _ => Domain::Nullable(nullable_domain.clone()),
                            },
                            Domain::Array(Some(box inner_domain)) => inner_domain.clone(),
                            _ => arg_domain.clone(),
                        })
                        .collect();
                    FunctionDomain::Domain(Domain::Array(Some(Box::new(Domain::Tuple(
                        inner_domains,
                    )))))
                }),
                eval: Box::new(move |args, ctx| {
                    let len = args.iter().find_map(|arg| match arg {
                        Value::Column(col) => Some(col.len()),
                        _ => None,
                    });

                    let mut offset = 0;
                    let mut offsets = Vec::new();
                    offsets.push(0);
                    let tuple_type = DataType::Tuple(inner_types.clone());
                    let mut builder = ColumnBuilder::with_capacity(&tuple_type, 0);
                    for i in 0..len.unwrap_or(1) {
                        let mut is_diff_len = false;
                        let mut array_len = None;
                        for arg in args {
                            let value = unsafe { arg.index_unchecked(i) };
                            if let ScalarRef::Array(col) = value {
                                if let Some(array_len) = array_len {
                                    if array_len != col.len() {
                                        is_diff_len = true;
                                        let err = format!(
                                            "array length must be equal, but got {} and {}",
                                            array_len,
                                            col.len()
                                        );
                                        ctx.set_error(builder.len(), err);
                                        offsets.push(offset);
                                        break;
                                    }
                                } else {
                                    array_len = Some(col.len());
                                }
                            }
                        }
                        if is_diff_len {
                            continue;
                        }
                        let array_len = array_len.unwrap_or(1);
                        for j in 0..array_len {
                            let mut tuple_values = Vec::with_capacity(args.len());
                            for arg in args {
                                let value = unsafe { arg.index_unchecked(i) };
                                match value {
                                    ScalarRef::Array(col) => {
                                        let tuple_value = unsafe { col.index_unchecked(j) };
                                        tuple_values.push(tuple_value.to_owned());
                                    }
                                    _ => {
                                        tuple_values.push(value.to_owned());
                                    }
                                }
                            }
                            let tuple_value = Scalar::Tuple(tuple_values);
                            builder.push(tuple_value.as_ref());
                        }
                        offset += array_len as u64;
                        offsets.push(offset);
                    }

                    match len {
                        Some(_) => {
                            let array_column = ArrayColumn {
                                values: builder.build(),
                                offsets: offsets.into(),
                            };
                            Value::Column(Column::Array(Box::new(array_column)))
                        }
                        _ => Value::Scalar(Scalar::Array(builder.build())),
                    }
                }),
            },
        }))
    });

    registry.register_function_factory("get", |params, args_type| {
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
                calc_domain: Box::new(move |_, args_domain| {
                    FunctionDomain::Domain(args_domain[0].as_tuple().unwrap()[idx].clone())
                }),
                eval: Box::new(move |args, _| match &args[0] {
                    Value::Scalar(Scalar::Tuple(fields)) => Value::Scalar(fields[idx].to_owned()),
                    Value::Column(Column::Tuple(fields)) => Value::Column(fields[idx].to_owned()),
                    _ => unreachable!(),
                }),
            },
        }))
    });

    registry.register_function_factory("get", |params, args_type| {
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
                calc_domain: Box::new(move |_, args_domain| {
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
                eval: Box::new(move |args, _| match &args[0] {
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
            },
        }))
    });

    registry.register_function_factory("get", |params, args_type| {
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
                calc_domain: Box::new(move |_, _| FunctionDomain::Full),
                eval: Box::new(move |_, _| Value::Scalar(Scalar::Null)),
            },
        }))
    });
}
