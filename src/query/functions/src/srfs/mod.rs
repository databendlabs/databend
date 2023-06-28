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

use common_expression::types::nullable::NullableColumn;
use common_expression::types::string::StringColumnBuilder;
use common_expression::types::DataType;
use common_expression::Column;
use common_expression::Function;
use common_expression::FunctionEval;
use common_expression::FunctionKind;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;
use common_expression::FunctionSignature;
use common_expression::Scalar;
use common_expression::ScalarRef;
use common_expression::Value;
use jsonb::array_values;
use jsonb::get_by_path;
use jsonb::jsonpath::parse_json_path;

pub fn register(registry: &mut FunctionRegistry) {
    registry.properties.insert(
        "unnest".to_string(),
        FunctionProperty::default().kind(FunctionKind::SRF),
    );

    registry.register_function_factory("unnest", |_, arg_types: &[DataType]| {
        match arg_types {
            [
                ty @ (DataType::Null
                | DataType::EmptyArray
                | DataType::Nullable(_)
                | DataType::Array(_)
                | DataType::Variant),
            ] => Some(build_unnest(ty, Box::new(|ty| ty))),
            _ => {
                // Generate a fake function with signature `unset(Array(T0 NULL))` to have a better error message.
                Some(build_unnest(
                    &DataType::Array(Box::new(DataType::Boolean)),
                    Box::new(|ty| ty),
                ))
            }
        }
    });

    registry.properties.insert(
        "json_path_query".to_string(),
        FunctionProperty::default().kind(FunctionKind::SRF),
    );

    registry.register_function_factory("json_path_query", |_, args_type| {
        if args_type.len() != 2 {
            return None;
        }
        if args_type[0].remove_nullable() != DataType::Variant
            || args_type[1].remove_nullable() != DataType::String
        {
            return None;
        }

        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "json_path_query".to_string(),
                args_type: args_type.to_vec(),
                return_type: DataType::Tuple(vec![DataType::Nullable(Box::new(DataType::Variant))]),
            },

            eval: FunctionEval::SRF {
                eval: Box::new(|args, ctx| {
                    let val_arg = args[0].clone().to_owned();
                    let path_arg = args[1].clone().to_owned();
                    let mut lengths = vec![0; ctx.num_rows];
                    let mut builder =
                        StringColumnBuilder::with_capacity(ctx.num_rows, ctx.num_rows * 20);
                    match path_arg {
                        Value::Scalar(Scalar::Null) => {}
                        Value::Scalar(Scalar::String(path)) => match parse_json_path(&path) {
                            Ok(json_path) => {
                                for (row, length) in
                                    lengths.iter_mut().enumerate().take(ctx.num_rows)
                                {
                                    let val = unsafe { val_arg.index_unchecked(row) };
                                    if let ScalarRef::Variant(val) = val {
                                        let vals = get_by_path(val, json_path.clone());
                                        *length = vals.len();
                                        for val in vals {
                                            builder.put(&val);
                                            builder.commit_row();
                                        }
                                    }
                                }
                            }
                            Err(_) => {
                                ctx.set_error(
                                    0,
                                    format!(
                                        "Invalid JSON Path '{}'",
                                        &String::from_utf8_lossy(&path),
                                    ),
                                );
                            }
                        },
                        _ => {
                            for (row, length) in lengths.iter_mut().enumerate().take(ctx.num_rows) {
                                let val = unsafe { val_arg.index_unchecked(row) };
                                let path = unsafe { path_arg.index_unchecked(row) };
                                if let ScalarRef::String(path) = path {
                                    match parse_json_path(path) {
                                        Ok(json_path) => {
                                            if let ScalarRef::Variant(val) = val {
                                                let vals = get_by_path(val, json_path);
                                                *length = vals.len();
                                                for val in vals {
                                                    builder.put(&val);
                                                    builder.commit_row();
                                                }
                                            }
                                        }
                                        Err(_) => {
                                            ctx.set_error(
                                                row,
                                                format!(
                                                    "Invalid JSON Path '{}'",
                                                    &String::from_utf8_lossy(path),
                                                ),
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                    let array = Column::Variant(builder.build()).wrap_nullable(None);
                    (Value::Column(Column::Tuple(vec![array])), lengths)
                }),
            },
        }))
    });
}

fn build_unnest(
    arg_type: &DataType,
    wrap_type: Box<dyn Fn(DataType) -> DataType>,
) -> Arc<Function> {
    match arg_type {
        DataType::Null | DataType::EmptyArray | DataType::Nullable(box DataType::EmptyArray) => {
            Arc::new(Function {
                signature: FunctionSignature {
                    name: "unnest".to_string(),
                    args_type: vec![wrap_type(arg_type.clone())],
                    return_type: DataType::Tuple(vec![DataType::Null]),
                },
                eval: FunctionEval::SRF {
                    eval: Box::new(|_, ctx| {
                        (
                            Value::Scalar(Scalar::Tuple(vec![Scalar::Null])),
                            vec![0; ctx.num_rows],
                        )
                    }),
                },
            })
        }
        DataType::Variant | DataType::Nullable(box DataType::Variant) => Arc::new(Function {
            signature: FunctionSignature {
                name: "unnest".to_string(),
                args_type: vec![wrap_type(DataType::Nullable(Box::new(DataType::Variant)))],
                return_type: DataType::Tuple(vec![DataType::Nullable(Box::new(DataType::Variant))]),
            },
            eval: FunctionEval::SRF {
                eval: Box::new(|args, ctx| {
                    let arg = args[0].clone().to_owned();
                    let mut lengths = vec![0; ctx.num_rows];
                    let mut builder =
                        StringColumnBuilder::with_capacity(ctx.num_rows, ctx.num_rows * 20);
                    for (row, length) in lengths.iter_mut().enumerate().take(ctx.num_rows) {
                        let value = unsafe { arg.index_unchecked(row) };
                        if let ScalarRef::Variant(value) = value {
                            if let Some(vals) = array_values(value) {
                                *length = vals.len();
                                for val in vals {
                                    builder.put_slice(&val);
                                    builder.commit_row();
                                }
                            }
                        }
                    }
                    let col = Column::Variant(builder.build()).wrap_nullable(None);
                    (Value::Column(Column::Tuple(vec![col])), lengths)
                }),
            },
        }),
        DataType::Array(ty) => build_unnest(
            ty,
            Box::new(move |ty| wrap_type(DataType::Array(Box::new(ty)))),
        ),
        DataType::Nullable(box DataType::Array(ty)) => build_unnest(
            ty,
            Box::new(move |ty| {
                wrap_type(DataType::Nullable(Box::new(DataType::Array(Box::new(ty)))))
            }),
        ),
        _ => Arc::new(Function {
            signature: FunctionSignature {
                name: "unnest".to_string(),
                args_type: vec![wrap_type(DataType::Nullable(Box::new(DataType::Generic(
                    0,
                ))))],
                return_type: DataType::Tuple(vec![DataType::Nullable(Box::new(
                    DataType::Generic(0),
                ))]),
            },
            eval: FunctionEval::SRF {
                eval: Box::new(|args, ctx| {
                    let arg = args[0].clone().to_owned();
                    match arg {
                        Value::Scalar(Scalar::Array(col)) => {
                            (Value::Column(Column::Tuple(vec![col.clone()])), vec![
                                col.len(),
                            ])
                        }
                        Value::Column(Column::Array(col))
                        | Value::Column(Column::Nullable(box NullableColumn {
                            column: Column::Array(col),
                            ..
                        })) => {
                            let mut lengths = Vec::with_capacity(ctx.num_rows);
                            for i in 1..=col.len() {
                                lengths.push((col.offsets[i] - col.offsets[i - 1]) as usize);
                            }
                            let column = col.values.clone().wrap_nullable(None);
                            (Value::Column(Column::Tuple(vec![column])), lengths)
                        }
                        _ => unreachable!(),
                    }
                }),
            },
        }),
    }
}
