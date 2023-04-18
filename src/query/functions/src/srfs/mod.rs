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
                | DataType::Array(_)),
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
                return_type: DataType::Tuple(vec![DataType::Variant]),
            },

            eval: FunctionEval::SRF {
                eval: Box::new(|args, num_rows, ctx| {
                    let val_arg = args[0].clone().to_owned();
                    let path_arg = args[1].clone().to_owned();
                    (0..num_rows)
                        .map(|row| {
                            let val = val_arg.index(row).unwrap();
                            let path = path_arg.index(row).unwrap();
                            let mut builder = StringColumnBuilder::with_capacity(0, 0);
                            if let ScalarRef::String(path) = path {
                                match parse_json_path(path) {
                                    Ok(json_path) => {
                                        if let ScalarRef::Variant(val) = val {
                                            let vals = get_by_path(val, json_path);
                                            for val in vals {
                                                builder.put(&val);
                                                builder.commit_row();
                                            }
                                        }
                                    }
                                    Err(_) => {
                                        ctx.set_error(
                                            0,
                                            format!(
                                                "Invalid JSON Path '{}'",
                                                &String::from_utf8_lossy(path),
                                            ),
                                        );
                                    }
                                }
                            }
                            let array = Column::Variant(builder.build());
                            let array_len = array.len();
                            (Value::Column(Column::Tuple(vec![array])), array_len)
                        })
                        .collect()
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
                    eval: Box::new(|_, num_rows, _| {
                        vec![(Value::Scalar(Scalar::Tuple(vec![Scalar::Null])), 0); num_rows]
                    }),
                },
            })
        }
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
                eval: Box::new(|args, num_rows, _| {
                    let arg = args[0].clone().to_owned();
                    (0..num_rows)
                        .map(|row| {
                            fn unnest_column(col: Column) -> Column {
                                match col {
                                    Column::Array(col) => unnest_column(col.underlying_column()),
                                    // Assuming that the invalid array has zero elements in the underlying column.
                                    Column::Nullable(box NullableColumn {
                                        column: Column::Array(col),
                                        ..
                                    }) => unnest_column(col.underlying_column()),
                                    _ => col,
                                }
                            }

                            match arg.index(row).unwrap() {
                                ScalarRef::Null => {
                                    (Value::Scalar(Scalar::Tuple(vec![Scalar::Null])), 0)
                                }
                                ScalarRef::Array(col) => {
                                    let unnest_array = unnest_column(col);
                                    let array_len = unnest_array.len();
                                    (Value::Column(Column::Tuple(vec![unnest_array])), array_len)
                                }
                                _ => unreachable!(),
                            }
                        })
                        .collect()
                }),
            },
        }),
    }
}
