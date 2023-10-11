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
use common_expression::types::AnyType;
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
use jsonb::jsonpath::parse_json_path;
use jsonb::jsonpath::Mode as SelectorMode;
use jsonb::jsonpath::Selector;
use jsonb::object_each;

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
        if (args_type[0].remove_nullable() != DataType::Variant && args_type[0] != DataType::Null)
            || (args_type[1].remove_nullable() != DataType::String
                && args_type[1] != DataType::Null)
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
                eval: Box::new(|args, ctx, max_nums_per_row| {
                    let val_arg = args[0].clone().to_owned();
                    let path_arg = args[1].clone().to_owned();
                    let mut results = Vec::with_capacity(ctx.num_rows);
                    match path_arg {
                        Value::Scalar(Scalar::String(path)) => match parse_json_path(&path) {
                            Ok(json_path) => {
                                let selector = Selector::new(json_path, SelectorMode::All);
                                for (row, max_nums_per_row) in
                                    max_nums_per_row.iter_mut().enumerate().take(ctx.num_rows)
                                {
                                    let val = unsafe { val_arg.index_unchecked(row) };
                                    let mut builder = StringColumnBuilder::with_capacity(0, 0);
                                    if let ScalarRef::Variant(val) = val {
                                        selector.select(
                                            val,
                                            &mut builder.data,
                                            &mut builder.offsets,
                                        );
                                    }
                                    let array =
                                        Column::Variant(builder.build()).wrap_nullable(None);
                                    let array_len = array.len();
                                    *max_nums_per_row = std::cmp::max(*max_nums_per_row, array_len);
                                    results.push((
                                        Value::Column(Column::Tuple(vec![array])),
                                        array_len,
                                    ));
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
                            for (row, max_nums_per_row) in
                                max_nums_per_row.iter_mut().enumerate().take(ctx.num_rows)
                            {
                                let val = unsafe { val_arg.index_unchecked(row) };
                                let path = unsafe { path_arg.index_unchecked(row) };
                                let mut builder = StringColumnBuilder::with_capacity(0, 0);
                                if let ScalarRef::String(path) = path {
                                    match parse_json_path(path) {
                                        Ok(json_path) => {
                                            if let ScalarRef::Variant(val) = val {
                                                let selector =
                                                    Selector::new(json_path, SelectorMode::All);
                                                selector.select(
                                                    val,
                                                    &mut builder.data,
                                                    &mut builder.offsets,
                                                );
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
                                let array = Column::Variant(builder.build()).wrap_nullable(None);
                                let array_len = array.len();
                                *max_nums_per_row = std::cmp::max(*max_nums_per_row, array_len);
                                results
                                    .push((Value::Column(Column::Tuple(vec![array])), array_len));
                            }
                        }
                    }
                    results
                }),
            },
        }))
    });

    registry.properties.insert(
        "json_array_elements".to_string(),
        FunctionProperty::default().kind(FunctionKind::SRF),
    );
    registry.register_function_factory("json_array_elements", |_, args_type| {
        if args_type.len() != 1 {
            return None;
        }
        if args_type[0].remove_nullable() != DataType::Variant && args_type[0] != DataType::Null {
            return None;
        }
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "json_array_elements".to_string(),
                args_type: args_type.to_vec(),
                return_type: DataType::Tuple(vec![DataType::Nullable(Box::new(DataType::Variant))]),
            },
            eval: FunctionEval::SRF {
                eval: Box::new(|args, ctx, max_nums_per_row| {
                    let arg = args[0].clone().to_owned();
                    (0..ctx.num_rows)
                        .map(|row| match arg.index(row).unwrap() {
                            ScalarRef::Null => {
                                (Value::Scalar(Scalar::Tuple(vec![Scalar::Null])), 0)
                            }
                            ScalarRef::Variant(val) => {
                                unnest_variant_array(val, row, max_nums_per_row)
                            }
                            _ => unreachable!(),
                        })
                        .collect()
                }),
            },
        }))
    });

    registry.properties.insert(
        "json_each".to_string(),
        FunctionProperty::default().kind(FunctionKind::SRF),
    );
    registry.register_function_factory("json_each", |_, args_type| {
        if args_type.len() != 1 {
            return None;
        }
        if args_type[0].remove_nullable() != DataType::Variant && args_type[0] != DataType::Null {
            return None;
        }
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "json_each".to_string(),
                args_type: args_type.to_vec(),
                return_type: DataType::Tuple(vec![DataType::Nullable(Box::new(DataType::Tuple(
                    vec![DataType::String, DataType::Variant],
                )))]),
            },
            eval: FunctionEval::SRF {
                eval: Box::new(|args, ctx, max_nums_per_row| {
                    let arg = args[0].clone().to_owned();
                    (0..ctx.num_rows)
                        .map(|row| match arg.index(row).unwrap() {
                            ScalarRef::Null => {
                                (Value::Scalar(Scalar::Tuple(vec![Scalar::Null])), 0)
                            }
                            ScalarRef::Variant(val) => {
                                unnest_variant_obj(val, row, max_nums_per_row)
                            }
                            _ => unreachable!(),
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
                    eval: Box::new(|_, ctx, _| {
                        vec![(Value::Scalar(Scalar::Tuple(vec![Scalar::Null])), 0); ctx.num_rows]
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
                eval: Box::new(|args, ctx, max_nums_per_row| {
                    let arg = args[0].clone().to_owned();
                    (0..ctx.num_rows)
                        .map(|row| match arg.index(row).unwrap() {
                            ScalarRef::Null => {
                                (Value::Scalar(Scalar::Tuple(vec![Scalar::Null])), 0)
                            }
                            ScalarRef::Variant(val) => {
                                unnest_variant_array(val, row, max_nums_per_row)
                            }
                            _ => unreachable!(),
                        })
                        .collect()
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
                eval: Box::new(|args, ctx, max_nums_per_row| {
                    let arg = args[0].clone().to_owned();
                    (0..ctx.num_rows)
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
                                    max_nums_per_row[row] =
                                        std::cmp::max(max_nums_per_row[row], array_len);
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

fn unnest_variant_array(
    val: &[u8],
    row: usize,
    max_nums_per_row: &mut [usize],
) -> (Value<AnyType>, usize) {
    match array_values(val) {
        Some(vals) if !vals.is_empty() => {
            let len = vals.len();
            let mut builder = StringColumnBuilder::with_capacity(0, 0);

            max_nums_per_row[row] = std::cmp::max(max_nums_per_row[row], len);

            for val in vals {
                builder.put_slice(&val);
                builder.commit_row();
            }

            let col = Column::Variant(builder.build()).wrap_nullable(None);
            (Value::Column(Column::Tuple(vec![col])), len)
        }
        _ => (Value::Scalar(Scalar::Tuple(vec![Scalar::Null])), 0),
    }
}

fn unnest_variant_obj(
    val: &[u8],
    row: usize,
    max_nums_per_row: &mut [usize],
) -> (Value<AnyType>, usize) {
    match object_each(val) {
        Some(vals) if !vals.is_empty() => {
            let len = vals.len();
            let mut val_builder = StringColumnBuilder::with_capacity(0, 0);
            let mut key_builder = StringColumnBuilder::with_capacity(0, 0);

            max_nums_per_row[row] = std::cmp::max(max_nums_per_row[row], len);

            for (key, val) in vals {
                key_builder.put_slice(&key);
                key_builder.commit_row();
                val_builder.put_slice(&val);
                val_builder.commit_row();
            }

            let key_col = Column::String(key_builder.build());
            let val_col = Column::Variant(val_builder.build());
            let tuple_col = Column::Tuple(vec![key_col, val_col]).wrap_nullable(None);

            (Value::Column(Column::Tuple(vec![tuple_col])), len)
        }
        _ => (Value::Scalar(Scalar::Tuple(vec![Scalar::Null])), 0),
    }
}
