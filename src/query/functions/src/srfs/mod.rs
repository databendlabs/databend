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
                    eval: Box::new(|_, num_rows| {
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
                eval: Box::new(|args, num_rows| {
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
