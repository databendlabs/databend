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

use common_expression::types::arithmetics_type::ResultTypeOfBinary;
use common_expression::types::number::*;
use common_expression::types::DataType;
use common_expression::vectorize_with_writer_2_arg;
use common_expression::with_number_mapped_type;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;
use common_expression::NumberDomain;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_aliases("+", &["plus", "add"]);
    registry.register_aliases("-", &["minus"]);
    registry.register_aliases("*", &["multiply"]);
    registry.register_aliases("/", &["divide"]);
    registry.register_aliases("intdiv", &["div"]);

    // TODO support modulo
    // registry.register_aliases("%", &["mod", "modulo"]);

    let all_numerics_types = &[
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
    ];

    for left in all_numerics_types {
        for right in all_numerics_types {
            with_number_mapped_type!(L, match left {
                DataType::L => with_number_mapped_type!(R, match right {
                    DataType::R => {
                        {
                            type T = <(L, R) as ResultTypeOfBinary>::AddMul;
                            registry.register_2_arg::<NumberType<L>, NumberType<R>, NumberType<T>, _, _>(
                                "+",
                                FunctionProperty::default(),
                                |lhs, rhs| {
                                    Some(NumberDomain::<T> {
                                        min: lhs.min as T + rhs.min as T,
                                        max: lhs.max as T + rhs.max as T,
                                    })
                                },
                                |a, b| a as T + b as T,
                            );
                        }

                        {
                            type T = <(L, R) as ResultTypeOfBinary>::Minus;
                            registry.register_2_arg::<NumberType<L>, NumberType<R>, NumberType<T>, _, _>(
                                "-",
                                FunctionProperty::default(),
                                |lhs, rhs| {
                                    Some(NumberDomain::<T> {
                                        min: lhs.min as T - rhs.max as T,
                                        max: lhs.max as T - rhs.min as T,
                                    })
                                },
                                |a, b| a as T - b as T,
                            );
                        }

                        {
                            type T = <(L, R) as ResultTypeOfBinary>::AddMul;
                            registry.register_2_arg::<NumberType<L>, NumberType<R>, NumberType<T>, _, _>(
                                "*",
                                FunctionProperty::default(),
                                |lhs, rhs| {
                                    Some(NumberDomain::<T> {
                                        min: lhs.min as T * rhs.min as T,
                                        max: lhs.max as T * rhs.max as T,
                                    })
                                },
                                |a, b| a as T * b as T,
                            );
                        }

                        {
                            type T = f64;
                            registry.register_2_arg::<NumberType<L>, NumberType<R>, NumberType<T>, _, _>(
                                "/",
                                FunctionProperty::default(),
                                |_lhs, _rhs| {
                                    None
                                },
                                |a, b| a as T / b as T,
                            );
                        }

                        {
                            type T = <(L, R) as ResultTypeOfBinary>::IntDiv;
                            registry.register_passthrough_nullable_2_arg::<NumberType<L>, NumberType<R>,  NumberType<T>,_, _>(
                            "div",
                            FunctionProperty::default(),
                            |_, _| None,
                            vectorize_with_writer_2_arg::<NumberType<L>, NumberType<R>,  NumberType<T>>(
                                    |a, b, output| {
                                    let b = b as f64;
                                    if std::intrinsics::unlikely(b == 0.0) {
                                            return Err("Division by zero".to_string());
                                        }
                                    output.push((a as f64 / b) as T);
                                    Ok(())
                                }),
                            );
                        }
                    }
                    _ => {}
                }),
                _ => {}
            });
        }
    }
}
