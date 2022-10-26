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

use common_expression::types::arithmetics_type::ResultTypeOfBinary;
use common_expression::types::arithmetics_type::ResultTypeOfUnary;
use common_expression::types::number::F64;
use common_expression::types::number::*;
use common_expression::types::NumberDataType;
use common_expression::types::ALL_NUMERICS_TYPES;
use common_expression::vectorize_with_builder_2_arg;
use common_expression::with_number_mapped_type;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;
use num_traits::AsPrimitive;

use super::arithmetic_modulo::vectorize_modulo;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_aliases("plus", &["add"]);
    registry.register_aliases("minus", &["substract", "neg"]);
    registry.register_aliases("div", &["intdiv"]);

    // TODO support modulo
    // registry.register_aliases("%", &["mod", "modulo"]);

    // Unary OP for minus and plus
    for left in ALL_NUMERICS_TYPES {
        with_number_mapped_type!(|NUM_TYPE| match left {
            NumberDataType::NUM_TYPE => {
                type T = <NUM_TYPE as ResultTypeOfUnary>::Negate;
                registry.register_1_arg::<NumberType<NUM_TYPE>, NumberType<T>, _, _>(
                    "minus",
                    FunctionProperty::default(),
                    |lhs| {
                        Some(SimpleDomain::<T> {
                            min: -(lhs.max.as_(): T),
                            max: -(lhs.min.as_(): T),
                        })
                    },
                    |a| -(a.as_(): T),
                );
            }
        });

        // Can be eliminated by optimizer
        with_number_mapped_type!(|NUM_TYPE| match left {
            NumberDataType::NUM_TYPE => {
                registry.register_1_arg::<NumberType<NUM_TYPE>, NumberType<NUM_TYPE>, _, _>(
                    "plus",
                    FunctionProperty::default(),
                    |lhs| Some(lhs.clone()),
                    |a| a,
                );
            }
        });
    }

    for left in ALL_NUMERICS_TYPES {
        for right in ALL_NUMERICS_TYPES {
            with_number_mapped_type!(|L| match left {
                NumberDataType::L => with_number_mapped_type!(|R| match right {
                    NumberDataType::R => {
                        {
                            type T = <(L, R) as ResultTypeOfBinary>::AddMul;
                            registry.register_2_arg::<NumberType<L>, NumberType<R>, NumberType<T>, _, _>(
                                "plus",
                                FunctionProperty::default(),
                                |lhs, rhs| {
                                    let lm: T =  num_traits::cast::cast(lhs.max)?;
                                    let ln: T =  num_traits::cast::cast(lhs.min)?;
                                    let rm: T =  num_traits::cast::cast(rhs.max)?;
                                    let rn: T =  num_traits::cast::cast(rhs.min)?;

                                    Some(SimpleDomain::<T> {
                                        min: ln.checked_add(rn)?,
                                        max: lm.checked_add(rm)?,
                                    })
                                },
                                |a, b| (a.as_() : T) + (b.as_() : T),
                            );
                        }

                        {
                            type T = <(L, R) as ResultTypeOfBinary>::Minus;
                            registry.register_2_arg::<NumberType<L>, NumberType<R>, NumberType<T>, _, _>(
                                "minus",
                                FunctionProperty::default(),
                                |lhs, rhs| {
                                    let lm: T =  num_traits::cast::cast(lhs.max)?;
                                    let ln: T =  num_traits::cast::cast(lhs.min)?;
                                    let rm: T =  num_traits::cast::cast(rhs.max)?;
                                    let rn: T =  num_traits::cast::cast(rhs.min)?;

                                    Some(SimpleDomain::<T> {
                                        min: ln.checked_sub(rm)?,
                                        max: lm.checked_sub(rn)?,
                                    })
                                },
                                |a, b| (a.as_() : T) - (b.as_() : T),
                            );
                        }

                        {
                            type T = <(L, R) as ResultTypeOfBinary>::AddMul;
                            registry.register_2_arg::<NumberType<L>, NumberType<R>, NumberType<T>, _, _>(
                                "multiply",
                                FunctionProperty::default(),
                                |lhs, rhs| {
                                    let lm: T =  num_traits::cast::cast(lhs.max)?;
                                    let ln: T =  num_traits::cast::cast(lhs.min)?;
                                    let rm: T =  num_traits::cast::cast(rhs.max)?;
                                    let rn: T =  num_traits::cast::cast(rhs.min)?;

                                    let x = lm.checked_mul(rm)?;
                                    let y = lm.checked_mul(rn)?;
                                    let m = ln.checked_mul(rm)?;
                                    let n = ln.checked_mul(rn)?;

                                    Some(SimpleDomain::<T> {
                                        min: x.min(y).min(m).min(n),
                                        max: x.max(y).max(m).max(n),
                                    })
                                },
                                |a, b| (a.as_() : T) * (b.as_() : T),
                            );
                        }

                        {
                            type T = F64;
                            registry.register_2_arg::<NumberType<L>, NumberType<R>, NumberType<T>, _, _>(
                                "divide",
                                FunctionProperty::default(),
                                |lhs, rhs| {
                                    let lm: T =  num_traits::cast::cast(lhs.max)?;
                                    let ln: T =  num_traits::cast::cast(lhs.min)?;
                                    let rm: T =  num_traits::cast::cast(rhs.max)?;
                                    let rn: T =  num_traits::cast::cast(rhs.min)?;

                                    let x = lm.checked_div(rm)?;
                                    let y = lm.checked_div(rn)?;
                                    let m = ln.checked_div(rm)?;
                                    let n = ln.checked_div(rn)?;

                                    Some(SimpleDomain::<T> {
                                        min: x.min(y).min(m).min(n),
                                        max: x.max(y).max(m).max(n),
                                    })
                                },
                                |a, b| (a.as_() : T) / (b.as_() : T),
                            );
                        }

                        {
                            type T = <(L, R) as ResultTypeOfBinary>::IntDiv;
                            registry.register_passthrough_nullable_2_arg::<NumberType<L>, NumberType<R>,  NumberType<T>,_, _>(
                            "div",
                            FunctionProperty::default(),
                            |_, _| None,
                            vectorize_with_builder_2_arg::<NumberType<L>, NumberType<R>,  NumberType<T>>(
                                    |a, b, output| {
                                    let b = (b.as_() : F64);
                                    if std::intrinsics::unlikely(b == 0.0) {
                                            return Err("Division by zero".to_string());
                                        }
                                    output.push(((a.as_() : F64) / b).as_() : T);
                                    Ok(())
                                }),
                            );
                        }

                        {
                            type M = <(L, R) as ResultTypeOfBinary>::LeastSuper;
                            type T = <(L, R) as ResultTypeOfBinary>::Modulo;

                            let rtype = M::data_type();
                            // slow path for modulo
                            if !matches!(
                                rtype,
                                NumberDataType::UInt8
                                    | NumberDataType::UInt16
                                    | NumberDataType::UInt32
                                    | NumberDataType::UInt64
                            ) {
                                registry.register_passthrough_nullable_2_arg::<NumberType<L>, NumberType<R>,  NumberType<T>,_, _>(
                                "modulo",
                                FunctionProperty::default(),
                                |_, _| None,
                                vectorize_with_builder_2_arg::<NumberType<L>, NumberType<R>,  NumberType<T>>(
                                        |a, b, output| {
                                        let b = (b.as_() : F64);
                                        if std::intrinsics::unlikely(b == 0.0) {
                                                return Err("Modulo by zero".to_string());
                                        }
                                        output.push(((a.as_() : M) % (b.as_() : M)).as_(): T);
                                        Ok(())
                                    }),
                                );
                            } else {
                                registry.register_passthrough_nullable_2_arg::<NumberType<L>, NumberType<R>, NumberType<T>, _, _>(
                                "modulo",
                                FunctionProperty::default(),
                                |_, _| None,
                                vectorize_modulo::<L, R, M, T>()
                                );
                            }
                        }
                    }
                }),
            });
        }
    }
}
