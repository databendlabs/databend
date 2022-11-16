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

use common_expression::types::nullable::NullableDomain;
use common_expression::types::number::F64;
use common_expression::types::number::*;
use common_expression::types::NullableType;
use common_expression::types::NumberDataType;
use common_expression::types::ALL_NUMERICS_TYPES;
use common_expression::utils::arithmetics_type::ResultTypeOfBinary;
use common_expression::utils::arithmetics_type::ResultTypeOfUnary;
use common_expression::vectorize_1_arg;
use common_expression::vectorize_with_builder_1_arg;
use common_expression::vectorize_with_builder_2_arg;
use common_expression::with_number_mapped_type;
use common_expression::FunctionDomain;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;
use num_traits::AsPrimitive;

use super::arithmetic_modulo::vectorize_modulo;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_aliases("plus", &["add"]);
    registry.register_aliases("minus", &["subtract", "neg"]);
    registry.register_aliases("div", &["intdiv"]);
    registry.register_aliases("modulo", &["mod"]);

    // Unary OP for minus and plus
    for left in ALL_NUMERICS_TYPES {
        with_number_mapped_type!(|NUM_TYPE| match left {
            NumberDataType::NUM_TYPE => {
                type T = <NUM_TYPE as ResultTypeOfUnary>::Negate;
                registry.register_1_arg::<NumberType<NUM_TYPE>, NumberType<T>, _, _>(
                    "minus",
                    FunctionProperty::default(),
                    |lhs| {
                        FunctionDomain::Domain(SimpleDomain::<T> {
                            min: -(lhs.max.as_(): T),
                            max: -(lhs.min.as_(): T),
                        })
                    },
                    |a, _| -(a.as_(): T),
                );
            }
        });

        // Can be eliminated by optimizer
        with_number_mapped_type!(|NUM_TYPE| match left {
            NumberDataType::NUM_TYPE => {
                registry.register_1_arg::<NumberType<NUM_TYPE>, NumberType<NUM_TYPE>, _, _>(
                    "plus",
                    FunctionProperty::default(),
                    |lhs| FunctionDomain::Domain(lhs.clone()),
                    |a, _| a,
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
                                    (|| {
                                        let lm: T =  num_traits::cast::cast(lhs.max)?;
                                        let ln: T =  num_traits::cast::cast(lhs.min)?;
                                        let rm: T =  num_traits::cast::cast(rhs.max)?;
                                        let rn: T =  num_traits::cast::cast(rhs.min)?;

                                        Some(FunctionDomain::Domain(SimpleDomain::<T> {
                                            min: ln.checked_add(rn)?,
                                            max: lm.checked_add(rm)?,
                                        }))
                                    })()
                                    .unwrap_or(FunctionDomain::Full)
                                },
                                |a, b, _| (a.as_() : T) + (b.as_() : T),
                            );
                        }

                        {
                            type T = <(L, R) as ResultTypeOfBinary>::Minus;
                            registry.register_2_arg::<NumberType<L>, NumberType<R>, NumberType<T>, _, _>(
                                "minus",
                                FunctionProperty::default(),
                                |lhs, rhs| {
                                    (|| {
                                        let lm: T =  num_traits::cast::cast(lhs.max)?;
                                        let ln: T =  num_traits::cast::cast(lhs.min)?;
                                        let rm: T =  num_traits::cast::cast(rhs.max)?;
                                        let rn: T =  num_traits::cast::cast(rhs.min)?;

                                        Some(FunctionDomain::Domain(SimpleDomain::<T> {
                                            min: ln.checked_sub(rm)?,
                                            max: lm.checked_sub(rn)?,
                                        }))
                                    })()
                                    .unwrap_or(FunctionDomain::Full)
                                },
                                |a, b, _| (a.as_() : T) - (b.as_() : T),
                            );
                        }

                        {
                            type T = <(L, R) as ResultTypeOfBinary>::AddMul;
                            registry.register_2_arg::<NumberType<L>, NumberType<R>, NumberType<T>, _, _>(
                                "multiply",
                                FunctionProperty::default(),
                                |lhs, rhs| {
                                    (|| {
                                        let lm: T =  num_traits::cast::cast(lhs.max)?;
                                        let ln: T =  num_traits::cast::cast(lhs.min)?;
                                        let rm: T =  num_traits::cast::cast(rhs.max)?;
                                        let rn: T =  num_traits::cast::cast(rhs.min)?;

                                        let x = lm.checked_mul(rm)?;
                                        let y = lm.checked_mul(rn)?;
                                        let m = ln.checked_mul(rm)?;
                                        let n = ln.checked_mul(rn)?;

                                        Some(FunctionDomain::Domain(SimpleDomain::<T> {
                                            min: x.min(y).min(m).min(n),
                                            max: x.max(y).max(m).max(n),
                                        }))
                                    })()
                                    .unwrap_or(FunctionDomain::Full)
                                },
                                |a, b, _| (a.as_() : T) * (b.as_() : T),
                            );
                        }

                        {
                            type T = F64;
                            registry.register_2_arg_core::<NumberType<L>, NumberType<R>, NumberType<T>, _, _>(
                                "divide",
                                FunctionProperty::default(),
                                |_, _| FunctionDomain::MayThrow,
                                vectorize_with_builder_2_arg::<NumberType<L>, NumberType<R>,  NumberType<T>>(
                                    |a, b, output, _| {
                                        let b = (b.as_() : T);
                                        if std::intrinsics::unlikely(b == 0.0) {
                                            return Err("/ by zero".to_string());
                                        }
                                        output.push((a.as_() : T) / b);
                                        Ok(())
                                    }),
                            );

                            registry.register_2_arg_core::<NullableType<NumberType<L>>, NullableType<NumberType<R>>,  NullableType<NumberType<T>>, _, _>(
                                "divide",
                                FunctionProperty::default(),
                                |_, _| FunctionDomain::MayThrow,
                                vectorize_with_builder_2_arg::<NullableType<NumberType<L>>, NullableType<NumberType<R>>,  NullableType<NumberType<T>>>(
                                    |a, b, output, _| {
                                    match (a,b) {
                                        (Some(a), Some(b)) => {
                                            let b = (b.as_() : T);
                                            if std::intrinsics::unlikely(b == 0.0) {
                                                return Err("/ by zero".to_string());
                                            }
                                            output.push(((a.as_() : T) / b));
                                        },
                                        _ => output.push_null(),
                                    }
                                    Ok(())
                                }),
                            )
                        }

                        {
                            type T = <(L, R) as ResultTypeOfBinary>::IntDiv;

                            registry.register_2_arg_core::<NumberType<L>, NumberType<R>,  NumberType<T>,_, _>(
                            "div",
                            FunctionProperty::default(),
                            |_, _| FunctionDomain::MayThrow,
                            vectorize_with_builder_2_arg::<NumberType<L>, NumberType<R>,  NumberType<T>>(
                                    |a, b, output, _| {
                                    let b = (b.as_() : F64);
                                    if std::intrinsics::unlikely(b == 0.0) {
                                        return Err("Division by zero".to_string());
                                    }
                                    output.push(((a.as_() : F64) / b).as_() : T);
                                    Ok(())
                                }),
                            );

                            // a   b
                            // 2   NULL (data = 0, validity = false), we should return NULL even the divisor is 0
                            registry.register_2_arg_core::<NullableType<NumberType<L>>, NullableType<NumberType<R>>,  NullableType<NumberType<T>>,_, _>(
                            "div",
                            FunctionProperty::default(),
                            |_, _| FunctionDomain::MayThrow,
                            vectorize_with_builder_2_arg::<NullableType<NumberType<L>>, NullableType<NumberType<R>>,  NullableType<NumberType<T>>>(
                                    |a, b, output, _| {
                                    match (a,b) {
                                        (Some(a), Some(b)) => {
                                            let b = (b.as_() : F64);
                                            if std::intrinsics::unlikely(b == 0.0) {
                                                return Err("Division by zero".to_string());
                                            }
                                            output.push(((a.as_() : F64) / b).as_() : T);
                                        },
                                        _ => output.push_null(),
                                    }
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
                                registry.register_2_arg_core::<NumberType<L>, NumberType<R>,  NumberType<T>,_, _>(
                                "modulo",
                                FunctionProperty::default(),
                                |_, _| FunctionDomain::MayThrow,
                                vectorize_with_builder_2_arg::<NumberType<L>, NumberType<R>,  NumberType<T>>(
                                        |a, b, output, _| {
                                        let b = (b.as_() : F64);
                                        if std::intrinsics::unlikely(b == 0.0) {
                                            return Err("Modulo by zero".to_string());
                                        }
                                        output.push(((a.as_() : M) % (b.as_() : M)).as_(): T);
                                        Ok(())
                                    }),
                                );
                            } else {
                                registry.register_2_arg_core::<NumberType<L>, NumberType<R>, NumberType<T>, _, _>(
                                    "modulo",
                                    FunctionProperty::default(),
                                    |_, _| FunctionDomain::MayThrow,
                                    vectorize_modulo::<L, R, M, T>()
                                );
                            }

                            // nullable modulo
                            registry.register_2_arg_core::<NullableType<NumberType<L>>, NullableType<NumberType<R>>,  NullableType<NumberType<T>>,_, _>(
                                "modulo",
                                FunctionProperty::default(),
                                |_, _| FunctionDomain::MayThrow,
                                vectorize_with_builder_2_arg::<NullableType<NumberType<L>>, NullableType<NumberType<R>>,  NullableType<NumberType<T>>>(
                                        |a, b, output, _| {
                                        match (a,b) {
                                            (Some(a), Some(b)) => {
                                                let b = (b.as_() : F64);
                                                if std::intrinsics::unlikely(b == 0.0) {
                                                    return Err("Modulo by zero".to_string());
                                                }
                                                output.push(((a.as_() : M) % (b.as_() : M)).as_(): T);
                                            },
                                            _ => output.push_null(),
                                        }
                                        Ok(())
                                }),
                            );
                        }
                    }
                }),
            });
        }
    }

    for src_type in ALL_NUMERICS_TYPES {
        for dest_type in ALL_NUMERICS_TYPES {
            with_number_mapped_type!(|SRC_TYPE| match src_type {
                NumberDataType::SRC_TYPE => with_number_mapped_type!(|DEST_TYPE| match dest_type {
                    NumberDataType::DEST_TYPE => {
                        let name = format!("to_{dest_type}").to_lowercase();
                        if src_type.can_lossless_cast_to(*dest_type) {
                            registry.register_1_arg::<NumberType<SRC_TYPE>, NumberType<DEST_TYPE>, _, _>(
                                &name,
                                FunctionProperty::default(),
                                |domain| {
                                    let (domain, overflowing) = domain.overflow_cast();
                                    debug_assert!(!overflowing);
                                    FunctionDomain::Domain(domain)
                                },
                                |val, _|  {
                                    val.as_()
                                },
                            );
                        } else {
                            registry.register_passthrough_nullable_1_arg::<NumberType<SRC_TYPE>, NumberType<DEST_TYPE>, _, _>(
                                &name,
                                FunctionProperty::default(),
                                |domain| {
                                    let (domain, overflowing) = domain.overflow_cast();
                                    if overflowing {
                                        FunctionDomain::MayThrow
                                    } else {
                                        FunctionDomain::Domain(domain)
                                    }
                                },
                                vectorize_with_builder_1_arg::<NumberType<SRC_TYPE>, NumberType<DEST_TYPE>>(
                                    move |val, output, _| {
                                        let new_val =
                                            num_traits::cast::cast(val).ok_or_else(|| {
                                                format!(
                                                    "unable to cast {} to {}",
                                                    val,
                                                    dest_type,
                                                )
                                            })?;
                                        output.push(new_val);
                                        Ok(())
                                    }
                                ),
                            );
                        }

                        let name = format!("try_to_{dest_type}").to_lowercase();
                        if src_type.can_lossless_cast_to(*dest_type) {
                            registry.register_combine_nullable_1_arg::<NumberType<SRC_TYPE>, NumberType<DEST_TYPE>, _, _>(
                                &name,
                                FunctionProperty::default(),
                                |domain| {
                                    let (domain, overflowing) = domain.overflow_cast();
                                    debug_assert!(!overflowing);
                                    FunctionDomain::Domain(NullableDomain {
                                        has_null: false,
                                        value: Some(Box::new(
                                            domain,
                                        )),
                                    })
                                },
                                vectorize_1_arg::<NumberType<SRC_TYPE>, NullableType<NumberType<DEST_TYPE>>>(|val, _| {
                                    Some(val.as_())
                                })
                            );
                        } else {
                            registry.register_combine_nullable_1_arg::<NumberType<SRC_TYPE>, NumberType<DEST_TYPE>, _, _>(
                                &name,
                                FunctionProperty::default(),
                                |domain| {
                                    let (domain, overflowing) = domain.overflow_cast();
                                    FunctionDomain::Domain(NullableDomain {
                                        has_null: overflowing,
                                        value: Some(Box::new(
                                            domain,
                                        )),
                                    })
                                },
                                vectorize_with_builder_1_arg::<NumberType<SRC_TYPE>, NullableType<NumberType<DEST_TYPE>>>(
                                    |val, output, _| {
                                        if let Some(new_val) = num_traits::cast::cast(val) {
                                            output.push(new_val);
                                        } else {
                                            output.push_null();
                                        }
                                        Ok(())
                                    }
                                ),
                            );
                        }
                    }
                }),
            })
        }
    }
}
