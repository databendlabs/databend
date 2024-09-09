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

#![allow(clippy::absurd_extreme_comparisons)]

use std::ops::BitAnd;
use std::ops::BitOr;
use std::ops::BitXor;
use std::sync::Arc;

use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_expression::types::decimal::DecimalDomain;
use databend_common_expression::types::decimal::DecimalType;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::nullable::NullableDomain;
use databend_common_expression::types::number::Number;
use databend_common_expression::types::number::NumberType;
use databend_common_expression::types::number::F64;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalDataType;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::NumberClass;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::SimpleDomain;
use databend_common_expression::types::StringType;
use databend_common_expression::types::ALL_FLOAT_TYPES;
use databend_common_expression::types::ALL_INTEGER_TYPES;
use databend_common_expression::types::ALL_NUMBER_CLASSES;
use databend_common_expression::types::ALL_NUMERICS_TYPES;
use databend_common_expression::types::ALL_UNSIGNED_INTEGER_TYPES;
use databend_common_expression::types::F32;
use databend_common_expression::utils::arithmetics_type::ResultTypeOfBinary;
use databend_common_expression::utils::arithmetics_type::ResultTypeOfUnary;
use databend_common_expression::values::Value;
use databend_common_expression::values::ValueRef;
use databend_common_expression::vectorize_1_arg;
use databend_common_expression::vectorize_2_arg;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::with_decimal_mapped_type;
use databend_common_expression::with_float_mapped_type;
use databend_common_expression::with_integer_mapped_type;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::with_number_mapped_type_without_64;
use databend_common_expression::with_unsigned_integer_mapped_type;
use databend_common_expression::Domain;
use databend_common_expression::EvalContext;
use databend_common_expression::Function;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use ethnum::i256;
use lexical_core::FormattedSize;
use num_traits::AsPrimitive;

use super::arithmetic_modulo::vectorize_modulo;
use super::decimal::register_decimal_to_int;
use crate::scalars::decimal::register_decimal_arithmetic;
use crate::scalars::decimal::register_decimal_to_float;
use crate::scalars::decimal::register_decimal_to_string;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_aliases("plus", &["add"]);
    registry.register_aliases("minus", &["subtract", "neg", "negate"]);
    registry.register_aliases("div", &["intdiv"]);
    registry.register_aliases("modulo", &["mod"]);
    registry.register_aliases("pow", &["power"]);

    register_unary_minus(registry);
    register_string_to_number(registry);
    register_number_to_string(registry);
    register_number_to_number(registry);
    register_binary_arithmetic(registry);
    register_unary_arithmetic(registry);
}

macro_rules! register_plus {
    ( $lt:ty, $rt:ty, $registry:expr) => {
        type L = $lt;
        type R = $rt;
        type T = <(L, R) as ResultTypeOfBinary>::AddMul;
        $registry.register_2_arg::<NumberType<L>, NumberType<R>, NumberType<T>, _, _>(
            "plus",
            |_, lhs, rhs| {
                (|| {
                    let lm: T = num_traits::cast::cast(lhs.max)?;
                    let ln: T = num_traits::cast::cast(lhs.min)?;
                    let rm: T = num_traits::cast::cast(rhs.max)?;
                    let rn: T = num_traits::cast::cast(rhs.min)?;

                    Some(FunctionDomain::Domain(SimpleDomain::<T> {
                        min: ln.checked_add(rn)?,
                        max: lm.checked_add(rm)?,
                    }))
                })()
                .unwrap_or(FunctionDomain::Full)
            },
            |a, b, _| (AsPrimitive::<T>::as_(a)) + (AsPrimitive::<T>::as_(b)),
        );
    };
}

macro_rules! register_minus {
    ( $lt:ty, $rt:ty, $registry:expr) => {
        type L = $lt;
        type R = $rt;
        type T = <(L, R) as ResultTypeOfBinary>::Minus;
        $registry.register_2_arg::<NumberType<L>, NumberType<R>, NumberType<T>, _, _>(
            "minus",
            |_, lhs, rhs| {
                (|| {
                    let lm: T = num_traits::cast::cast(lhs.max)?;
                    let ln: T = num_traits::cast::cast(lhs.min)?;
                    let rm: T = num_traits::cast::cast(rhs.max)?;
                    let rn: T = num_traits::cast::cast(rhs.min)?;

                    Some(FunctionDomain::Domain(SimpleDomain::<T> {
                        min: ln.checked_sub(rm)?,
                        max: lm.checked_sub(rn)?,
                    }))
                })()
                .unwrap_or(FunctionDomain::Full)
            },
            |a, b, _| (AsPrimitive::<T>::as_(a)) - (AsPrimitive::<T>::as_(b)),
        );
    };
}

macro_rules! register_multiply {
    ( $lt:ty, $rt:ty, $registry:expr) => {
        type L = $lt;
        type R = $rt;
        type T = <(L, R) as ResultTypeOfBinary>::AddMul;
        $registry.register_2_arg::<NumberType<L>, NumberType<R>, NumberType<T>, _, _>(
            "multiply",
            |_, lhs, rhs| {
                (|| {
                    let lm: T = num_traits::cast::cast(lhs.max)?;
                    let ln: T = num_traits::cast::cast(lhs.min)?;
                    let rm: T = num_traits::cast::cast(rhs.max)?;
                    let rn: T = num_traits::cast::cast(rhs.min)?;

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
            |a, b, _| (AsPrimitive::<T>::as_(a)) * (AsPrimitive::<T>::as_(b)),
        );
    };
}

macro_rules! register_divide {
    ( $lt:ty, $rt:ty, $registry:expr) => {
        type L = $lt;
        type R = $rt;
        type T = F64;
        $registry.register_passthrough_nullable_2_arg::<NumberType<L>, NumberType<R>, NumberType<T>, _, _>(
            "divide",

            |_, _, _| FunctionDomain::MayThrow,
            vectorize_with_builder_2_arg::<NumberType<L>, NumberType<R>,  NumberType<T>>(
                |a, b, output, ctx| {
                    let b: T = b.as_();
                    if std::intrinsics::unlikely(b == 0.0) {
                        ctx.set_error(output.len(), "divided by zero");
                        output.push(F64::default());
                    } else {
                        output.push(((AsPrimitive::<T>::as_(a)) / b));
                    }
                }),
        );
    };
}

macro_rules! register_div0 {
    ($lt:ty, $rt:ty, $registry:expr) => {
        type L = $lt;
        type R = $rt;
        type T = F64;

        $registry.register_passthrough_nullable_2_arg::<NumberType<L>, NumberType<R>, NumberType<T>, _, _>(
            "div0",
            |_, _, _| FunctionDomain::Full,
            vectorize_with_builder_2_arg::<NumberType<L>, NumberType<R>, NumberType<T>>(
                |a, b, output, _ctx| {
                    let b: F64 = b.as_();
                    if std::intrinsics::unlikely(b == 0.0) {
                        output.push(T::default()); // Push the default value for type T
                    } else {
                        output.push(AsPrimitive::<T>::as_(a) / b);
                    }
                }
            ),
        );
    };
}

macro_rules! register_divnull {
    ($lt:ty, $rt:ty, $registry:expr) => {
        type L = $lt;
        type R = $rt;
        type T = F64;

        $registry.register_2_arg_core::<NullableType<NumberType<L>>, NullableType<NumberType<R>>, NullableType<NumberType<T>>, _, _>(
            "divnull",
            |_, _, _| FunctionDomain::Full,
            vectorize_2_arg::<NullableType<NumberType<L>>, NullableType<NumberType<R>>, NullableType<NumberType<T>>>(|a, b, _| {
                match (a, b) {
                    (Some(a), Some(b)) => {
                        let b: F64 = b.as_();
                        if std::intrinsics::unlikely(b == 0.0) {
                            None
                        } else {
                            Some(AsPrimitive::<T>::as_(a) / b)
                        }
                    },
                    _ => None,
                }
            }));
    }
}

macro_rules! register_intdiv {
    ( $lt:ty, $rt:ty, $registry:expr) => {
        type L = $lt;
        type R = $rt;
        type T = <(L, R) as ResultTypeOfBinary>::IntDiv;
        $registry.register_passthrough_nullable_2_arg::<NumberType<L>, NumberType<R>,  NumberType<T>,_, _>(
            "div",

            |_, _, _| FunctionDomain::MayThrow,
            vectorize_with_builder_2_arg::<NumberType<L>, NumberType<R>, NumberType<T>>(
                |a, b, output, ctx| {
                    let b: F64 = b.as_();
                    if std::intrinsics::unlikely(b == 0.0) {
                        ctx.set_error(output.len(), "divided by zero");
                        output.push(T::default());
                    } else {
                        output.push(AsPrimitive::<T>::as_((F64::from(AsPrimitive::<f64>::as_(a))) / b));
                    }
                }
            ),
        );
    };
}

macro_rules! register_modulo {
    ( $lt:ty, $rt:ty, $registry:expr) => {
        type L = $lt;
        type R = $rt;
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
            $registry.register_passthrough_nullable_2_arg::<NumberType<L>, NumberType<R>,  NumberType<T>,_, _>(
                "modulo",

                |_, _, _| FunctionDomain::MayThrow,
                vectorize_with_builder_2_arg::<NumberType<L>, NumberType<R>,  NumberType<T>>(
                    |a, b, output, ctx| {
                        let b: F64 = b.as_();
                        if std::intrinsics::unlikely(b == 0.0) {
                            ctx.set_error(output.len(), "divided by zero");
                            output.push(T::default());
                        } else {
                            output.push(AsPrimitive::<T>::as_((AsPrimitive::<M>::as_(a)) % (AsPrimitive::<M>::as_(b))));
                        }
                    }
                ),
            );
        } else {
            $registry.register_passthrough_nullable_2_arg::<NumberType<L>, NumberType<R>, NumberType<T>, _, _>(
                "modulo",

                |_, _, _| FunctionDomain::MayThrow,
                vectorize_modulo::<L, R, M, T>()
            );
        }
    };
}

macro_rules! register_basic_arithmetic {
    ( $lt:ty, $rt:ty, $registry:expr) => {{
        register_plus!($lt, $rt, $registry);
    }
    {
        register_minus!($lt, $rt, $registry);
    }
    {
        register_multiply!($lt, $rt, $registry);
    }
    {
        register_divide!($lt, $rt, $registry);
    }
    {
        register_intdiv!($lt, $rt, $registry);
    }
    {
        register_div0!($lt, $rt, $registry);
    }
    {
        register_divnull!($lt, $rt, $registry);
    }
    {
        register_modulo!($lt, $rt, $registry);
    }};
}

macro_rules! register_bitwise_and {
    ( $lt:ty, $rt:ty, $registry:expr) => {
        type L = $lt;
        type R = $rt;
        $registry.register_2_arg::<NumberType<L>, NumberType<R>, NumberType<i64>, _, _>(
            "bit_and",
            |_, _, _| FunctionDomain::Full,
            |a, b, _| (AsPrimitive::<i64>::as_(a)).bitand(AsPrimitive::<i64>::as_(b)),
        );
    };
}

macro_rules! register_bitwise_or {
    ( $lt:ty, $rt:ty, $registry:expr) => {
        type L = $lt;
        type R = $rt;
        $registry.register_2_arg::<NumberType<L>, NumberType<R>, NumberType<i64>, _, _>(
            "bit_or",
            |_, _, _| FunctionDomain::Full,
            |a, b, _| (AsPrimitive::<i64>::as_(a)).bitor(AsPrimitive::<i64>::as_(b)),
        );
    };
}

macro_rules! register_bitwise_xor {
    ( $lt:ty, $rt:ty, $registry:expr) => {
        type L = $lt;
        type R = $rt;
        $registry.register_2_arg::<NumberType<L>, NumberType<R>, NumberType<i64>, _, _>(
            "bit_xor",
            |_, _, _| FunctionDomain::Full,
            |a, b, _| (AsPrimitive::<i64>::as_(a)).bitxor(AsPrimitive::<i64>::as_(b)),
        );
    };
}

macro_rules! register_bitwise_shift_left {
    ( $lt:ty, $rt:ty, $registry:expr) => {
        type L = $lt;
        type R = $rt;
        $registry.register_2_arg::<NumberType<L>, NumberType<R>, NumberType<i64>, _, _>(
            "bit_shift_left",
            |_, _, _| FunctionDomain::Full,
            |a, b, _| (AsPrimitive::<i64>::as_(a)) << (AsPrimitive::<u64>::as_(b)),
        );
    };
}

macro_rules! register_bitwise_shift_right {
    ( $lt:ty, $rt:ty, $registry:expr) => {
        type L = $lt;
        type R = $rt;
        $registry.register_2_arg::<NumberType<L>, NumberType<R>, NumberType<i64>, _, _>(
            "bit_shift_right",
            |_, _, _| FunctionDomain::Full,
            |a, b, _| (AsPrimitive::<i64>::as_(a)) >> (AsPrimitive::<u64>::as_(b)),
        );
    };
}

macro_rules! register_bitwise_operation {
    ( $lt:ty, $rt:ty, $registry:expr) => {{
        register_bitwise_and!($lt, $rt, $registry);
    }
    {
        register_bitwise_or!($lt, $rt, $registry);
    }
    {
        register_bitwise_xor!($lt, $rt, $registry);
    }};
}

macro_rules! register_bitwise_shift {
    ( $lt:ty, $rt:ty, $registry:expr) => {{
        register_bitwise_shift_left!($lt, $rt, $registry);
    }
    {
        register_bitwise_shift_right!($lt, $rt, $registry);
    }};
}

fn register_binary_arithmetic(registry: &mut FunctionRegistry) {
    // register basic arithmetic operation (+ - * / %)
    register_decimal_arithmetic(registry);

    for left in ALL_INTEGER_TYPES {
        for right in ALL_INTEGER_TYPES {
            with_integer_mapped_type!(|L| match left {
                NumberDataType::L => with_integer_mapped_type!(|R| match right {
                    NumberDataType::R => {
                        register_basic_arithmetic!(L, R, registry);
                    }
                    _ => unreachable!(),
                }),
                _ => unreachable!(),
            });
        }
    }

    for left in ALL_INTEGER_TYPES {
        for right in ALL_FLOAT_TYPES {
            with_integer_mapped_type!(|L| match left {
                NumberDataType::L => with_float_mapped_type!(|R| match right {
                    NumberDataType::R => {
                        register_basic_arithmetic!(L, R, registry);
                    }
                    _ => unreachable!(),
                }),
                _ => unreachable!(),
            });
        }
    }

    for left in ALL_FLOAT_TYPES {
        for right in ALL_INTEGER_TYPES {
            with_float_mapped_type!(|L| match left {
                NumberDataType::L => with_integer_mapped_type!(|R| match right {
                    NumberDataType::R => {
                        register_basic_arithmetic!(L, R, registry);
                    }
                    _ => unreachable!(),
                }),
                _ => unreachable!(),
            });
        }
    }

    for left in ALL_FLOAT_TYPES {
        for right in ALL_FLOAT_TYPES {
            with_float_mapped_type!(|L| match left {
                NumberDataType::L => with_float_mapped_type!(|R| match right {
                    NumberDataType::R => {
                        register_basic_arithmetic!(L, R, registry);
                    }
                    _ => unreachable!(),
                }),
                _ => unreachable!(),
            });
        }
    }

    // register bitwise operation : AND/OR/XOR
    for left in ALL_INTEGER_TYPES {
        for right in ALL_INTEGER_TYPES {
            with_integer_mapped_type!(|L| match left {
                NumberDataType::L => with_integer_mapped_type!(|R| match right {
                    NumberDataType::R => {
                        register_bitwise_operation!(L, R, registry);
                    }
                    _ => unreachable!(),
                }),
                _ => unreachable!(),
            });
        }
    }

    // register shift operation : shift left/shift right
    for left in ALL_INTEGER_TYPES {
        for right in ALL_UNSIGNED_INTEGER_TYPES {
            with_integer_mapped_type!(|L| match left {
                NumberDataType::L => with_unsigned_integer_mapped_type!(|R| match right {
                    NumberDataType::R => {
                        register_bitwise_shift!(L, R, registry);
                    }
                    _ => unreachable!(),
                }),
                _ => unreachable!(),
            });
        }
    }
}

macro_rules! register_bitwise_not {
    ( $n:ty, $registry:expr) => {
        type N = $n;
        $registry.register_1_arg::<NumberType<N>, NumberType<i64>, _, _>(
            "bit_not",
            |_, _| FunctionDomain::Full,
            |a, _| !(AsPrimitive::<i64>::as_(a)),
        );
    };
}

macro_rules! register_unary_arithmetic {
    ( $n:ty, $registry:expr) => {{
        register_bitwise_not!($n, $registry);
    }};
}

fn register_unary_arithmetic(registry: &mut FunctionRegistry) {
    for dest_ty in ALL_INTEGER_TYPES {
        with_integer_mapped_type!(|DEST_TYPE| match dest_ty {
            NumberDataType::DEST_TYPE => {
                register_unary_arithmetic!(DEST_TYPE, registry);
            }
            _ => unreachable!(),
        });
    }
}

fn register_unary_minus(registry: &mut FunctionRegistry) {
    for num_ty in ALL_NUMBER_CLASSES {
        with_number_mapped_type_without_64!(|NUM_TYPE| match num_ty {
            NumberClass::NUM_TYPE => {
                type T = <NUM_TYPE as ResultTypeOfUnary>::Negate;
                registry.register_1_arg::<NumberType<NUM_TYPE>, NumberType<T>, _, _>(
                    "minus",
                    |_, val| {
                        FunctionDomain::Domain(SimpleDomain::<T> {
                            min: -(AsPrimitive::<T>::as_(val.max)),
                            max: -(AsPrimitive::<T>::as_(val.min)),
                        })
                    },
                    |a, _| -(AsPrimitive::<T>::as_(a)),
                );
            }
            NumberClass::UInt64 => {
                registry
                    .register_passthrough_nullable_1_arg::<NumberType<u64>, NumberType<i64>, _, _>(
                        "minus",
                        |_, val| {
                            let min = (val.max as i128).wrapping_neg();
                            let max = (val.min as i128).wrapping_neg();

                            if min < i64::MIN as i128 || max > i64::MAX as i128 {
                                return FunctionDomain::MayThrow;
                            }

                            FunctionDomain::Domain(SimpleDomain::<i64> {
                                min: min as i64,
                                max: max as i64,
                            })
                        },
                        vectorize_with_builder_1_arg::<NumberType<u64>, NumberType<i64>>(
                            |a, output, ctx| {
                                let val = (a as i128).wrapping_neg();
                                if val < i64::MIN as i128 {
                                    ctx.set_error(output.len(), "number overflowed");
                                    output.push(0);
                                } else {
                                    output.push(val as i64);
                                }
                            },
                        ),
                    );
            }
            NumberClass::Int64 => {
                registry
                    .register_passthrough_nullable_1_arg::<NumberType<i64>, NumberType<i64>, _, _>(
                        "minus",
                        |_, val| {
                            let min = val.max.checked_neg();
                            let max = val.min.checked_neg();
                            if min.is_none() || max.is_none() {
                                return FunctionDomain::MayThrow;
                            }
                            FunctionDomain::Domain(SimpleDomain::<i64> {
                                min: min.unwrap(),
                                max: max.unwrap(),
                            })
                        },
                        vectorize_with_builder_1_arg::<NumberType<i64>, NumberType<i64>>(
                            |a, output, ctx| match a.checked_neg() {
                                Some(a) => output.push(a),
                                None => {
                                    ctx.set_error(output.len(), "number overflowed");
                                    output.push(0);
                                }
                            },
                        ),
                    );
            }

            NumberClass::Decimal128 => {
                register_decimal_minus(registry)
            }
            NumberClass::Decimal256 => {
                // already registered in Decimal128 branch
            }
        });
    }
}

pub fn register_number_to_number(registry: &mut FunctionRegistry) {
    for dest_type in ALL_NUMERICS_TYPES {
        // each out loop register all to_{dest_type}
        // dest_type not include decimal
        for src_type in ALL_NUMBER_CLASSES {
            with_number_mapped_type!(|SRC_TYPE| match src_type {
                NumberClass::SRC_TYPE => with_number_mapped_type!(|DEST_TYPE| match dest_type {
                    NumberDataType::DEST_TYPE => {
                        let src_type = src_type.get_number_type().unwrap();
                        if src_type == *dest_type {
                            continue;
                        }
                        let name = format!("to_{dest_type}").to_lowercase();
                        if src_type.can_lossless_cast_to(*dest_type) {
                            registry.register_1_arg::<NumberType<SRC_TYPE>, NumberType<DEST_TYPE>, _, _>(
                                &name,
                                |_, domain| {
                                    let (domain, overflowing) = domain.overflow_cast();
                                    debug_assert!(!overflowing);
                                    FunctionDomain::Domain(domain)
                                },
                                |val, _| val.as_()
                            );
                        } else if src_type.need_round_cast_to(*dest_type) {
                            registry.register_passthrough_nullable_1_arg::<NumberType<SRC_TYPE>, NumberType<DEST_TYPE>, _, _>(
                                &name,
                                |func_ctx, domain| {
                                    let (domain, overflowing) = if func_ctx.rounding_mode {
                                        // Perform round on domain to keep the result of domain
                                        // matches the result of function
                                        let min = AsPrimitive::<f64>::as_(domain.min);
                                        let max = AsPrimitive::<f64>::as_(domain.max);
                                        let round_domain = SimpleDomain::<F64>{
                                            min: min.round().into(),
                                            max: max.round().into(),
                                        };
                                        round_domain.overflow_cast()
                                    } else {
                                        domain.overflow_cast()
                                    };
                                    if overflowing {
                                        FunctionDomain::MayThrow
                                    } else {
                                        FunctionDomain::Domain(domain)
                                    }
                                },
                                vectorize_with_builder_1_arg::<NumberType<SRC_TYPE>, NumberType<DEST_TYPE>>(
                                    move |val, output, ctx| {
                                        let val = if ctx.func_ctx.rounding_mode {
                                            let val = AsPrimitive::<f64>::as_(val);
                                            num_traits::cast::cast(val.round())
                                        } else {
                                            num_traits::cast::cast(val)
                                        };
                                        if let Some(new_val) = val {
                                            output.push(new_val);
                                        } else {
                                            ctx.set_error(output.len(),"number overflowed");
                                            output.push(DEST_TYPE::default());
                                        }
                                    }
                                ),
                            );
                        } else {
                            registry.register_passthrough_nullable_1_arg::<NumberType<SRC_TYPE>, NumberType<DEST_TYPE>, _, _>(
                                &name,
                                |_, domain| {
                                    let (domain, overflowing) = domain.overflow_cast();
                                    if overflowing {
                                        FunctionDomain::MayThrow
                                    } else {
                                        FunctionDomain::Domain(domain)
                                    }
                                },
                                vectorize_with_builder_1_arg::<NumberType<SRC_TYPE>, NumberType<DEST_TYPE>>(
                                    move |val, output, ctx| {
                                        if let Some(new_val) = num_traits::cast::cast(val) {
                                            output.push(new_val);
                                        } else {
                                            ctx.set_error(output.len(),"number overflowed");
                                            output.push(DEST_TYPE::default());
                                        }
                                    }
                                ),
                            );
                        }

                        let name = format!("try_to_{dest_type}").to_lowercase();
                        if src_type.can_lossless_cast_to(*dest_type) {
                            registry.register_combine_nullable_1_arg::<NumberType<SRC_TYPE>, NumberType<DEST_TYPE>, _, _>(
                                &name,
                                |_, domain| {
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
                        } else if src_type.need_round_cast_to(*dest_type) {
                            registry.register_combine_nullable_1_arg::<NumberType<SRC_TYPE>, NumberType<DEST_TYPE>, _, _>(
                                &name,
                                |func_ctx, domain| {
                                    let (domain, overflowing) = if func_ctx.rounding_mode {
                                        // Perform round on domain to keep the result of domain
                                        // matches the result of function
                                        let min = AsPrimitive::<f64>::as_(domain.min);
                                        let max = AsPrimitive::<f64>::as_(domain.max);
                                        let round_domain = SimpleDomain::<F64>{
                                            min: min.round().into(),
                                            max: max.round().into(),
                                        };
                                        round_domain.overflow_cast()
                                    } else {
                                        domain.overflow_cast()
                                    };
                                    FunctionDomain::Domain(NullableDomain {
                                        has_null: overflowing,
                                        value: Some(Box::new(
                                            domain,
                                        )),
                                    })
                                },
                                vectorize_with_builder_1_arg::<NumberType<SRC_TYPE>, NullableType<NumberType<DEST_TYPE>>>(
                                    |val, output, ctx| {
                                        let val = if ctx.func_ctx.rounding_mode {
                                            let val = AsPrimitive::<f64>::as_(val);
                                            num_traits::cast::cast(val.round())
                                        } else {
                                            num_traits::cast::cast(val)
                                        };
                                        if let Some(new_val) = val {
                                            output.push(new_val);
                                        } else {
                                            output.push_null();
                                        }
                                    }
                                ),
                            );
                        } else {
                            registry.register_combine_nullable_1_arg::<NumberType<SRC_TYPE>, NumberType<DEST_TYPE>, _, _>(
                                &name,
                                |_, domain| {
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
                                    }
                                ),
                            );
                        }
                    }
                }),
                NumberClass::Decimal128 => {
                    // todo(youngsofun): add decimal try_cast and decimal to int and float
                    if matches!(dest_type, NumberDataType::Float32) {
                        register_decimal_to_float::<F32>(registry);
                    }
                    if matches!(dest_type, NumberDataType::Float64) {
                        register_decimal_to_float::<F64>(registry);
                    }

                    with_number_mapped_type!(|DEST_TYPE| match dest_type {
                        NumberDataType::DEST_TYPE => register_decimal_to_int::<DEST_TYPE>(registry),
                    })
                }
                NumberClass::Decimal256 => {
                    // already registered in Decimal128 branch
                }
            })
        }
    }
}

pub fn register_decimal_minus(registry: &mut FunctionRegistry) {
    registry.register_function_factory("minus", |_params, args_type| {
        if args_type.len() != 1 {
            return None;
        }

        let is_nullable = args_type[0].is_nullable();
        let arg_type = args_type[0].remove_nullable();
        if !arg_type.is_decimal() {
            return None;
        }

        let function = Function {
            signature: FunctionSignature {
                name: "minus".to_string(),
                args_type: vec![arg_type.clone()],
                return_type: arg_type.clone(),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, d| match &d[0] {
                    Domain::Decimal(DecimalDomain::Decimal128(d, size)) => {
                        FunctionDomain::Domain(Domain::Decimal(DecimalDomain::Decimal128(
                            SimpleDomain {
                                min: -d.max,
                                max: d.min.checked_neg().unwrap_or(i128::MAX), // Only -MIN could overflow
                            },
                            *size,
                        )))
                    }
                    Domain::Decimal(DecimalDomain::Decimal256(d, size)) => {
                        FunctionDomain::Domain(Domain::Decimal(DecimalDomain::Decimal256(
                            SimpleDomain {
                                min: -d.max,
                                max: d.min.checked_neg().unwrap_or(i256::MAX), // Only -MIN could overflow
                            },
                            *size,
                        )))
                    }
                    _ => unreachable!(),
                }),
                eval: Box::new(move |args, ctx| unary_minus_decimal(args, arg_type.clone(), ctx)),
            },
        };

        if is_nullable {
            Some(Arc::new(function.passthrough_nullable()))
        } else {
            Some(Arc::new(function))
        }
    });
}

fn unary_minus_decimal(
    args: &[ValueRef<AnyType>],
    arg_type: DataType,
    ctx: &mut EvalContext,
) -> Value<AnyType> {
    let arg = &args[0];
    let arg_type = arg_type.as_decimal().unwrap();
    with_decimal_mapped_type!(|DECIMAL_TYPE| match arg_type {
        DecimalDataType::DECIMAL_TYPE(size) => {
            type Type = DecimalType<DECIMAL_TYPE>;
            let arg = arg.try_downcast().unwrap();
            vectorize_1_arg::<Type, Type>(|t, _| -t)(arg, ctx).upcast_decimal(*size)
        }
    })
}

fn register_string_to_number(registry: &mut FunctionRegistry) {
    for dest_type in ALL_NUMERICS_TYPES {
        with_number_mapped_type!(|DEST_TYPE| match dest_type {
            NumberDataType::DEST_TYPE => {
                let name = format!("to_{dest_type}").to_lowercase();
                registry
                    .register_passthrough_nullable_1_arg::<StringType, NumberType<DEST_TYPE>, _, _>(
                        &name,
                        |_, _| FunctionDomain::MayThrow,
                        vectorize_with_builder_1_arg::<StringType, NumberType<DEST_TYPE>>(
                            move |val, output, ctx| {
                                match val.parse::<DEST_TYPE>() {
                                    Ok(new_val) => output.push(new_val),
                                    Err(e) => {
                                        ctx.set_error(output.len(), e.to_string());
                                        output.push(DEST_TYPE::default());
                                    }
                                };
                            },
                        ),
                    );

                let name = format!("try_to_{dest_type}").to_lowercase();
                registry
                    .register_combine_nullable_1_arg::<StringType, NumberType<DEST_TYPE>, _, _>(
                        &name,
                        |_, _| FunctionDomain::Full,
                        vectorize_with_builder_1_arg::<
                            StringType,
                            NullableType<NumberType<DEST_TYPE>>,
                        >(|val, output, _| {
                            if let Ok(new_val) = val.parse::<DEST_TYPE>() {
                                output.push(new_val);
                            } else {
                                output.push_null();
                            }
                        }),
                    );
            }
        });
    }
}

pub fn register_number_to_string(registry: &mut FunctionRegistry) {
    for src_type in ALL_NUMBER_CLASSES {
        with_number_mapped_type!(|NUM_TYPE| match src_type {
            NumberClass::NUM_TYPE => {
                registry
                    .register_passthrough_nullable_1_arg::<NumberType<NUM_TYPE>, StringType, _, _>(
                        "to_string",
                        |_, _| FunctionDomain::Full,
                        |from, _| match from {
                            ValueRef::Scalar(s) => Value::Scalar(s.to_string()),
                            ValueRef::Column(from) => {
                                let options = NUM_TYPE::lexical_options();
                                const FORMAT: u128 = lexical_core::format::STANDARD;

                                let mut builder =
                                    StringColumnBuilder::with_capacity(from.len(), from.len() + 1);
                                let values = &mut builder.data;

                                type Native = <NUM_TYPE as Number>::Native;
                                let mut offset: usize = 0;
                                unsafe {
                                    for x in from.iter() {
                                        values.reserve(offset + Native::FORMATTED_SIZE_DECIMAL);
                                        values.set_len(offset + Native::FORMATTED_SIZE_DECIMAL);
                                        let bytes = &mut values[offset..];

                                        let len = lexical_core::write_with_options_unchecked::<
                                            _,
                                            FORMAT,
                                        >(
                                            Native::from(*x), bytes, &options
                                        )
                                        .len();
                                        offset += len;
                                        builder.offsets.push(offset as u64);
                                    }
                                    values.set_len(offset);
                                }
                                Value::Column(builder.build())
                            }
                        },
                    );
                registry.register_combine_nullable_1_arg::<NumberType<NUM_TYPE>, StringType, _, _>(
                    "try_to_string",
                    |_, _| FunctionDomain::Full,
                    |from, _| match from {
                        ValueRef::Scalar(s) => Value::Scalar(Some(s.to_string())),
                        ValueRef::Column(from) => {
                            let options = NUM_TYPE::lexical_options();
                            const FORMAT: u128 = lexical_core::format::STANDARD;
                            let mut builder =
                                StringColumnBuilder::with_capacity(from.len(), from.len() + 1);
                            let values = &mut builder.data;

                            type Native = <NUM_TYPE as Number>::Native;
                            let mut offset: usize = 0;
                            unsafe {
                                for x in from.iter() {
                                    values.reserve(offset + Native::FORMATTED_SIZE_DECIMAL);
                                    values.set_len(offset + Native::FORMATTED_SIZE_DECIMAL);
                                    let bytes = &mut values[offset..];
                                    let len =
                                        lexical_core::write_with_options_unchecked::<_, FORMAT>(
                                            Native::from(*x),
                                            bytes,
                                            &options,
                                        )
                                        .len();
                                    offset += len;
                                    builder.offsets.push(offset as u64);
                                }
                                values.set_len(offset);
                            }
                            let result = builder.build();
                            Value::Column(NullableColumn::new(
                                result,
                                Bitmap::new_constant(true, from.len()),
                            ))
                        }
                    },
                );
            }
            NumberClass::Decimal128 => {
                register_decimal_to_string(registry)
            }
            NumberClass::Decimal256 => {
                // already registered in Decimal128 branch
            }
        });
    }
}
