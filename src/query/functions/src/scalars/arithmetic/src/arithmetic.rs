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
use std::str::FromStr;

use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::serialize::read_decimal_with_size;
use databend_common_expression::types::ALL_INTEGER_TYPES;
use databend_common_expression::types::ALL_NUMBER_CLASSES;
use databend_common_expression::types::ALL_NUMERICS_TYPES;
use databend_common_expression::types::ALL_UNSIGNED_INTEGER_TYPES;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::F32;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::NumberClass;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::SimpleDomain;
use databend_common_expression::types::StringType;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::nullable::NullableDomain;
use databend_common_expression::types::number::F64;
use databend_common_expression::types::number::Number;
use databend_common_expression::types::number::NumberType;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::utils::arithmetics_type::ResultTypeOfUnary;
use databend_common_expression::values::Value;
use databend_common_expression::vectorize_1_arg;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::with_integer_mapped_type;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::with_number_mapped_type_without_64;
use databend_common_expression::with_unsigned_integer_mapped_type;
use databend_functions_scalar_decimal::register_decimal_to_float;
use databend_functions_scalar_decimal::register_decimal_to_int;
use databend_functions_scalar_decimal::register_decimal_to_string;
use lexical_core::FormattedSize;
use num_traits::AsPrimitive;

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
}

macro_rules! register_bitwise_and {
    ( $lt:ty, $rt:ty, $registry:expr) => {
        type L = $lt;
        type R = $rt;
        $registry.register_2_arg::<NumberType<L>, NumberType<R>, NumberType<i64>, _>(
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
        $registry.register_2_arg::<NumberType<L>, NumberType<R>, NumberType<i64>, _>(
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
        $registry.register_2_arg::<NumberType<L>, NumberType<R>, NumberType<i64>, _>(
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
        $registry.register_2_arg::<NumberType<L>, NumberType<R>, NumberType<i64>, _>(
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
        $registry.register_2_arg::<NumberType<L>, NumberType<R>, NumberType<i64>, _>(
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

pub fn register_binary_arithmetic(registry: &mut FunctionRegistry) {
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
        $registry.register_1_arg::<NumberType<N>, NumberType<i64>, _>(
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

pub fn register_unary_arithmetic(registry: &mut FunctionRegistry) {
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
                registry.register_1_arg::<NumberType<NUM_TYPE>, NumberType<T>, _>(
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
                    .scalar_builder("minus")
                    .function()
                    .typed_1_arg::<NumberType<u64>, NumberType<i64>>()
                    .passthrough_nullable()
                    .calc_domain(|_, val| {
                        let min = (val.max as i128).wrapping_neg();
                        let max = (val.min as i128).wrapping_neg();

                        if min < i64::MIN as i128 || max > i64::MAX as i128 {
                            return FunctionDomain::MayThrow;
                        }

                        FunctionDomain::Domain(SimpleDomain::<i64> {
                            min: min as i64,
                            max: max as i64,
                        })
                    })
                    .each_row_throw(|a, _| {
                        let val = (a as i128).wrapping_neg();
                        if val < i64::MIN as i128 {
                            Err("number overflowed")
                        } else {
                            Ok(val as i64)
                        }
                    })
                    .register();
            }
            NumberClass::Int64 => {
                registry
                    .scalar_builder("minus")
                    .function()
                    .typed_1_arg::<NumberType<i64>, NumberType<i64>>()
                    .passthrough_nullable()
                    .calc_domain(|_, val| {
                        let min = val.max.checked_neg();
                        let max = val.min.checked_neg();
                        if min.is_none() || max.is_none() {
                            return FunctionDomain::MayThrow;
                        }
                        FunctionDomain::Domain(SimpleDomain::<i64> {
                            min: min.unwrap(),
                            max: max.unwrap(),
                        })
                    })
                    .each_row_throw(|a, _| match a.checked_neg() {
                        Some(a) => Ok(a),
                        None => Err("number overflowed"),
                    })
                    .register();
            }
            NumberClass::Decimal128 | NumberClass::Decimal256 => {}
        });
    }
}

#[inline]
fn parse_number<T>(
    s: &str,
    number_datatype: &NumberDataType,
    rounding_mode: bool,
) -> Result<T, <T as FromStr>::Err>
where
    T: FromStr + num_traits::Num,
{
    match s.parse::<T>() {
        Ok(v) => Ok(v),
        Err(e) => {
            if !number_datatype.is_float() {
                let decimal_pro = number_datatype.get_decimal_properties().unwrap();
                let (res, _) =
                    read_decimal_with_size::<i128>(s.as_bytes(), decimal_pro, true, rounding_mode)
                        .map_err(|_| e)?;
                format!("{}", res).parse::<T>()
            } else {
                Err(e)
            }
        }
    }
}

fn register_string_to_number(registry: &mut FunctionRegistry) {
    for dest_type in ALL_NUMERICS_TYPES {
        with_number_mapped_type!(|DEST_TYPE| match dest_type {
            NumberDataType::DEST_TYPE => {
                let name = format!("to_{dest_type}").to_lowercase();
                let data_type = DEST_TYPE::data_type();
                registry
                    .scalar_builder(&name)
                    .function()
                    .typed_1_arg::<StringType, NumberType<DEST_TYPE>>()
                    .passthrough_nullable()
                    .calc_domain(|_, _| FunctionDomain::MayThrow)
                    .vectorized(vectorize_with_builder_1_arg::<
                        StringType,
                        NumberType<DEST_TYPE>,
                    >(move |val, output, ctx| {
                        match parse_number::<DEST_TYPE>(val, &data_type, ctx.func_ctx.rounding_mode)
                        {
                            Ok(new_val) => output.push(new_val),
                            Err(e) => {
                                ctx.set_error(output.len(), e);
                                output.push(DEST_TYPE::default());
                            }
                        };
                    }))
                    .register();

                let name = format!("try_to_{dest_type}").to_lowercase();
                let data_type = DEST_TYPE::data_type();
                registry
                    .register_combine_nullable_1_arg::<StringType, NumberType<DEST_TYPE>, _, _>(
                        &name,
                        |_, _| FunctionDomain::Full,
                        vectorize_with_builder_1_arg::<
                            StringType,
                            NullableType<NumberType<DEST_TYPE>>,
                        >(move |val, output, ctx| {
                            if let Ok(new_val) = parse_number::<DEST_TYPE>(
                                val,
                                &data_type,
                                ctx.func_ctx.rounding_mode,
                            ) {
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
                    .scalar_builder("to_string")
                    .function()
                    .typed_1_arg::<NumberType<NUM_TYPE>, StringType>()
                    .passthrough_nullable()
                    .calc_domain(|_, _| FunctionDomain::Full)
                    .vectorized(|from, _| match from {
                        Value::Scalar(s) => Value::Scalar(s.to_string()),
                        Value::Column(from) => {
                            let options = NUM_TYPE::lexical_options();
                            const FORMAT: u128 = lexical_core::format::STANDARD;
                            type Native = <NUM_TYPE as Number>::Native;
                            let mut builder = StringColumnBuilder::with_capacity(from.len());

                            unsafe {
                                builder.row_buffer.resize(
                                    <NUM_TYPE as Number>::Native::FORMATTED_SIZE_DECIMAL,
                                    0,
                                );
                                for x in from.iter() {
                                    let len = lexical_core::write_with_options::<_, FORMAT>(
                                        Native::from(*x),
                                        &mut builder.row_buffer[0..],
                                        &options,
                                    )
                                    .len();
                                    builder.data.push_value(std::str::from_utf8_unchecked(
                                        &builder.row_buffer[0..len],
                                    ));
                                }
                            }
                            Value::Column(builder.build())
                        }
                    })
                    .register();
                registry.register_combine_nullable_1_arg::<NumberType<NUM_TYPE>, StringType, _, _>(
                    "try_to_string",
                    |_, _| FunctionDomain::Full,
                    |from, _| match from {
                        Value::Scalar(s) => Value::Scalar(Some(s.to_string())),
                        Value::Column(from) => {
                            let options = NUM_TYPE::lexical_options();
                            const FORMAT: u128 = lexical_core::format::STANDARD;
                            type Native = <NUM_TYPE as Number>::Native;
                            let mut builder = StringColumnBuilder::with_capacity(from.len());

                            unsafe {
                                builder.row_buffer.resize(
                                    <NUM_TYPE as Number>::Native::FORMATTED_SIZE_DECIMAL,
                                    0,
                                );
                                for x in from.iter() {
                                    let len = lexical_core::write_with_options::<_, FORMAT>(
                                        Native::from(*x),
                                        &mut builder.row_buffer[0..],
                                        &options,
                                    )
                                    .len();
                                    builder.data.push_value(std::str::from_utf8_unchecked(
                                        &builder.row_buffer[0..len],
                                    ));
                                }
                            }
                            let result = builder.build();
                            Value::Column(NullableColumn::new_unchecked(
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
                            register_lossless_cast::<SRC_TYPE, DEST_TYPE>(registry, &name);
                        } else if src_type.need_round_cast_to(*dest_type) {
                            register_round_cast::<SRC_TYPE, DEST_TYPE>(registry, &name);
                        } else {
                            register_lossy_cast::<SRC_TYPE, DEST_TYPE>(registry, &name);
                        }

                        let name = format!("try_to_{dest_type}").to_lowercase();
                        if src_type.can_lossless_cast_to(*dest_type) {
                            register_try_lossless_cast::<SRC_TYPE, DEST_TYPE>(registry, &name);
                        } else if src_type.need_round_cast_to(*dest_type) {
                            register_try_round_cast::<SRC_TYPE, DEST_TYPE>(registry, &name);
                        } else {
                            register_try_lossy_cast::<SRC_TYPE, DEST_TYPE>(registry, &name);
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

fn register_lossless_cast<
    SrcType: Number + AsPrimitive<DestType>,
    DestType: Number + AsPrimitive<SrcType>,
>(
    registry: &mut FunctionRegistry,
    name: &str,
) {
    registry.register_1_arg::<NumberType<SrcType>, NumberType<DestType>, _>(
        name,
        |_, domain| {
            let (domain, overflowing) = domain.overflow_cast();
            debug_assert!(!overflowing);
            FunctionDomain::Domain(domain)
        },
        |val, _| AsPrimitive::<DestType>::as_(val),
    );
}

fn register_round_cast<
    SrcType: Number + AsPrimitive<SrcType> + AsPrimitive<f64>,
    DestType: Number + AsPrimitive<SrcType>,
>(
    registry: &mut FunctionRegistry,
    name: &str,
) {
    registry
        .scalar_builder(name.to_string())
        .function()
        .typed_1_arg::<NumberType<SrcType>, NumberType<DestType>>()
        .passthrough_nullable()
        .calc_domain(|func_ctx, domain| {
            let (domain, overflowing) = if func_ctx.rounding_mode {
                let min = AsPrimitive::<f64>::as_(domain.min);
                let max = AsPrimitive::<f64>::as_(domain.max);
                let round_domain = SimpleDomain::<F64> {
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
        })
        .each_row_throw(|val, ctx| {
            let val = if ctx.func_ctx.rounding_mode {
                let val = AsPrimitive::<f64>::as_(val);
                num_traits::cast::cast(val.round())
            } else {
                num_traits::cast::cast(val)
            };
            match val {
                Some(new_val) => Ok(new_val),
                None => Err("number overflowed"),
            }
        })
        .register();
}

fn register_lossy_cast<
    SrcType: Number + AsPrimitive<SrcType>,
    DestType: Number + AsPrimitive<SrcType>,
>(
    registry: &mut FunctionRegistry,
    name: &str,
) {
    registry
        .scalar_builder(name.to_string())
        .function()
        .typed_1_arg::<NumberType<SrcType>, NumberType<DestType>>()
        .passthrough_nullable()
        .calc_domain(|_, domain| {
            let (domain, overflowing) = domain.overflow_cast();
            if overflowing {
                FunctionDomain::MayThrow
            } else {
                FunctionDomain::Domain(domain)
            }
        })
        .vectorized(vectorize_with_builder_1_arg::<
            NumberType<SrcType>,
            NumberType<DestType>,
        >(move |val, output, ctx| {
            if let Some(new_val) = num_traits::cast::cast(val) {
                output.push(new_val);
            } else {
                ctx.set_error(output.len(), "number overflowed");
                output.push(DestType::default());
            }
        }))
        .register();
}

fn register_try_lossless_cast<
    SrcType: Number + AsPrimitive<DestType>,
    DestType: Number + AsPrimitive<SrcType>,
>(
    registry: &mut FunctionRegistry,
    name: &str,
) {
    registry.register_combine_nullable_1_arg::<NumberType<SrcType>, NumberType<DestType>, _, _>(
        name,
        |_, domain| {
            let (domain, overflowing) = domain.overflow_cast();
            debug_assert!(!overflowing);
            FunctionDomain::Domain(NullableDomain {
                has_null: false,
                value: Some(Box::new(domain)),
            })
        },
        vectorize_1_arg::<NumberType<SrcType>, NullableType<NumberType<DestType>>>(|val, _| {
            Some(val.as_())
        }),
    );
}

fn register_try_round_cast<
    SrcType: Number + AsPrimitive<SrcType> + AsPrimitive<f64>,
    DestType: Number + AsPrimitive<SrcType>,
>(
    registry: &mut FunctionRegistry,
    name: &str,
) {
    registry.register_combine_nullable_1_arg::<NumberType<SrcType>, NumberType<DestType>, _, _>(
        name,
        |func_ctx, domain| {
            let (domain, overflowing) = if func_ctx.rounding_mode {
                let min = AsPrimitive::<f64>::as_(domain.min);
                let max = AsPrimitive::<f64>::as_(domain.max);
                let round_domain = SimpleDomain::<F64> {
                    min: min.round().into(),
                    max: max.round().into(),
                };
                round_domain.overflow_cast()
            } else {
                domain.overflow_cast()
            };
            FunctionDomain::Domain(NullableDomain {
                has_null: overflowing,
                value: Some(Box::new(domain)),
            })
        },
        vectorize_with_builder_1_arg::<NumberType<SrcType>, NullableType<NumberType<DestType>>>(
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
            },
        ),
    );
}

fn register_try_lossy_cast<
    SrcType: Number + AsPrimitive<SrcType>,
    DestType: Number + AsPrimitive<SrcType>,
>(
    registry: &mut FunctionRegistry,
    name: &str,
) {
    registry.register_combine_nullable_1_arg::<NumberType<SrcType>, NumberType<DestType>, _, _>(
        name,
        |_, domain| {
            let (domain, overflowing) = domain.overflow_cast();
            FunctionDomain::Domain(NullableDomain {
                has_null: overflowing,
                value: Some(Box::new(domain)),
            })
        },
        vectorize_with_builder_1_arg::<NumberType<SrcType>, NullableType<NumberType<DestType>>>(
            |val, output, _| {
                if let Some(new_val) = num_traits::cast::cast(val) {
                    output.push(new_val);
                } else {
                    output.push_null();
                }
            },
        ),
    );
}
