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

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_expression::Column;
use databend_common_expression::EvalContext;
use databend_common_expression::Function;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionFactory;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::LikePattern;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::SimpleDomainCmp;
use databend_common_expression::generate_like_pattern;
use databend_common_expression::scalar_evaluator;
use databend_common_expression::type_check;
use databend_common_expression::types::ALL_FLOAT_TYPES;
use databend_common_expression::types::ALL_INTEGER_TYPES;
use databend_common_expression::types::ALL_NUMBER_CLASSES;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::BitmapType;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::EmptyArrayType;
use databend_common_expression::types::F64;
use databend_common_expression::types::GenericType;
use databend_common_expression::types::IntervalType;
use databend_common_expression::types::MutableBitmap;
use databend_common_expression::types::Number;
use databend_common_expression::types::NumberClass;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::ReturnType;
use databend_common_expression::types::StringColumn;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::ValueType;
use databend_common_expression::types::VariantType;
use databend_common_expression::types::boolean::BooleanDomain;
use databend_common_expression::types::string::StringDomain;
use databend_common_expression::types::timestamp_tz::TimestampTzType;
use databend_common_expression::values::Value;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::with_float_mapped_type;
use databend_common_expression::with_integer_mapped_type;
use databend_common_expression::with_number_mapped_type;
use databend_common_io::deserialize_bitmap;
use databend_functions_scalar_decimal::register_decimal_compare;
use jsonb::RawJsonb;
use num_traits::AsPrimitive;
use regex::Regex;

use crate::scalars::string_multi_args::regexp;

pub fn register(registry: &mut FunctionRegistry) {
    register_variant_cmp(registry);
    register_string_cmp(registry);
    register_date_cmp(registry);
    register_timestamp_cmp(registry);
    register_timestamp_tz_cmp(registry);
    register_number_cmp(registry);
    register_string_number_cmp(registry);
    register_boolean_cmp(registry);
    register_array_cmp(registry);
    register_tuple_cmp(registry);
    register_like(registry);
    register_interval_cmp(registry);
    register_bitmap_cmp(registry);
}

pub const ALL_COMP_FUNC_NAMES: &[&str] = &["eq", "noteq", "lt", "lte", "gt", "gte"];

const ALL_TRUE_DOMAIN: BooleanDomain = BooleanDomain {
    has_true: true,
    has_false: false,
};

const ALL_FALSE_DOMAIN: BooleanDomain = BooleanDomain {
    has_true: false,
    has_false: true,
};

fn register_variant_cmp(registry: &mut FunctionRegistry) {
    registry.register_comparison_2_arg::<VariantType, VariantType, _, _>(
        "eq",
        |_, _, _| FunctionDomain::Full,
        |lhs, rhs, _| {
            let left_jsonb = RawJsonb::new(lhs);
            let right_jsonb = RawJsonb::new(rhs);
            left_jsonb.cmp(&right_jsonb) == Ordering::Equal
        },
    );
    registry.register_comparison_2_arg::<VariantType, VariantType, _, _>(
        "noteq",
        |_, _, _| FunctionDomain::Full,
        |lhs, rhs, _| {
            let left_jsonb = RawJsonb::new(lhs);
            let right_jsonb = RawJsonb::new(rhs);
            left_jsonb.cmp(&right_jsonb) != Ordering::Equal
        },
    );
    registry.register_comparison_2_arg::<VariantType, VariantType, _, _>(
        "gt",
        |_, _, _| FunctionDomain::Full,
        |lhs, rhs, _| {
            let left_jsonb = RawJsonb::new(lhs);
            let right_jsonb = RawJsonb::new(rhs);
            left_jsonb.cmp(&right_jsonb) == Ordering::Greater
        },
    );
    registry.register_comparison_2_arg::<VariantType, VariantType, _, _>(
        "gte",
        |_, _, _| FunctionDomain::Full,
        |lhs, rhs, _| {
            let left_jsonb = RawJsonb::new(lhs);
            let right_jsonb = RawJsonb::new(rhs);
            left_jsonb.cmp(&right_jsonb) != Ordering::Less
        },
    );
    registry.register_comparison_2_arg::<VariantType, VariantType, _, _>(
        "lt",
        |_, _, _| FunctionDomain::Full,
        |lhs, rhs, _| {
            let left_jsonb = RawJsonb::new(lhs);
            let right_jsonb = RawJsonb::new(rhs);
            left_jsonb.cmp(&right_jsonb) == Ordering::Less
        },
    );
    registry.register_comparison_2_arg::<VariantType, VariantType, _, _>(
        "lte",
        |_, _, _| FunctionDomain::Full,
        |lhs, rhs, _| {
            let left_jsonb = RawJsonb::new(lhs);
            let right_jsonb = RawJsonb::new(rhs);
            left_jsonb.cmp(&right_jsonb) != Ordering::Greater
        },
    );
}

macro_rules! register_simple_domain_type_cmp {
    ($registry:ident, $T:ty) => {
        $registry.register_comparison_2_arg::<$T, $T, _, _>(
            "eq",
            |_, d1, d2| d1.domain_eq(d2),
            |lhs, rhs, _| lhs == rhs,
        );
        $registry.register_comparison_2_arg::<$T, $T, _, _>(
            "noteq",
            |_, d1, d2| d1.domain_noteq(d2),
            |lhs, rhs, _| lhs != rhs,
        );
        $registry.register_comparison_2_arg::<$T, $T, _, _>(
            "gt",
            |_, d1, d2| d1.domain_gt(d2),
            |lhs, rhs, _| lhs > rhs,
        );
        $registry.register_comparison_2_arg::<$T, $T, _, _>(
            "gte",
            |_, d1, d2| d1.domain_gte(d2),
            |lhs, rhs, _| lhs >= rhs,
        );
        $registry.register_comparison_2_arg::<$T, $T, _, _>(
            "lt",
            |_, d1, d2| d1.domain_lt(d2),
            |lhs, rhs, _| lhs < rhs,
        );
        $registry.register_comparison_2_arg::<$T, $T, _, _>(
            "lte",
            |_, d1, d2| d1.domain_lte(d2),
            |lhs, rhs, _| lhs <= rhs,
        );
    };
}

fn register_string_cmp(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_2_arg::<StringType, StringType, BooleanType, _, _>(
        "eq",
        |_, d1, d2| d1.domain_eq(d2),
        vectorize_string_cmp(|cmp| cmp == Ordering::Equal),
    );
    registry.register_passthrough_nullable_2_arg::<StringType, StringType, BooleanType, _, _>(
        "noteq",
        |_, d1, d2| d1.domain_noteq(d2),
        vectorize_string_cmp(|cmp| cmp != Ordering::Equal),
    );
    registry.register_passthrough_nullable_2_arg::<StringType, StringType, BooleanType, _, _>(
        "gt",
        |_, d1, d2| d1.domain_gt(d2),
        vectorize_string_cmp(|cmp| cmp == Ordering::Greater),
    );
    registry.register_passthrough_nullable_2_arg::<StringType, StringType, BooleanType, _, _>(
        "gte",
        |_, d1, d2| d1.domain_gte(d2),
        vectorize_string_cmp(|cmp| cmp != Ordering::Less),
    );
    registry.register_passthrough_nullable_2_arg::<StringType, StringType, BooleanType, _, _>(
        "lt",
        |_, d1, d2| d1.domain_lt(d2),
        vectorize_string_cmp(|cmp| cmp == Ordering::Less),
    );
    registry.register_passthrough_nullable_2_arg::<StringType, StringType, BooleanType, _, _>(
        "lte",
        |_, d1, d2| d1.domain_lte(d2),
        vectorize_string_cmp(|cmp| cmp != Ordering::Greater),
    );
}

fn vectorize_string_cmp(
    func: impl Fn(Ordering) -> bool + Copy,
) -> impl Fn(Value<StringType>, Value<StringType>, &mut EvalContext) -> Value<BooleanType> + Copy {
    move |arg1, arg2, ctx| match (arg1, arg2) {
        (Value::Scalar(arg1), Value::Scalar(arg2)) => Value::Scalar(func(arg1.cmp(&arg2))),
        (Value::Column(arg1), Value::Scalar(arg2)) => {
            let col = Bitmap::collect_bool(ctx.num_rows, |i| {
                func(StringColumn::compare_str(&arg1, i, &arg2))
            });
            Value::Column(col)
        }
        (Value::Scalar(arg1), Value::Column(arg2)) => {
            let col = Bitmap::collect_bool(ctx.num_rows, |i| {
                func(StringColumn::compare_str(&arg2, i, &arg1).reverse())
            });
            Value::Column(col)
        }
        (Value::Column(arg1), Value::Column(arg2)) => {
            let col = Bitmap::collect_bool(ctx.num_rows, |i| {
                func(StringColumn::compare(&arg1, i, &arg2, i))
            });
            Value::Column(col)
        }
    }
}

fn register_date_cmp(registry: &mut FunctionRegistry) {
    register_simple_domain_type_cmp!(registry, DateType);
}

fn register_timestamp_cmp(registry: &mut FunctionRegistry) {
    register_simple_domain_type_cmp!(registry, TimestampType);
}

fn register_timestamp_tz_cmp(registry: &mut FunctionRegistry) {
    register_simple_domain_type_cmp!(registry, TimestampTzType);
}

fn register_interval_cmp(registry: &mut FunctionRegistry) {
    register_simple_domain_type_cmp!(registry, IntervalType);
}

fn register_boolean_cmp(registry: &mut FunctionRegistry) {
    registry.register_comparison_2_arg::<BooleanType, BooleanType, _, _>(
        "eq",
        |_, d1, d2| match (d1.has_true, d1.has_false, d2.has_true, d2.has_false) {
            (true, false, true, false) => FunctionDomain::Domain(ALL_TRUE_DOMAIN),
            (false, true, false, true) => FunctionDomain::Domain(ALL_TRUE_DOMAIN),
            (true, false, false, true) => FunctionDomain::Domain(ALL_FALSE_DOMAIN),
            (false, true, true, false) => FunctionDomain::Domain(ALL_FALSE_DOMAIN),
            _ => FunctionDomain::Full,
        },
        |lhs, rhs, _| lhs == rhs,
    );
    registry.register_comparison_2_arg::<BooleanType, BooleanType, _, _>(
        "noteq",
        |_, d1, d2| match (d1.has_true, d1.has_false, d2.has_true, d2.has_false) {
            (true, false, true, false) => FunctionDomain::Domain(ALL_FALSE_DOMAIN),
            (false, true, false, true) => FunctionDomain::Domain(ALL_FALSE_DOMAIN),
            (true, false, false, true) => FunctionDomain::Domain(ALL_TRUE_DOMAIN),
            (false, true, true, false) => FunctionDomain::Domain(ALL_TRUE_DOMAIN),
            _ => FunctionDomain::Full,
        },
        |lhs, rhs, _| lhs != rhs,
    );
    registry.register_comparison_2_arg::<BooleanType, BooleanType, _, _>(
        "gt",
        |_, d1, d2| match (d1.has_true, d1.has_false, d2.has_true, d2.has_false) {
            (true, false, false, true) => FunctionDomain::Domain(ALL_TRUE_DOMAIN),
            (false, true, _, _) => FunctionDomain::Domain(ALL_FALSE_DOMAIN),
            _ => FunctionDomain::Full,
        },
        |lhs, rhs, _| lhs & !rhs,
    );
    registry.register_comparison_2_arg::<BooleanType, BooleanType, _, _>(
        "gte",
        |_, d1, d2| match (d1.has_true, d1.has_false, d2.has_true, d2.has_false) {
            (true, false, _, _) => FunctionDomain::Domain(ALL_TRUE_DOMAIN),
            (_, _, false, true) => FunctionDomain::Domain(ALL_TRUE_DOMAIN),
            (false, true, true, false) => FunctionDomain::Domain(ALL_FALSE_DOMAIN),
            _ => FunctionDomain::Full,
        },
        |lhs, rhs, _| lhs | !rhs,
    );
    registry.register_comparison_2_arg::<BooleanType, BooleanType, _, _>(
        "lt",
        |_, d1, d2| match (d1.has_true, d1.has_false, d2.has_true, d2.has_false) {
            (false, true, true, false) => FunctionDomain::Domain(ALL_TRUE_DOMAIN),
            (_, _, false, true) => FunctionDomain::Domain(ALL_FALSE_DOMAIN),
            _ => FunctionDomain::Full,
        },
        |lhs, rhs, _| !lhs & rhs,
    );
    registry.register_comparison_2_arg::<BooleanType, BooleanType, _, _>(
        "lte",
        |_, d1, d2| match (d1.has_true, d1.has_false, d2.has_true, d2.has_false) {
            (false, true, _, _) => FunctionDomain::Domain(ALL_TRUE_DOMAIN),
            (_, _, true, false) => FunctionDomain::Domain(ALL_TRUE_DOMAIN),
            (true, false, false, true) => FunctionDomain::Domain(ALL_FALSE_DOMAIN),
            _ => FunctionDomain::Full,
        },
        |lhs, rhs, _| !lhs | rhs,
    );
}

fn register_number_cmp(registry: &mut FunctionRegistry) {
    for ty in ALL_NUMBER_CLASSES {
        with_number_mapped_type!(|NUM_TYPE| match ty {
            NumberClass::NUM_TYPE => {
                register_simple_domain_type_cmp!(registry, NumberType<NUM_TYPE>);
            }
            NumberClass::Decimal128 => {
                register_decimal_compare(registry)
            }
            NumberClass::Decimal256 => {
                // already registered in Decimal128 branch
            }
        });
    }
}

fn register_string_number_cmp(registry: &mut FunctionRegistry) {
    for num_type in ALL_INTEGER_TYPES {
        with_integer_mapped_type!(|NUM_TYPE| match num_type {
            NumberDataType::NUM_TYPE => {
                registry
                    .register_passthrough_nullable_2_arg::<StringType, NumberType<NUM_TYPE>, BooleanType, _, _>(
                        "eq",
                        |_, _, _| FunctionDomain::MayThrow,
                        vectorize_string_integer_cmp(|ord| ord == Ordering::Equal),
                    );
                registry
                    .register_passthrough_nullable_2_arg::<StringType, NumberType<NUM_TYPE>, BooleanType, _, _>(
                        "noteq",
                        |_, _, _| FunctionDomain::MayThrow,
                        vectorize_string_integer_cmp(|ord| ord != Ordering::Equal),
                    );
                registry
                    .register_passthrough_nullable_2_arg::<StringType, NumberType<NUM_TYPE>, BooleanType, _, _>(
                        "gt",
                        |_, _, _| FunctionDomain::MayThrow,
                        vectorize_string_integer_cmp(|ord| ord == Ordering::Greater),
                    );
                registry
                    .register_passthrough_nullable_2_arg::<StringType, NumberType<NUM_TYPE>, BooleanType, _, _>(
                        "gte",
                        |_, _, _| FunctionDomain::MayThrow,
                        vectorize_string_integer_cmp(|ord| ord != Ordering::Less),
                    );
                registry
                    .register_passthrough_nullable_2_arg::<StringType, NumberType<NUM_TYPE>, BooleanType, _, _>(
                        "lt",
                        |_, _, _| FunctionDomain::MayThrow,
                        vectorize_string_integer_cmp(|ord| ord == Ordering::Less),
                    );
                registry
                    .register_passthrough_nullable_2_arg::<StringType, NumberType<NUM_TYPE>, BooleanType, _, _>(
                        "lte",
                        |_, _, _| FunctionDomain::MayThrow,
                        vectorize_string_integer_cmp(|ord| ord != Ordering::Greater),
                    );

                registry
                    .register_passthrough_nullable_2_arg::<NumberType<NUM_TYPE>, StringType, BooleanType, _, _>(
                        "eq",
                        |_, _, _| FunctionDomain::MayThrow,
                        vectorize_integer_string_cmp(|ord| ord.reverse() == Ordering::Equal),
                    );
                registry
                    .register_passthrough_nullable_2_arg::<NumberType<NUM_TYPE>, StringType, BooleanType, _, _>(
                        "noteq",
                        |_, _, _| FunctionDomain::MayThrow,
                        vectorize_integer_string_cmp(|ord| ord.reverse() != Ordering::Equal),
                    );
                registry
                    .register_passthrough_nullable_2_arg::<NumberType<NUM_TYPE>, StringType, BooleanType, _, _>(
                        "gt",
                        |_, _, _| FunctionDomain::MayThrow,
                        vectorize_integer_string_cmp(|ord| ord.reverse() == Ordering::Greater),
                    );
                registry
                    .register_passthrough_nullable_2_arg::<NumberType<NUM_TYPE>, StringType, BooleanType, _, _>(
                        "gte",
                        |_, _, _| FunctionDomain::MayThrow,
                        vectorize_integer_string_cmp(|ord| ord.reverse() != Ordering::Less),
                    );
                registry
                    .register_passthrough_nullable_2_arg::<NumberType<NUM_TYPE>, StringType, BooleanType, _, _>(
                        "lt",
                        |_, _, _| FunctionDomain::MayThrow,
                        vectorize_integer_string_cmp(|ord| ord.reverse() == Ordering::Less),
                    );
                registry
                    .register_passthrough_nullable_2_arg::<NumberType<NUM_TYPE>, StringType, BooleanType, _, _>(
                        "lte",
                        |_, _, _| FunctionDomain::MayThrow,
                        vectorize_integer_string_cmp(|ord| ord.reverse() != Ordering::Greater),
                    );
            }
            _ => {}
        });
    }

    for num_type in ALL_FLOAT_TYPES {
        with_float_mapped_type!(|NUM_TYPE| match num_type {
            NumberDataType::NUM_TYPE => {
                registry
                    .register_passthrough_nullable_2_arg::<StringType, NumberType<NUM_TYPE>, BooleanType, _, _>(
                        "eq",
                        |_, _, _| FunctionDomain::MayThrow,
                        vectorize_string_float_cmp(|ord| ord == Ordering::Equal),
                    );
                registry
                    .register_passthrough_nullable_2_arg::<StringType, NumberType<NUM_TYPE>, BooleanType, _, _>(
                        "noteq",
                        |_, _, _| FunctionDomain::MayThrow,
                        vectorize_string_float_cmp(|ord| ord != Ordering::Equal),
                    );
                registry
                    .register_passthrough_nullable_2_arg::<StringType, NumberType<NUM_TYPE>, BooleanType, _, _>(
                        "gt",
                        |_, _, _| FunctionDomain::MayThrow,
                        vectorize_string_float_cmp(|ord| ord == Ordering::Greater),
                    );
                registry
                    .register_passthrough_nullable_2_arg::<StringType, NumberType<NUM_TYPE>, BooleanType, _, _>(
                        "gte",
                        |_, _, _| FunctionDomain::MayThrow,
                        vectorize_string_float_cmp(|ord| ord != Ordering::Less),
                    );
                registry
                    .register_passthrough_nullable_2_arg::<StringType, NumberType<NUM_TYPE>, BooleanType, _, _>(
                        "lt",
                        |_, _, _| FunctionDomain::MayThrow,
                        vectorize_string_float_cmp(|ord| ord == Ordering::Less),
                    );
                registry
                    .register_passthrough_nullable_2_arg::<StringType, NumberType<NUM_TYPE>, BooleanType, _, _>(
                        "lte",
                        |_, _, _| FunctionDomain::MayThrow,
                        vectorize_string_float_cmp(|ord| ord != Ordering::Greater),
                    );

                registry
                    .register_passthrough_nullable_2_arg::<NumberType<NUM_TYPE>, StringType, BooleanType, _, _>(
                        "eq",
                        |_, _, _| FunctionDomain::MayThrow,
                        vectorize_float_string_cmp(|ord| ord.reverse() == Ordering::Equal),
                    );
                registry
                    .register_passthrough_nullable_2_arg::<NumberType<NUM_TYPE>, StringType, BooleanType, _, _>(
                        "noteq",
                        |_, _, _| FunctionDomain::MayThrow,
                        vectorize_float_string_cmp(|ord| ord.reverse() != Ordering::Equal),
                    );
                registry
                    .register_passthrough_nullable_2_arg::<NumberType<NUM_TYPE>, StringType, BooleanType, _, _>(
                        "gt",
                        |_, _, _| FunctionDomain::MayThrow,
                        vectorize_float_string_cmp(|ord| ord.reverse() == Ordering::Greater),
                    );
                registry
                    .register_passthrough_nullable_2_arg::<NumberType<NUM_TYPE>, StringType, BooleanType, _, _>(
                        "gte",
                        |_, _, _| FunctionDomain::MayThrow,
                        vectorize_float_string_cmp(|ord| ord.reverse() != Ordering::Less),
                    );
                registry
                    .register_passthrough_nullable_2_arg::<NumberType<NUM_TYPE>, StringType, BooleanType, _, _>(
                        "lt",
                        |_, _, _| FunctionDomain::MayThrow,
                        vectorize_float_string_cmp(|ord| ord.reverse() == Ordering::Less),
                    );
                registry
                    .register_passthrough_nullable_2_arg::<NumberType<NUM_TYPE>, StringType, BooleanType, _, _>(
                        "lte",
                        |_, _, _| FunctionDomain::MayThrow,
                        vectorize_float_string_cmp(|ord| ord.reverse() != Ordering::Greater),
                    );
            }
            _ => {}
        });
    }
}

fn vectorize_string_integer_cmp<T: Number + std::str::FromStr + num_traits::AsPrimitive<F64>>(
    func: impl Fn(Ordering) -> bool + Copy,
) -> impl Fn(Value<StringType>, Value<NumberType<T>>, &mut EvalContext) -> Value<BooleanType> + Copy
{
    move |arg1, arg2, ctx| string_integer_cmp(arg1, arg2, ctx, func)
}

fn vectorize_integer_string_cmp<T: Number + std::str::FromStr + num_traits::AsPrimitive<F64>>(
    func: impl Fn(Ordering) -> bool + Copy,
) -> impl Fn(Value<NumberType<T>>, Value<StringType>, &mut EvalContext) -> Value<BooleanType> + Copy
{
    move |arg1, arg2, ctx| string_integer_cmp(arg2, arg1, ctx, func)
}

fn string_integer_cmp<T: Number + std::str::FromStr + num_traits::AsPrimitive<F64>>(
    arg1: Value<StringType>,
    arg2: Value<NumberType<T>>,
    ctx: &mut EvalContext,
    func: impl Fn(Ordering) -> bool + Copy,
) -> Value<BooleanType> {
    let input_all_scalars = arg1.as_scalar().is_some() && arg2.as_scalar().is_some();
    let process_rows = if input_all_scalars { 1 } else { ctx.num_rows };
    let mut builder = MutableBitmap::with_capacity(process_rows);
    match arg1 {
        Value::Scalar(arg1) => {
            if let Ok(val1) = arg1.parse::<T>() {
                for index in 0..process_rows {
                    let val2 = unsafe { arg2.index_unchecked(index) };
                    let ord = val1.cmp(&val2);
                    builder.push(func(ord));
                }
            } else {
                match arg1.parse::<F64>() {
                    Ok(val1) => {
                        for index in 0..process_rows {
                            let val2 = unsafe { arg2.index_unchecked(index) };
                            let val2 = AsPrimitive::<F64>::as_(val2);
                            let ord = val1.cmp(&val2);
                            builder.push(func(ord));
                        }
                    }
                    Err(_) => {
                        ctx.set_error(0, format!("Could not convert string '{}' to number", arg1));
                        for _ in 0..process_rows {
                            builder.push(false);
                        }
                    }
                }
            }
        }
        Value::Column(arg1) => {
            for index in 0..process_rows {
                let val1 = unsafe { arg1.index_unchecked(index) };
                let val2 = unsafe { arg2.index_unchecked(index) };
                if let Ok(val1) = val1.parse::<T>() {
                    let ord = val1.cmp(&val2);
                    builder.push(func(ord));
                } else {
                    match val1.parse::<F64>() {
                        Ok(val1) => {
                            let val2 = AsPrimitive::<F64>::as_(val2);
                            let ord = val1.cmp(&val2);
                            builder.push(func(ord));
                        }
                        Err(_) => {
                            ctx.set_error(
                                builder.len(),
                                format!("Could not convert string '{}' to number", val1),
                            );
                            builder.push(false);
                        }
                    }
                }
            }
        }
    }
    if input_all_scalars {
        Value::Scalar(builder.get(0))
    } else {
        Value::Column(builder.into())
    }
}

fn vectorize_string_float_cmp<T: Number + std::str::FromStr>(
    func: impl Fn(Ordering) -> bool + Copy,
) -> impl Fn(Value<StringType>, Value<NumberType<T>>, &mut EvalContext) -> Value<BooleanType> + Copy
{
    move |arg1, arg2, ctx| string_float_cmp(arg1, arg2, ctx, func)
}

fn vectorize_float_string_cmp<T: Number + std::str::FromStr>(
    func: impl Fn(Ordering) -> bool + Copy,
) -> impl Fn(Value<NumberType<T>>, Value<StringType>, &mut EvalContext) -> Value<BooleanType> + Copy
{
    move |arg1, arg2, ctx| string_float_cmp(arg2, arg1, ctx, func)
}

fn string_float_cmp<T: Number + std::str::FromStr>(
    arg1: Value<StringType>,
    arg2: Value<NumberType<T>>,
    ctx: &mut EvalContext,
    func: impl Fn(Ordering) -> bool + Copy,
) -> Value<BooleanType> {
    let input_all_scalars = arg1.as_scalar().is_some() && arg2.as_scalar().is_some();
    let process_rows = if input_all_scalars { 1 } else { ctx.num_rows };
    let mut builder = MutableBitmap::with_capacity(process_rows);
    match arg1 {
        Value::Scalar(arg1) => match arg1.parse::<T>() {
            Ok(val1) => {
                for index in 0..process_rows {
                    let val2 = unsafe { arg2.index_unchecked(index) };
                    let ord = val1.cmp(&val2);
                    builder.push(func(ord));
                }
            }
            Err(_) => {
                ctx.set_error(0, format!("Could not convert string '{}' to number", arg1));
                for _ in 0..process_rows {
                    builder.push(false);
                }
            }
        },
        Value::Column(arg1) => {
            for index in 0..process_rows {
                let val1 = unsafe { arg1.index_unchecked(index) };
                let val2 = unsafe { arg2.index_unchecked(index) };
                match val1.parse::<T>() {
                    Ok(val1) => {
                        let ord = val1.cmp(&val2);
                        builder.push(func(ord));
                    }
                    Err(_) => {
                        ctx.set_error(
                            builder.len(),
                            format!("Could not convert string '{}' to number", val1),
                        );
                        builder.push(false);
                    }
                }
            }
        }
    }
    if input_all_scalars {
        Value::Scalar(builder.get(0))
    } else {
        Value::Column(builder.into())
    }
}

fn register_array_cmp(registry: &mut FunctionRegistry) {
    registry.register_2_arg::<EmptyArrayType, EmptyArrayType, BooleanType, _>(
        "eq",
        |_, _, _| FunctionDomain::Domain(ALL_TRUE_DOMAIN),
        |_, _, _| true,
    );
    registry.register_2_arg::<EmptyArrayType, EmptyArrayType, BooleanType, _>(
        "noteq",
        |_, _, _| FunctionDomain::Domain(ALL_FALSE_DOMAIN),
        |_, _, _| false,
    );
    registry.register_2_arg::<EmptyArrayType, EmptyArrayType, BooleanType, _>(
        "gt",
        |_, _, _| FunctionDomain::Domain(ALL_FALSE_DOMAIN),
        |_, _, _| false,
    );
    registry.register_2_arg::<EmptyArrayType, EmptyArrayType, BooleanType, _>(
        "gte",
        |_, _, _| FunctionDomain::Domain(ALL_TRUE_DOMAIN),
        |_, _, _| true,
    );
    registry.register_2_arg::<EmptyArrayType, EmptyArrayType, BooleanType, _>(
        "lt",
        |_, _, _| FunctionDomain::Domain(ALL_FALSE_DOMAIN),
        |_, _, _| false,
    );
    registry.register_2_arg::<EmptyArrayType, EmptyArrayType, BooleanType, _>(
        "lte",
        |_, _, _| FunctionDomain::Domain(ALL_TRUE_DOMAIN),
        |_, _, _| true,
    );

    registry
        .register_2_arg::<ArrayType<GenericType<0>>, ArrayType<GenericType<0>>, BooleanType, _>(
            "eq",
            |_, _, _| FunctionDomain::Full,
            |lhs, rhs, _| lhs == rhs,
        );
    registry
        .register_2_arg::<ArrayType<GenericType<0>>, ArrayType<GenericType<0>>, BooleanType, _>(
            "noteq",
            |_, _, _| FunctionDomain::Full,
            |lhs, rhs, _| lhs != rhs,
        );
    registry
        .register_2_arg::<ArrayType<GenericType<0>>, ArrayType<GenericType<0>>, BooleanType, _>(
            "gt",
            |_, _, _| FunctionDomain::Full,
            |lhs, rhs, _| lhs > rhs,
        );
    registry
        .register_2_arg::<ArrayType<GenericType<0>>, ArrayType<GenericType<0>>, BooleanType, _>(
            "gte",
            |_, _, _| FunctionDomain::Full,
            |lhs, rhs, _| lhs >= rhs,
        );
    registry
        .register_2_arg::<ArrayType<GenericType<0>>, ArrayType<GenericType<0>>, BooleanType, _>(
            "lt",
            |_, _, _| FunctionDomain::Full,
            |lhs, rhs, _| lhs < rhs,
        );
    registry
        .register_2_arg::<ArrayType<GenericType<0>>, ArrayType<GenericType<0>>, BooleanType, _>(
            "lte",
            |_, _, _| FunctionDomain::Full,
            |lhs, rhs, _| lhs <= rhs,
        );
}

fn register_tuple_cmp(registry: &mut FunctionRegistry) {
    fn register_tuple_cmp_op(
        registry: &mut FunctionRegistry,
        name: &str,
        default_result: bool,
        // Compare the fields of each row from left to right, break on the first `Some()` result.
        // If all fields are `None`, return `default_result`.
        cmp_op: impl Fn(ScalarRef, ScalarRef) -> Option<bool> + 'static + Send + Sync + Copy,
    ) {
        let name_cloned = name.to_string();
        let factory = FunctionFactory::Closure(Box::new(move |_, args_type: &[DataType]| {
            let fields_generics = match args_type {
                [DataType::Tuple(lhs_fields_ty), _] => (0..lhs_fields_ty.len())
                    .map(DataType::Generic)
                    .collect::<Vec<_>>(),
                _ => return None,
            };
            Some(Arc::new(Function {
                signature: FunctionSignature {
                    name: name_cloned.clone(),
                    args_type: vec![
                        DataType::Tuple(fields_generics.clone()),
                        DataType::Tuple(fields_generics),
                    ],
                    return_type: DataType::Boolean,
                },
                eval: FunctionEval::Scalar {
                    calc_domain: Box::new(FunctionDomain::Full),
                    eval: scalar_evaluator(move |args, _| {
                        let len = args.iter().find_map(|arg| match arg {
                            Value::Column(col) => Some(col.len()),
                            _ => None,
                        });

                        let lhs_fields: Vec<Value<AnyType>> = match &args[0] {
                            Value::Scalar(Scalar::Tuple(fields)) => {
                                fields.iter().cloned().map(Value::Scalar).collect()
                            }
                            Value::Column(Column::Tuple(fields)) => {
                                fields.iter().cloned().map(Value::Column).collect()
                            }
                            _ => unreachable!(),
                        };
                        let rhs_fields: Vec<Value<AnyType>> = match &args[1] {
                            Value::Scalar(Scalar::Tuple(fields)) => {
                                fields.iter().cloned().map(Value::Scalar).collect()
                            }
                            Value::Column(Column::Tuple(fields)) => {
                                fields.iter().cloned().map(Value::Column).collect()
                            }
                            _ => unreachable!(),
                        };

                        let size = len.unwrap_or(1);
                        let mut builder = BooleanType::create_builder(size, &[]);

                        'outer: for row in 0..size {
                            for (lhs_field, rhs_field) in lhs_fields.iter().zip(&rhs_fields) {
                                let lhs = lhs_field.index(row).unwrap();
                                let rhs = rhs_field.index(row).unwrap();
                                if let Some(result) = cmp_op(lhs, rhs) {
                                    builder.push(result);
                                    continue 'outer;
                                }
                            }
                            builder.push(default_result);
                        }

                        match len {
                            Some(_) => {
                                let col =
                                    BooleanType::upcast_column(BooleanType::build_column(builder));
                                Value::Column(col)
                            }
                            _ => Value::Scalar(BooleanType::upcast_scalar(
                                BooleanType::build_scalar(builder),
                            )),
                        }
                    }),
                    derive_stat: None,
                },
            }))
        }));
        registry.register_function_factory(name, factory);
    }

    register_tuple_cmp_op(registry, "eq", true, |lhs, rhs| {
        if lhs != rhs { Some(false) } else { None }
    });
    register_tuple_cmp_op(registry, "noteq", false, |lhs, rhs| {
        if lhs != rhs { Some(true) } else { None }
    });
    register_tuple_cmp_op(registry, "gt", false, |lhs, rhs| {
        match lhs.partial_cmp(&rhs) {
            Some(Ordering::Greater) => Some(true),
            Some(Ordering::Less) => Some(false),
            _ => None,
        }
    });
    register_tuple_cmp_op(registry, "gte", true, |lhs, rhs| {
        match lhs.partial_cmp(&rhs) {
            Some(Ordering::Greater) => Some(true),
            Some(Ordering::Less) => Some(false),
            _ => None,
        }
    });
    register_tuple_cmp_op(registry, "lt", false, |lhs, rhs| {
        match lhs.partial_cmp(&rhs) {
            Some(Ordering::Less) => Some(true),
            Some(Ordering::Greater) => Some(false),
            _ => None,
        }
    });
    register_tuple_cmp_op(registry, "lte", true, |lhs, rhs| {
        match lhs.partial_cmp(&rhs) {
            Some(Ordering::Less) => Some(true),
            Some(Ordering::Greater) => Some(false),
            _ => None,
        }
    });
}

fn register_like(registry: &mut FunctionRegistry) {
    registry.register_aliases("regexp", &["rlike"]);

    registry.register_passthrough_nullable_2_arg::<VariantType, StringType, BooleanType, _, _>(
        "like",
        |_, _, _| FunctionDomain::Full,
        |arg1, arg2, ctx| {
            variant_vectorize_like_jsonb()(arg1, arg2, Value::Scalar("".to_string()), ctx)
        },
    );
    registry.register_passthrough_nullable_3_arg::<VariantType, StringType, StringType, BooleanType, _, _>(
        "like",
        |_, _, _, _| FunctionDomain::Full,
       |arg1, arg2, arg3, ctx| {
           variant_vectorize_like_jsonb()(arg1, arg2, arg3, ctx)
       },
    );

    registry.register_function_factory(
        "like_any",
        FunctionFactory::Closure(Box::new(|_, args_type: &[DataType]| {
            if args_type.len() < 2 || args_type.len() > 3 {
                return None;
            }
            let is_nullable = args_type[0].is_nullable();
            let arg_type = args_type[0].remove_nullable();
            if !arg_type.is_string() && !arg_type.is_variant() {
                return None;
            }
            let mut new_args_type = match &args_type[1] {
                DataType::Tuple(patterns_ty) => {
                    if patterns_ty
                        .iter()
                        .any(|ty| !ty.is_string() && !ty.is_variant())
                    {
                        return None;
                    }
                    vec![
                        arg_type,
                        DataType::Tuple(vec![DataType::String; patterns_ty.len()]),
                    ]
                }
                DataType::String => vec![arg_type, DataType::String],
                _ => return None,
            };
            if args_type.len() > 2 {
                new_args_type.push(DataType::String);
            }

            let signature = FunctionSignature {
                name: "like_any".to_string(),
                args_type: new_args_type,
                return_type: DataType::Boolean,
            };
            Some(Arc::new(Function::with_passthrough_nullable(
                signature,
                FunctionDomain::Full,
                like_any_fn,
                None,
                is_nullable,
            )))
        })),
    );

    registry.register_passthrough_nullable_2_arg::<StringType, StringType, BooleanType, _, _>(
        "like",
        |_, lhs, rhs| {
            if rhs.max.as_ref() == Some(&rhs.min)
                && let Some(value) = calc_like_domain(lhs, rhs.min.to_string())
            {
                return value;
            }
            FunctionDomain::Full
        },
        |arg1, arg2, ctx| {
            vectorize_like(|str, pattern_type| pattern_type.compare(str))(
                arg1,
                arg2,
                Value::Scalar("".to_string()),
                ctx,
            )
        },
    );

    registry.register_passthrough_nullable_3_arg::<StringType, StringType, StringType, BooleanType, _, _>(
        "like",
        |_, lhs, rhs, escape| {
            if rhs.max.as_ref() == Some(&rhs.min) && escape.max.as_ref() == Some(&escape.min) {
                let pattern = escape
                    .min
                    .chars()
                    .next()
                    .map(|escape| type_check::convert_escape_pattern(&rhs.min, escape))
                    .unwrap_or(rhs.min.to_string());
                if let Some(value) = calc_like_domain(lhs, pattern) {
                    return value;
                }
            }
            FunctionDomain::Full
        },
        |arg1, arg2, arg3, ctx| {
            vectorize_like(|str, pattern_type| pattern_type.compare(str))(
                arg1,
                arg2,
                arg3,
                ctx,
            )
        },
    );

    registry.register_passthrough_nullable_2_arg::<StringType, StringType, BooleanType, _, _>(
        "regexp",
        |_, _, _| FunctionDomain::Full,
        vectorize_regexp(|str, pat, builder, ctx, map, _| {
            if let Some(re) = map.get(pat) {
                builder.push(re.is_match(str));
            } else {
                // TODO error
                match regexp::build_regexp_from_pattern("regexp", pat, None) {
                    Ok(re) => {
                        builder.push(re.is_match(str));
                        map.insert(pat.to_string(), re);
                    }
                    Err(e) => {
                        ctx.set_error(builder.len(), e);
                        builder.push(false);
                    }
                }
            }
        }),
    );

    registry.register_passthrough_nullable_2_arg::<StringType, StringType, BooleanType, _, _>(
        "glob",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<StringType, StringType, BooleanType>(
            |a, b, builder, _ctx| {
                // Create a glob pattern from the second argument
                let pattern = match glob::Pattern::new(b) {
                    Ok(pattern) => pattern,
                    Err(_) => {
                        builder.push(false);
                        return;
                    }
                };
                builder.push(pattern.matches(a));
            },
        ),
    );
}

fn calc_like_domain(lhs: &StringDomain, pattern: String) -> Option<FunctionDomain<BooleanType>> {
    match generate_like_pattern(pattern.as_bytes(), 1) {
        LikePattern::StartOfPercent(v) if v.is_empty() => {
            Some(FunctionDomain::Domain(ALL_TRUE_DOMAIN))
        }
        LikePattern::OrdinalStr(_) => Some(lhs.domain_eq(&StringDomain {
            min: pattern.clone(),
            max: Some(pattern),
        })),
        LikePattern::EndOfPercent(_) => {
            let mut pat_str = pattern;
            // remove the last char '%'
            pat_str.pop();
            let pat_len = pat_str.chars().count();
            let other = StringDomain {
                min: pat_str.clone(),
                max: Some(pat_str),
            };
            let lhs = StringDomain {
                min: lhs.min.chars().take(pat_len).collect(),
                max: lhs
                    .max
                    .as_ref()
                    .map(|max| max.chars().take(pat_len).collect()),
            };
            Some(lhs.domain_eq(&other))
        }
        _ => None,
    }
}

fn variant_vectorize_like_jsonb() -> impl Fn(
    Value<VariantType>,
    Value<StringType>,
    Value<StringType>,
    &mut EvalContext,
) -> Value<BooleanType>
+ Copy
+ Sized {
    variant_vectorize_like(|val, pattern_type| match pattern_type {
        LikePattern::OrdinalStr(_)
        | LikePattern::StartOfPercent(_)
        | LikePattern::EndOfPercent(_)
        | LikePattern::Constant(_) => {
            let raw_jsonb = RawJsonb::new(val);
            match raw_jsonb.as_str() {
                Ok(Some(s)) => pattern_type.compare(s.as_bytes()),
                Ok(None) => false,
                Err(_) => {
                    let s = raw_jsonb.to_string();
                    pattern_type.compare(s.as_bytes())
                }
            }
        }
        _ => {
            let raw_jsonb = RawJsonb::new(val);
            match raw_jsonb.traverse_check_string(|v| pattern_type.compare(v)) {
                Ok(res) => res,
                Err(_) => {
                    let s = raw_jsonb.to_string();
                    pattern_type.compare(s.as_bytes())
                }
            }
        }
    })
}

fn vectorize_like(
    func: impl Fn(&[u8], &LikePattern) -> bool + Copy,
) -> impl Fn(
    Value<StringType>,
    Value<StringType>,
    Value<StringType>,
    &mut EvalContext,
) -> Value<BooleanType>
+ Copy {
    move |arg1, arg2, arg3, _ctx| {
        let Value::Scalar(escape) = arg3 else {
            unreachable!()
        };
        match (arg1, arg2) {
            (Value::Scalar(arg1), Value::Scalar(arg2)) => {
                let pattern = convert_escape_pattern(&escape, arg2);
                let pattern_type = generate_like_pattern(pattern.as_bytes(), 1);
                Value::Scalar(func(arg1.as_bytes(), &pattern_type))
            }
            (Value::Column(arg1), Value::Scalar(arg2)) => {
                let arg1_iter = StringType::iter_column(&arg1);
                let mut builder = MutableBitmap::with_capacity(arg1.len());
                let pattern = convert_escape_pattern(&escape, arg2);
                let pattern_type =
                    generate_like_pattern(pattern.as_bytes(), arg1.total_bytes_len());
                if let LikePattern::SurroundByPercent(searcher) = pattern_type {
                    for arg1 in arg1_iter {
                        builder.push(searcher.search(arg1.as_bytes()).is_some());
                    }
                } else {
                    for arg1 in arg1_iter {
                        builder.push(func(arg1.as_bytes(), &pattern_type));
                    }
                }

                Value::Column(builder.into())
            }
            (Value::Scalar(arg1), Value::Column(arg2)) => {
                let arg2_iter = StringType::iter_column(&arg2);
                let mut builder = MutableBitmap::with_capacity(arg2.len());
                for arg2 in arg2_iter {
                    let pattern = convert_escape_pattern(&escape, arg2.to_string());
                    let pattern_type = generate_like_pattern(pattern.as_bytes(), 1);
                    builder.push(func(arg1.as_bytes(), &pattern_type));
                }
                Value::Column(builder.into())
            }
            (Value::Column(arg1), Value::Column(arg2)) => {
                let arg1_iter = StringType::iter_column(&arg1);
                let arg2_iter = StringType::iter_column(&arg2);
                let mut builder = MutableBitmap::with_capacity(arg2.len());
                for (arg1, arg2) in arg1_iter.zip(arg2_iter) {
                    let pattern = convert_escape_pattern(&escape, arg2.to_string());
                    let pattern_type = generate_like_pattern(pattern.as_bytes(), 1);
                    builder.push(func(arg1.as_bytes(), &pattern_type));
                }
                Value::Column(builder.into())
            }
        }
    }
}

fn like_any_fn(args: &[Value<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
    let arg = &args[0];
    let input_all_scalars = arg.as_scalar().is_some();
    let process_rows = if input_all_scalars { 1 } else { ctx.num_rows };

    let patterns = match args[1].as_scalar() {
        Some(Scalar::Tuple(patterns)) => patterns.clone(),
        Some(Scalar::String(pattern)) => vec![Scalar::String(pattern.clone())],
        _ => {
            ctx.set_error(
                1,
                "The second parameter of `like_any` must be of Tuple or String type",
            );
            return Value::Scalar(Scalar::Boolean(Default::default()));
        }
    };

    let escape: Value<StringType> = args
        .get(2)
        .cloned()
        .and_then(|value| value.try_downcast().ok())
        .unwrap_or(Value::Scalar("".to_string()));

    let result = if let Ok(value) = arg.try_downcast::<StringType>() {
        let like = vectorize_like(|str, pattern_type| pattern_type.compare(str));
        patterns
            .iter()
            .map(|pattern| {
                like(
                    value.clone(),
                    Value::<StringType>::Scalar(pattern.as_string().unwrap().clone()),
                    escape.clone(),
                    ctx,
                )
            })
            .collect::<Vec<_>>()
    } else if let Ok(value) = arg.try_downcast::<VariantType>() {
        let like = variant_vectorize_like_jsonb();
        patterns
            .iter()
            .map(|pattern| {
                like(
                    value.clone(),
                    Value::<StringType>::Scalar(pattern.as_string().unwrap().clone()),
                    escape.clone(),
                    ctx,
                )
            })
            .collect::<Vec<_>>()
    } else {
        ctx.set_error(
            1,
            "The first parameter of 'like_any' can only be of type String or Variant",
        );
        return Value::Scalar(Scalar::Boolean(Default::default()));
    };

    let mut builder = BooleanType::create_builder(process_rows, ctx.generics);
    let patterns_len = patterns.len();
    for row in 0..process_rows {
        builder.push((0..patterns_len).any(|i| result[i].index(row).unwrap()));
    }

    if input_all_scalars {
        Value::<BooleanType>::Scalar(BooleanType::build_scalar(builder))
    } else {
        Value::<BooleanType>::Column(BooleanType::build_column(builder))
    }
    .upcast()
}

fn variant_vectorize_like(
    func: impl Fn(&[u8], &LikePattern) -> bool + Copy,
) -> impl Fn(
    Value<VariantType>,
    Value<StringType>,
    Value<StringType>,
    &mut EvalContext,
) -> Value<BooleanType>
+ Copy {
    move |arg1, arg2, arg3, _ctx| {
        let Value::Scalar(escape) = arg3 else {
            unreachable!()
        };
        match (arg1, arg2) {
            (Value::Scalar(arg1), Value::Scalar(arg2)) => {
                let pattern = convert_escape_pattern(&escape, arg2);
                let pattern_type = generate_like_pattern(pattern.as_bytes(), 1);
                Value::Scalar(func(&arg1, &pattern_type))
            }
            (Value::Column(arg1), Value::Scalar(arg2)) => {
                let arg1_iter = VariantType::iter_column(&arg1);

                let pattern = convert_escape_pattern(&escape, arg2);
                let pattern_type =
                    generate_like_pattern(pattern.as_bytes(), arg1.total_bytes_len());
                let mut builder = MutableBitmap::with_capacity(arg1.len());
                for arg1 in arg1_iter {
                    builder.push(func(arg1, &pattern_type));
                }
                Value::Column(builder.into())
            }
            (Value::Scalar(arg1), Value::Column(arg2)) => {
                let arg2_iter = StringType::iter_column(&arg2);
                let mut builder = MutableBitmap::with_capacity(arg2.len());
                for arg2 in arg2_iter {
                    let pattern = convert_escape_pattern(&escape, arg2.to_string());
                    let pattern_type = generate_like_pattern(pattern.as_bytes(), 1);
                    builder.push(func(&arg1, &pattern_type));
                }
                Value::Column(builder.into())
            }
            (Value::Column(arg1), Value::Column(arg2)) => {
                let arg1_iter = VariantType::iter_column(&arg1);
                let arg2_iter = StringType::iter_column(&arg2);
                let mut builder = MutableBitmap::with_capacity(arg2.len());
                for (arg1, arg2) in arg1_iter.zip(arg2_iter) {
                    let pattern = convert_escape_pattern(&escape, arg2.to_string());
                    let pattern_type = generate_like_pattern(pattern.as_bytes(), 1);
                    builder.push(func(arg1, &pattern_type));
                }
                Value::Column(builder.into())
            }
        }
    }
}

fn convert_escape_pattern(escape: &str, arg2: String) -> String {
    escape
        .chars()
        .next()
        .map(|escape| type_check::convert_escape_pattern(&arg2, escape))
        .unwrap_or(arg2)
}

fn vectorize_regexp(
    func: impl Fn(
        &str,
        &str,
        &mut MutableBitmap,
        &mut EvalContext,
        &mut HashMap<String, Regex>,
        &mut HashMap<Vec<u8>, String>,
    ) + Copy,
) -> impl Fn(Value<StringType>, Value<StringType>, &mut EvalContext) -> Value<BooleanType> + Copy {
    move |arg1, arg2, ctx| {
        let mut map = HashMap::new();
        let mut string_map = HashMap::new();
        match (arg1, arg2) {
            (Value::Scalar(arg1), Value::Scalar(arg2)) => {
                let mut builder = MutableBitmap::with_capacity(1);
                func(&arg1, &arg2, &mut builder, ctx, &mut map, &mut string_map);
                Value::Scalar(BooleanType::build_scalar(builder))
            }
            (Value::Column(arg1), Value::Scalar(arg2)) => {
                let arg1_iter = StringType::iter_column(&arg1);
                let mut builder = MutableBitmap::with_capacity(arg1.len());
                for arg1 in arg1_iter {
                    func(arg1, &arg2, &mut builder, ctx, &mut map, &mut string_map);
                }
                Value::Column(builder.into())
            }
            (Value::Scalar(arg1), Value::Column(arg2)) => {
                let arg2_iter = StringType::iter_column(&arg2);
                let mut builder = MutableBitmap::with_capacity(arg2.len());
                for arg2 in arg2_iter {
                    func(&arg1, arg2, &mut builder, ctx, &mut map, &mut string_map);
                }
                Value::Column(builder.into())
            }
            (Value::Column(arg1), Value::Column(arg2)) => {
                let arg1_iter = StringType::iter_column(&arg1);
                let arg2_iter = StringType::iter_column(&arg2);
                let mut builder = MutableBitmap::with_capacity(arg2.len());
                for (arg1, arg2) in arg1_iter.zip(arg2_iter) {
                    func(arg1, arg2, &mut builder, ctx, &mut map, &mut string_map);
                }
                Value::Column(builder.into())
            }
        }
    }
}

fn register_bitmap_cmp(registry: &mut FunctionRegistry) {
    registry.register_passthrough_nullable_2_arg::<BitmapType, BitmapType, BooleanType, _, _>(
        "eq",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<BitmapType, BitmapType, BooleanType>(
            |lhs, rhs, builder, ctx| {
                let row = builder.len();
                builder.push(compare_bitmap_bytes(lhs, rhs, ctx, row));
            },
        ),
    );
    registry.register_passthrough_nullable_2_arg::<BitmapType, BitmapType, BooleanType, _, _>(
        "noteq",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<BitmapType, BitmapType, BooleanType>(
            |lhs, rhs, builder, ctx| {
                let row = builder.len();
                let equals = compare_bitmap_bytes(lhs, rhs, ctx, row);
                builder.push(!equals);
            },
        ),
    );
}

fn compare_bitmap_bytes(lhs: &[u8], rhs: &[u8], ctx: &mut EvalContext, row: usize) -> bool {
    if lhs == rhs {
        return true;
    }

    let left = match deserialize_bitmap(lhs) {
        Ok(v) => v,
        Err(e) => {
            ctx.set_error(row, e.to_string());
            return false;
        }
    };
    let right = match deserialize_bitmap(rhs) {
        Ok(v) => v,
        Err(e) => {
            ctx.set_error(row, e.to_string());
            return false;
        }
    };

    left.iter().eq(right.iter())
}
