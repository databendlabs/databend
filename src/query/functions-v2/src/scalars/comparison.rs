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

use std::collections::HashMap;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::ArgType;
use common_expression::types::BooleanType;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::types::NumberDataType::*;
use common_expression::types::NumberType;
use common_expression::types::StringType;
use common_expression::types::TimestampType;
use common_expression::values::Value;
use common_expression::with_number_mapped_type;
use common_expression::FunctionContext;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;
use common_expression::ValueRef;
use regex::bytes::Regex;
use regex::bytes::RegexBuilder;

pub const ALL_CMP_TYPES: &[DataType; 13] = &[
    DataType::String,
    DataType::Boolean,
    DataType::Timestamp,
    DataType::Number(UInt8),
    DataType::Number(UInt16),
    DataType::Number(UInt32),
    DataType::Number(UInt64),
    DataType::Number(Int8),
    DataType::Number(Int16),
    DataType::Number(Int32),
    DataType::Number(Int64),
    DataType::Number(Float32),
    DataType::Number(Float64),
];

#[macro_export]
macro_rules! with_cmp_mapped_type {
    (| $t:tt | $($tail:tt)*) => {
        match_template::match_template! {
            $t = [
                String => StringType, Timestamp => TimestampType
            ],
            $($tail)*
        }
    }
}

pub fn register(registry: &mut FunctionRegistry) {
    for ty in ALL_CMP_TYPES {
        with_cmp_mapped_type!(|DATA_TYPE| match ty {
            DataType::DATA_TYPE => {
                registry.register_2_arg::<DATA_TYPE, DATA_TYPE, BooleanType, _, _>(
                    "eq",
                    FunctionProperty::default(),
                    |_, _| None,
                    |lhs, rhs, _| lhs == rhs,
                );
                registry.register_2_arg::<DATA_TYPE, DATA_TYPE, BooleanType, _, _>(
                    "noteq",
                    FunctionProperty::default(),
                    |_, _| None,
                    |lhs, rhs, _| lhs != rhs,
                );
                registry.register_2_arg::<DATA_TYPE, DATA_TYPE, BooleanType, _, _>(
                    "gt",
                    FunctionProperty::default(),
                    |_, _| None,
                    |lhs, rhs, _| lhs > rhs,
                );
                registry.register_2_arg::<DATA_TYPE, DATA_TYPE, BooleanType, _, _>(
                    "gte",
                    FunctionProperty::default(),
                    |_, _| None,
                    |lhs, rhs, _| lhs >= rhs,
                );
                registry.register_2_arg::<DATA_TYPE, DATA_TYPE, BooleanType, _, _>(
                    "lt",
                    FunctionProperty::default(),
                    |_, _| None,
                    |lhs, rhs, _| lhs < rhs,
                );
                registry.register_2_arg::<DATA_TYPE, DATA_TYPE, BooleanType, _, _>(
                    "lte",
                    FunctionProperty::default(),
                    |_, _| None,
                    |lhs, rhs, _| lhs <= rhs,
                );
            }
            DataType::Boolean => {
                registry.register_2_arg::<BooleanType, BooleanType, BooleanType, _, _>(
                    "eq",
                    FunctionProperty::default(),
                    |_, _| None,
                    |lhs, rhs, _| lhs == rhs,
                );
                registry.register_2_arg::<BooleanType, BooleanType, BooleanType, _, _>(
                    "noteq",
                    FunctionProperty::default(),
                    |_, _| None,
                    |lhs, rhs, _| lhs != rhs,
                );
                registry.register_2_arg::<BooleanType, BooleanType, BooleanType, _, _>(
                    "gt",
                    FunctionProperty::default(),
                    |_, _| None,
                    |lhs, rhs, _| lhs & !rhs,
                );
                registry.register_2_arg::<BooleanType, BooleanType, BooleanType, _, _>(
                    "gte",
                    FunctionProperty::default(),
                    |_, _| None,
                    |lhs, rhs, _| (lhs & !rhs) || (lhs & rhs),
                );
                registry.register_2_arg::<BooleanType, BooleanType, BooleanType, _, _>(
                    "lt",
                    FunctionProperty::default(),
                    |_, _| None,
                    |lhs, rhs, _| !lhs & rhs,
                );
                registry.register_2_arg::<BooleanType, BooleanType, BooleanType, _, _>(
                    "lte",
                    FunctionProperty::default(),
                    |_, _| None,
                    |lhs, rhs, _| (!lhs & rhs) || (lhs & rhs),
                );
            }
            DataType::Number(u) => {
                with_number_mapped_type!(|NUM_TYPE| match u {
                    NumberDataType::NUM_TYPE => {
                        registry
                            .register_2_arg::<NumberType<NUM_TYPE>, NumberType<NUM_TYPE>, BooleanType, _, _>(
                                "eq",
                                FunctionProperty::default(),
                                |_, _| None,
                                |lhs, rhs, _| lhs == rhs,
                            );
                        registry
                            .register_2_arg::<NumberType<NUM_TYPE>, NumberType<NUM_TYPE>, BooleanType, _, _>(
                                "noteq",
                                FunctionProperty::default(),
                                |_, _| None,
                                |lhs, rhs, _| lhs != rhs,
                            );
                        registry
                            .register_2_arg::<NumberType<NUM_TYPE>, NumberType<NUM_TYPE>, BooleanType, _, _>(
                                "gt",
                                FunctionProperty::default(),
                                |_, _| None,
                                |lhs, rhs, _| lhs > rhs,
                            );
                        registry
                            .register_2_arg::<NumberType<NUM_TYPE>, NumberType<NUM_TYPE>, BooleanType, _, _>(
                                "gte",
                                FunctionProperty::default(),
                                |_, _| None,
                                |lhs, rhs, _| lhs >= rhs,
                            );
                        registry
                            .register_2_arg::<NumberType<NUM_TYPE>, NumberType<NUM_TYPE>, BooleanType, _, _>(
                                "lt",
                                FunctionProperty::default(),
                                |_, _| None,
                                |lhs, rhs, _| lhs < rhs,
                            );
                        registry
                            .register_2_arg::<NumberType<NUM_TYPE>, NumberType<NUM_TYPE>, BooleanType, _, _>(
                                "lte",
                                FunctionProperty::default(),
                                |_, _| None,
                                |lhs, rhs, _| lhs <= rhs,
                            );
                    }
                });
            }
            _ => todo!(),
        });
    }

    // TODO: wait @b41sh complete jsonb cmp, and need support
    // `VariantType like VariantType`
    // `VariantType regexp VariantType`
    // registry.register_2_arg::<VariantType, VariantType, BooleanType, _, _>(
    //     "eq",
    //     FunctionProperty::default(),
    //     |_lhs, _rhs| None,
    //     |lhs, rhs, _| lhs.cmp(rhs) == Ordering::Equal,
    // );

    // registry.register_2_arg::<VariantType, VariantType, BooleanType, _, _>(
    //     "gt",
    //     FunctionProperty::default(),
    //     |_lhs, _rhs| None,
    //     |lhs, rhs, _| lhs.cmp(rhs) == Ordering::Greater,
    // );

    // registry.register_2_arg::<VariantType, VariantType, BooleanType, _, _>(
    //     "lt",
    //     FunctionProperty::default(),
    //     |_lhs, _rhs| None,
    //     |lhs, rhs, _| lhs.cmp(rhs) == Ordering::Less,
    // );

    // registry.register_2_arg::<VariantType, VariantType, BooleanType, _, _>(
    //     "gte",
    //     FunctionProperty::default(),
    //     |_lhs, _rhs| None,
    //     |lhs, rhs, _| {
    //         let res = lhs.cmp(rhs);
    //         res == Ordering::Equal || res == Ordering::Greater
    //     },
    // );

    // registry.register_2_arg::<VariantType, VariantType, BooleanType, _, _>(
    //     "lte",
    //     FunctionProperty::default(),
    //     |_lhs, _rhs| None,
    //     |lhs, rhs, _| {
    //         let res = lhs.cmp(rhs);
    //         res == Ordering::Equal || res == Ordering::Less
    //     },
    // );

    // registry.register_2_arg::<VariantType, VariantType, BooleanType, _, _>(
    //     "noteq",
    //     FunctionProperty::default(),
    //     |_lhs, _rhs| None,
    //     |lhs, rhs, _| lhs.cmp(rhs) != Ordering::Equal,
    // );

    registry.register_passthrough_nullable_2_arg::<StringType, StringType, BooleanType, _, _>(
        "like",
        FunctionProperty::default(),
        |_lhs, _rhs| None,
        vectorize_2_like_arg::<StringType, StringType, BooleanType>(|lhs, rhs, _, mut map| {
            let pattern = if let Some(pattern) = map.get(rhs) {
                pattern
            } else {
                let pattern_str = simdutf8::basic::from_utf8(rhs)
                    .expect("Unable to convert the LIKE pattern to string: {}");
                let re_pattern = like_pattern_to_regex(pattern_str);
                let re =
                    Regex::new(&re_pattern).expect("Unable to build regex from LIKE pattern: {}");
                map.insert(rhs, re);
                map.get(rhs).unwrap()
            };
            pattern.is_match(lhs)
        }),
    );

    registry.register_passthrough_nullable_2_arg::<StringType, StringType, BooleanType, _, _>(
        "regexp",
        FunctionProperty::default(),
        |_lhs, _rhs| None,
        vectorize_2_like_arg::<StringType, StringType, BooleanType>(|lhs, rhs, _, mut map| {
            let pattern = if let Some(pattern) = map.get(rhs) {
                pattern
            } else {
                let re = build_regexp_from_pattern(rhs).unwrap();
                map.insert(rhs, re);
                map.get(rhs).unwrap()
            };
            pattern.is_match(lhs)
        }),
    );
    registry.register_aliases("regexp", &["rlike"]);
}

fn vectorize_2_like_arg<I1: ArgType, I2: ArgType, O: ArgType>(
    func: impl Fn(
        I1::ScalarRef<'_>,
        I2::ScalarRef<'_>,
        FunctionContext,
        HashMap<&[u8], Regex>,
    ) -> O::Scalar
    + Copy,
) -> impl Fn(ValueRef<I1>, ValueRef<I2>, FunctionContext) -> Result<Value<O>, String> + Copy {
    move |arg1, arg2, ctx| {
        let map = HashMap::new();
        match (arg1, arg2) {
            (ValueRef::Scalar(arg1), ValueRef::Scalar(arg2)) => {
                Ok(Value::Scalar(func(arg1, arg2, ctx, map)))
            }
            (ValueRef::Column(arg1), ValueRef::Scalar(arg2)) => {
                let arg1_iter = I1::iter_column(&arg1);
                let iter = arg1_iter.map(|arg1| func(arg1, arg2.clone(), ctx, map));
                let col = O::column_from_iter(iter, ctx.generics);
                Ok(Value::Column(col))
            }
            (ValueRef::Scalar(arg1), ValueRef::Column(arg2)) => {
                let arg2_iter = I2::iter_column(&arg2);
                let iter = arg2_iter.map(|arg2| func(arg1.clone(), arg2, ctx, map));
                let col = O::column_from_iter(iter, ctx.generics);
                Ok(Value::Column(col))
            }
            (ValueRef::Column(arg1), ValueRef::Column(arg2)) => {
                let arg1_iter = I1::iter_column(&arg1);
                let arg2_iter = I2::iter_column(&arg2);
                let iter = arg1_iter
                    .zip(arg2_iter)
                    .map(|(arg1, arg2)| func(arg1, arg2, ctx, map));
                let col = O::column_from_iter(iter, ctx.generics);
                Ok(Value::Column(col))
            }
        }
    }
}

/// Transform the like pattern to regex pattern.
/// e.g. 'Hello\._World%\%' tranform to '^Hello\\\..World.*%$'.
#[inline]
fn like_pattern_to_regex(pattern: &str) -> String {
    let mut regex = String::with_capacity(pattern.len() * 2);
    regex.push('^');

    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            // Use double backslash to escape special character.
            '^' | '$' | '(' | ')' | '*' | '+' | '.' | '[' | '?' | '{' | '|' => {
                regex.push('\\');
                regex.push(c);
            }
            '%' => regex.push_str(".*"),
            '_' => regex.push('.'),
            '\\' => match chars.peek().cloned() {
                Some('%') => {
                    regex.push('%');
                    chars.next();
                }
                Some('_') => {
                    regex.push('_');
                    chars.next();
                }
                Some('\\') => {
                    regex.push_str("\\\\");
                    chars.next();
                }
                _ => regex.push_str("\\\\"),
            },
            _ => regex.push(c),
        }
    }

    regex.push('$');
    regex
}

#[inline]
fn build_regexp_from_pattern(pat: &[u8]) -> Result<Regex> {
    let pattern = match pat.is_empty() {
        true => "^$",
        false => simdutf8::basic::from_utf8(pat).map_err(|e| {
            ErrorCode::BadArguments(format!(
                "Unable to convert the REGEXP pattern to string: {}",
                e
            ))
        })?,
    };

    RegexBuilder::new(pattern)
        .case_insensitive(true)
        .build()
        .map_err(|e| {
            ErrorCode::BadArguments(format!("Unable to build regex from REGEXP pattern: {}", e))
        })
}
