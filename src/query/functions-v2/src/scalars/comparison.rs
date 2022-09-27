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

use common_arrow::arrow::bitmap::MutableBitmap;
use common_exception::Result;
use common_expression::types::BooleanType;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::types::NumberDataType::*;
use common_expression::types::NumberType;
use common_expression::types::StringType;
use common_expression::types::TimestampType;
use common_expression::types::ValueType;
use common_expression::types::VariantType;
use common_expression::values::Value;
use common_expression::with_number_mapped_type;
use common_expression::FunctionContext;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;
use common_expression::ValueRef;
use regex::bytes::Regex;

use crate::scalars::string_multi_args::regexp;

pub const ALL_CMP_TYPES: &[DataType; 14] = &[
    DataType::String,
    DataType::Boolean,
    DataType::Timestamp,
    DataType::Variant,
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
                String => StringType, Timestamp => TimestampType, Variant => VariantType
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

    registry.register_passthrough_nullable_2_arg::<StringType, StringType, BooleanType, _, _>(
        "like",
        FunctionProperty::default(),
        |_, _| None,
        vectorize_regexp(|str, pat, map, _| {
            let pattern = if let Some(pattern) = map.get(pat) {
                pattern
            } else {
                let pattern_str = simdutf8::basic::from_utf8(pat).map_err(|err| {
                    format!("unable to convert the LIKE pattern to string: {err}")
                })?;
                let re_pattern = like_pattern_to_regex(pattern_str);
                let re = Regex::new(&re_pattern)
                    .map_err(|err| format!("unable to build the LIKE pattern: {err}"))?;
                map.insert(pat.to_vec(), re);
                map.get(pat).unwrap()
            };
            Ok(pattern.is_match(str))
        }),
    );

    registry.register_passthrough_nullable_2_arg::<StringType, StringType, BooleanType, _, _>(
        "regexp",
        FunctionProperty::default(),
        |_, _| None,
        vectorize_regexp(|str, pat, map, _| {
            let pattern = if let Some(pattern) = map.get(pat) {
                pattern
            } else {
                let re = regexp::build_regexp_from_pattern("regexp", pat, None)?;
                map.insert(pat.to_vec(), re);
                map.get(pat).unwrap()
            };
            Ok(pattern.is_match(str))
        }),
    );
    registry.register_aliases("regexp", &["rlike"]);
}

fn vectorize_regexp(
    func: impl Fn(&[u8], &[u8], &mut HashMap<Vec<u8>, Regex>, FunctionContext) -> Result<bool, String>
    + Copy,
) -> impl Fn(
    ValueRef<StringType>,
    ValueRef<StringType>,
    FunctionContext,
) -> Result<Value<BooleanType>, String>
+ Copy {
    move |arg1, arg2, ctx| {
        let mut map = HashMap::new();
        match (arg1, arg2) {
            (ValueRef::Scalar(arg1), ValueRef::Scalar(arg2)) => {
                Ok(Value::Scalar(func(arg1, arg2, &mut map, ctx)?))
            }
            (ValueRef::Column(arg1), ValueRef::Scalar(arg2)) => {
                let arg1_iter = StringType::iter_column(&arg1);
                let mut builder = MutableBitmap::with_capacity(arg1.len());
                for arg1 in arg1_iter {
                    builder.push(func(arg1, arg2, &mut map, ctx)?);
                }
                Ok(Value::Column(builder.into()))
            }
            (ValueRef::Scalar(arg1), ValueRef::Column(arg2)) => {
                let arg2_iter = StringType::iter_column(&arg2);
                let mut builder = MutableBitmap::with_capacity(arg2.len());
                for arg2 in arg2_iter {
                    builder.push(func(arg1, arg2, &mut map, ctx)?);
                }
                Ok(Value::Column(builder.into()))
            }
            (ValueRef::Column(arg1), ValueRef::Column(arg2)) => {
                let arg1_iter = StringType::iter_column(&arg1);
                let arg2_iter = StringType::iter_column(&arg2);
                let mut builder = MutableBitmap::with_capacity(arg2.len());
                for (arg1, arg2) in arg1_iter.zip(arg2_iter) {
                    builder.push(func(arg1, arg2, &mut map, ctx)?);
                }
                Ok(Value::Column(builder.into()))
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
