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

use std::cmp::Ordering;
use std::collections::HashMap;

use common_arrow::arrow::bitmap::MutableBitmap;
use common_exception::Result;
use common_expression::types::boolean::BooleanDomain;
use common_expression::types::BooleanType;
use common_expression::types::NumberDataType;
use common_expression::types::NumberType;
use common_expression::types::StringType;
use common_expression::types::TimestampType;
use common_expression::types::ValueType;
use common_expression::types::VariantType;
use common_expression::types::ALL_NUMERICS_TYPES;
use common_expression::values::Value;
use common_expression::with_number_mapped_type;
use common_expression::EvalContext;
use common_expression::FunctionDomain;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;
use common_expression::ValueRef;
use regex::bytes::Regex;

use crate::scalars::string_multi_args::regexp;

pub fn register(registry: &mut FunctionRegistry) {
    register_string_cmp(registry);
    register_date_cmp(registry);
    register_boolean_cmp(registry);
    register_number_cmp(registry);
    register_variant_cmp(registry);
    register_like(registry);
}

const ALL_TRUE_DOMAIN: BooleanDomain = BooleanDomain {
    has_true: true,
    has_false: false,
};

const ALL_FALSE_DOMAIN: BooleanDomain = BooleanDomain {
    has_true: false,
    has_false: true,
};

fn register_string_cmp(registry: &mut FunctionRegistry) {
    registry.register_2_arg::<StringType, StringType, BooleanType, _, _>(
        "eq",
        FunctionProperty::default(),
        |_, _| FunctionDomain::Full,
        |lhs, rhs, _| lhs == rhs,
    );
    registry.register_2_arg::<StringType, StringType, BooleanType, _, _>(
        "noteq",
        FunctionProperty::default(),
        |_, _| FunctionDomain::Full,
        |lhs, rhs, _| lhs != rhs,
    );
    registry.register_2_arg::<StringType, StringType, BooleanType, _, _>(
        "gt",
        FunctionProperty::default(),
        |_, _| FunctionDomain::Full,
        |lhs, rhs, _| lhs > rhs,
    );
    registry.register_2_arg::<StringType, StringType, BooleanType, _, _>(
        "gte",
        FunctionProperty::default(),
        |_, _| FunctionDomain::Full,
        |lhs, rhs, _| lhs >= rhs,
    );
    registry.register_2_arg::<StringType, StringType, BooleanType, _, _>(
        "lt",
        FunctionProperty::default(),
        |_, _| FunctionDomain::Full,
        |lhs, rhs, _| lhs < rhs,
    );
    registry.register_2_arg::<StringType, StringType, BooleanType, _, _>(
        "lte",
        FunctionProperty::default(),
        |_, _| FunctionDomain::Full,
        |lhs, rhs, _| lhs <= rhs,
    );
}

macro_rules! register_simple_domain_type_cmp {
    ($registry:ident, $T:ty) => {
        $registry.register_2_arg::<$T, $T, BooleanType, _, _>(
            "eq",
            FunctionProperty::default(),
            |d1, d2| {
                if d1.min > d2.max || d1.max < d2.min {
                    FunctionDomain::Domain(ALL_FALSE_DOMAIN)
                } else {
                    FunctionDomain::Full
                }
            },
            |lhs, rhs, _| lhs == rhs,
        );
        $registry.register_2_arg::<$T, $T, BooleanType, _, _>(
            "noteq",
            FunctionProperty::default(),
            |d1, d2| {
                if d1.min > d2.max || d1.max < d2.min {
                    FunctionDomain::Domain(ALL_TRUE_DOMAIN)
                } else {
                    FunctionDomain::Full
                }
            },
            |lhs, rhs, _| lhs != rhs,
        );
        $registry.register_2_arg::<$T, $T, BooleanType, _, _>(
            "gt",
            FunctionProperty::default(),
            |d1, d2| {
                if d1.min > d2.max {
                    FunctionDomain::Domain(ALL_TRUE_DOMAIN)
                } else if d1.max <= d2.min {
                    FunctionDomain::Domain(ALL_FALSE_DOMAIN)
                } else {
                    FunctionDomain::Full
                }
            },
            |lhs, rhs, _| lhs > rhs,
        );
        $registry.register_2_arg::<$T, $T, BooleanType, _, _>(
            "gte",
            FunctionProperty::default(),
            |d1, d2| {
                if d1.min >= d2.max {
                    FunctionDomain::Domain(ALL_TRUE_DOMAIN)
                } else if d1.max < d2.min {
                    FunctionDomain::Domain(ALL_FALSE_DOMAIN)
                } else {
                    FunctionDomain::Full
                }
            },
            |lhs, rhs, _| lhs >= rhs,
        );
        $registry.register_2_arg::<$T, $T, BooleanType, _, _>(
            "lt",
            FunctionProperty::default(),
            |d1, d2| {
                if d1.max < d2.min {
                    FunctionDomain::Domain(ALL_TRUE_DOMAIN)
                } else if d1.min >= d2.max {
                    FunctionDomain::Domain(ALL_FALSE_DOMAIN)
                } else {
                    FunctionDomain::Full
                }
            },
            |lhs, rhs, _| lhs < rhs,
        );
        $registry.register_2_arg::<$T, $T, BooleanType, _, _>(
            "lte",
            FunctionProperty::default(),
            |d1, d2| {
                if d1.max <= d2.min {
                    FunctionDomain::Domain(ALL_TRUE_DOMAIN)
                } else if d1.min > d2.max {
                    FunctionDomain::Domain(ALL_FALSE_DOMAIN)
                } else {
                    FunctionDomain::Full
                }
            },
            |lhs, rhs, _| lhs <= rhs,
        );
    };
}

fn register_date_cmp(registry: &mut FunctionRegistry) {
    register_simple_domain_type_cmp!(registry, TimestampType);
}

fn register_boolean_cmp(registry: &mut FunctionRegistry) {
    registry.register_2_arg::<BooleanType, BooleanType, BooleanType, _, _>(
        "eq",
        FunctionProperty::default(),
        |d1, d2| match (d1.has_true, d1.has_false, d2.has_true, d2.has_false) {
            (true, false, true, false) => FunctionDomain::Domain(ALL_TRUE_DOMAIN),
            (false, true, false, true) => FunctionDomain::Domain(ALL_TRUE_DOMAIN),
            (true, false, false, true) => FunctionDomain::Domain(ALL_FALSE_DOMAIN),
            (false, true, true, false) => FunctionDomain::Domain(ALL_FALSE_DOMAIN),
            _ => FunctionDomain::Full,
        },
        |lhs, rhs, _| lhs == rhs,
    );
    registry.register_2_arg::<BooleanType, BooleanType, BooleanType, _, _>(
        "noteq",
        FunctionProperty::default(),
        |d1, d2| match (d1.has_true, d1.has_false, d2.has_true, d2.has_false) {
            (true, false, true, false) => FunctionDomain::Domain(ALL_FALSE_DOMAIN),
            (false, true, false, true) => FunctionDomain::Domain(ALL_FALSE_DOMAIN),
            (true, false, false, true) => FunctionDomain::Domain(ALL_TRUE_DOMAIN),
            (false, true, true, false) => FunctionDomain::Domain(ALL_TRUE_DOMAIN),
            _ => FunctionDomain::Full,
        },
        |lhs, rhs, _| lhs != rhs,
    );
    registry.register_2_arg::<BooleanType, BooleanType, BooleanType, _, _>(
        "gt",
        FunctionProperty::default(),
        |d1, d2| match (d1.has_true, d1.has_false, d2.has_true, d2.has_false) {
            (true, false, false, true) => FunctionDomain::Domain(ALL_TRUE_DOMAIN),
            (false, true, _, _) => FunctionDomain::Domain(ALL_FALSE_DOMAIN),
            _ => FunctionDomain::Full,
        },
        |lhs, rhs, _| lhs & !rhs,
    );
    registry.register_2_arg::<BooleanType, BooleanType, BooleanType, _, _>(
        "gte",
        FunctionProperty::default(),
        |d1, d2| match (d1.has_true, d1.has_false, d2.has_true, d2.has_false) {
            (true, false, _, _) => FunctionDomain::Domain(ALL_TRUE_DOMAIN),
            (_, _, false, true) => FunctionDomain::Domain(ALL_TRUE_DOMAIN),
            (false, true, true, false) => FunctionDomain::Domain(ALL_FALSE_DOMAIN),
            _ => FunctionDomain::Full,
        },
        |lhs, rhs, _| (lhs & !rhs) || (lhs & rhs),
    );
    registry.register_2_arg::<BooleanType, BooleanType, BooleanType, _, _>(
        "lt",
        FunctionProperty::default(),
        |d1, d2| match (d1.has_true, d1.has_false, d2.has_true, d2.has_false) {
            (false, true, true, false) => FunctionDomain::Domain(ALL_TRUE_DOMAIN),
            (_, _, false, true) => FunctionDomain::Domain(ALL_FALSE_DOMAIN),
            _ => FunctionDomain::Full,
        },
        |lhs, rhs, _| !lhs & rhs,
    );
    registry.register_2_arg::<BooleanType, BooleanType, BooleanType, _, _>(
        "lte",
        FunctionProperty::default(),
        |d1, d2| match (d1.has_true, d1.has_false, d2.has_true, d2.has_false) {
            (false, true, _, _) => FunctionDomain::Domain(ALL_TRUE_DOMAIN),
            (_, _, true, false) => FunctionDomain::Domain(ALL_TRUE_DOMAIN),
            (true, false, false, true) => FunctionDomain::Domain(ALL_FALSE_DOMAIN),
            _ => FunctionDomain::Full,
        },
        |lhs, rhs, _| (!lhs & rhs) || (lhs & rhs),
    );
}

fn register_number_cmp(registry: &mut FunctionRegistry) {
    for ty in ALL_NUMERICS_TYPES {
        with_number_mapped_type!(|NUM_TYPE| match ty {
            NumberDataType::NUM_TYPE => {
                register_simple_domain_type_cmp!(registry, NumberType<NUM_TYPE>);
            }
        });
    }
}

fn register_variant_cmp(registry: &mut FunctionRegistry) {
    registry.register_2_arg::<VariantType, VariantType, BooleanType, _, _>(
        "eq",
        FunctionProperty::default(),
        |_, _| FunctionDomain::Full,
        |lhs, rhs, _| {
            common_jsonb::compare(lhs, rhs).expect("unable to parse jsonb value") == Ordering::Equal
        },
    );
    registry.register_2_arg::<VariantType, VariantType, BooleanType, _, _>(
        "noteq",
        FunctionProperty::default(),
        |_, _| FunctionDomain::Full,
        |lhs, rhs, _| {
            common_jsonb::compare(lhs, rhs).expect("unable to parse jsonb value") != Ordering::Equal
        },
    );
    registry.register_2_arg::<VariantType, VariantType, BooleanType, _, _>(
        "gt",
        FunctionProperty::default(),
        |_, _| FunctionDomain::Full,
        |lhs, rhs, _| {
            common_jsonb::compare(lhs, rhs).expect("unable to parse jsonb value")
                == Ordering::Greater
        },
    );
    registry.register_2_arg::<VariantType, VariantType, BooleanType, _, _>(
        "gte",
        FunctionProperty::default(),
        |_, _| FunctionDomain::Full,
        |lhs, rhs, _| {
            common_jsonb::compare(lhs, rhs).expect("unable to parse jsonb value") != Ordering::Less
        },
    );
    registry.register_2_arg::<VariantType, VariantType, BooleanType, _, _>(
        "lt",
        FunctionProperty::default(),
        |_, _| FunctionDomain::Full,
        |lhs, rhs, _| {
            common_jsonb::compare(lhs, rhs).expect("unable to parse jsonb value") == Ordering::Less
        },
    );
    registry.register_2_arg::<VariantType, VariantType, BooleanType, _, _>(
        "lte",
        FunctionProperty::default(),
        |_, _| FunctionDomain::Full,
        |lhs, rhs, _| {
            common_jsonb::compare(lhs, rhs).expect("unable to parse jsonb value")
                != Ordering::Greater
        },
    );
}

fn register_like(registry: &mut FunctionRegistry) {
    registry.register_aliases("regexp", &["rlike"]);

    registry.register_passthrough_nullable_2_arg::<StringType, StringType, BooleanType, _, _>(
        "like",
        FunctionProperty::default(),
        |_, _| FunctionDomain::Full,
        vectorize_like(|str, pat, _, pattern_type| {
            match pattern_type {
                PatternType::OrdinalStr => Ok(str == pat),
                PatternType::EndOfPercent => {
                    // fast path, can use starts_with
                    let starts_with = &pat[..pat.len() - 1];
                    Ok(str.starts_with(starts_with))
                }
                PatternType::StartOfPercent => {
                    // fast path, can use ends_with
                    let ends_with = &pat[1..];
                    Ok(str.ends_with(ends_with))
                }
                PatternType::PatternStr => Ok(like(str, pat)),
            }
        }),
    );

    registry.register_passthrough_nullable_2_arg::<StringType, StringType, BooleanType, _, _>(
        "regexp",
        FunctionProperty::default(),
        |_, _| FunctionDomain::Full,
        vectorize_regexp(|str, pat, _, map, _| {
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
}

fn vectorize_like(
    func: impl Fn(&[u8], &[u8], EvalContext, PatternType) -> Result<bool, String> + Copy,
) -> impl Fn(
    ValueRef<StringType>,
    ValueRef<StringType>,
    EvalContext,
) -> Result<Value<BooleanType>, String>
+ Copy {
    move |arg1, arg2, ctx| match (arg1, arg2) {
        (ValueRef::Scalar(arg1), ValueRef::Scalar(arg2)) => {
            let pattern_type = check_pattern_type(arg2, false);
            Ok(Value::Scalar(func(arg1, arg2, ctx, pattern_type)?))
        }
        (ValueRef::Column(arg1), ValueRef::Scalar(arg2)) => {
            let arg1_iter = StringType::iter_column(&arg1);
            let mut builder = MutableBitmap::with_capacity(arg1.len());
            let pattern_type = check_pattern_type(arg2, false);
            for arg1 in arg1_iter {
                builder.push(func(arg1, arg2, ctx, pattern_type)?);
            }
            Ok(Value::Column(builder.into()))
        }
        (ValueRef::Scalar(arg1), ValueRef::Column(arg2)) => {
            let arg2_iter = StringType::iter_column(&arg2);
            let mut builder = MutableBitmap::with_capacity(arg2.len());
            for arg2 in arg2_iter {
                let pattern_type = check_pattern_type(arg2, false);
                builder.push(func(arg1, arg2, ctx, pattern_type)?);
            }
            Ok(Value::Column(builder.into()))
        }
        (ValueRef::Column(arg1), ValueRef::Column(arg2)) => {
            let arg1_iter = StringType::iter_column(&arg1);
            let arg2_iter = StringType::iter_column(&arg2);
            let mut builder = MutableBitmap::with_capacity(arg2.len());
            for (arg1, arg2) in arg1_iter.zip(arg2_iter) {
                let pattern_type = check_pattern_type(arg2, false);
                builder.push(func(arg1, arg2, ctx, pattern_type)?);
            }
            Ok(Value::Column(builder.into()))
        }
    }
}

fn vectorize_regexp(
    func: impl Fn(
        &[u8],
        &[u8],
        EvalContext,
        &mut HashMap<Vec<u8>, Regex>,
        &mut HashMap<Vec<u8>, String>,
    ) -> Result<bool, String>
    + Copy,
) -> impl Fn(
    ValueRef<StringType>,
    ValueRef<StringType>,
    EvalContext,
) -> Result<Value<BooleanType>, String>
+ Copy {
    move |arg1, arg2, ctx| {
        let mut map = HashMap::new();
        let mut string_map = HashMap::new();
        match (arg1, arg2) {
            (ValueRef::Scalar(arg1), ValueRef::Scalar(arg2)) => Ok(Value::Scalar(func(
                arg1,
                arg2,
                ctx,
                &mut map,
                &mut string_map,
            )?)),
            (ValueRef::Column(arg1), ValueRef::Scalar(arg2)) => {
                let arg1_iter = StringType::iter_column(&arg1);
                let mut builder = MutableBitmap::with_capacity(arg1.len());
                for arg1 in arg1_iter {
                    builder.push(func(arg1, arg2, ctx, &mut map, &mut string_map)?);
                }
                Ok(Value::Column(builder.into()))
            }
            (ValueRef::Scalar(arg1), ValueRef::Column(arg2)) => {
                let arg2_iter = StringType::iter_column(&arg2);
                let mut builder = MutableBitmap::with_capacity(arg2.len());
                for arg2 in arg2_iter {
                    builder.push(func(arg1, arg2, ctx, &mut map, &mut string_map)?);
                }
                Ok(Value::Column(builder.into()))
            }
            (ValueRef::Column(arg1), ValueRef::Column(arg2)) => {
                let arg1_iter = StringType::iter_column(&arg1);
                let arg2_iter = StringType::iter_column(&arg2);
                let mut builder = MutableBitmap::with_capacity(arg2.len());
                for (arg1, arg2) in arg1_iter.zip(arg2_iter) {
                    builder.push(func(arg1, arg2, ctx, &mut map, &mut string_map)?);
                }
                Ok(Value::Column(builder.into()))
            }
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum PatternType {
    // e.g. 'Arrow'
    OrdinalStr,
    // e.g. 'A%row'
    PatternStr,
    // e.g. '%rrow'
    StartOfPercent,
    // e.g. 'Arro%'
    EndOfPercent,
}

#[inline]
fn is_like_pattern_escape(c: char) -> bool {
    c == '%' || c == '_' || c == '\\'
}

/// Check the like pattern type.
///
/// is_pruning: indicate whether to be called on range_filter for pruning.
///
/// For example:
///
/// 'a\\%row'
/// '\\%' will be escaped to a percent. Need transform to `a%row`.
///
/// If is_pruning is true, will be called on range_filter:L379.
/// OrdinalStr is returned, because the pattern can be transformed by range_filter:L382.
///
/// If is_pruning is false, will be called on like.rs:L74.
/// PatternStr is returned, because the pattern cannot be used directly on like.rs:L76.
#[inline]
pub fn check_pattern_type(pattern: &[u8], is_pruning: bool) -> PatternType {
    let len = pattern.len();
    if len == 0 {
        return PatternType::OrdinalStr;
    }

    let mut index = 0;
    let start_percent = pattern[0] == b'%';
    if start_percent {
        if is_pruning {
            return PatternType::PatternStr;
        }
        index += 1;
    }

    while index < len {
        match pattern[index] {
            b'_' => return PatternType::PatternStr,
            b'%' => {
                if index == len - 1 && !start_percent {
                    return PatternType::EndOfPercent;
                }
                return PatternType::PatternStr;
            }
            b'\\' => {
                if index < len - 1 {
                    index += 1;
                    if !is_pruning && is_like_pattern_escape(pattern[index] as char) {
                        return PatternType::PatternStr;
                    }
                }
            }
            _ => {}
        }
        index += 1;
    }

    if start_percent {
        PatternType::StartOfPercent
    } else {
        PatternType::OrdinalStr
    }
}

#[inline]
fn decode_one(data: &[u8]) -> Option<(u8, usize)> {
    if data.is_empty() {
        None
    } else {
        Some((data[0], 1))
    }
}

#[inline]
/// Borrow from [tikv](https://github.com/tikv/tikv/blob/fe997db4db8a5a096f8a45c0db3eb3c2e5879262/components/tidb_query_expr/src/impl_like.rs)
fn like(haystack: &[u8], pattern: &[u8]) -> bool {
    // current search positions in pattern and target.
    let (mut px, mut tx) = (0, 0);
    // positions for backtrace.
    let (mut next_px, mut next_tx) = (0, 0);
    while px < pattern.len() || tx < haystack.len() {
        if let Some((c, mut poff)) = decode_one(&pattern[px..]) {
            let code: u32 = c.into();
            if code == '_' as u32 {
                if let Some((_, toff)) = decode_one(&haystack[tx..]) {
                    px += poff;
                    tx += toff;
                    continue;
                }
            } else if code == '%' as u32 {
                // update the backtrace point.
                next_px = px;
                px += poff;
                next_tx = tx;
                next_tx += if let Some((_, toff)) = decode_one(&haystack[tx..]) {
                    toff
                } else {
                    1
                };
                continue;
            } else {
                if code == '\\' as u32 && px + poff < pattern.len() {
                    px += poff;
                    poff = if let Some((_, off)) = decode_one(&pattern[px..]) {
                        off
                    } else {
                        break;
                    }
                }
                if let Some((_, toff)) = decode_one(&haystack[tx..]) {
                    if let std::cmp::Ordering::Equal =
                        haystack[tx..tx + toff].cmp(&pattern[px..px + poff])
                    {
                        tx += toff;
                        px += poff;
                        continue;
                    }
                }
            }
        }
        // mismatch and backtrace to last %.
        if 0 < next_tx && next_tx <= haystack.len() {
            px = next_px;
            tx = next_tx;
            continue;
        }
        return false;
    }
    true
}
