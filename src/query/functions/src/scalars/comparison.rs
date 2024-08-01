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

use databend_common_arrow::arrow::bitmap::MutableBitmap;
use databend_common_expression::generate_like_pattern;
use databend_common_expression::types::boolean::BooleanDomain;
use databend_common_expression::types::string::StringDomain;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::EmptyArrayType;
use databend_common_expression::types::GenericType;
use databend_common_expression::types::NumberClass;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::ValueType;
use databend_common_expression::types::VariantType;
use databend_common_expression::types::ALL_NUMBER_CLASSES;
use databend_common_expression::values::Value;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::Column;
use databend_common_expression::EvalContext;
use databend_common_expression::Function;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::LikePattern;
use databend_common_expression::ScalarRef;
use databend_common_expression::SimpleDomainCmp;
use databend_common_expression::ValueRef;
use memchr::memmem;
use regex::Regex;

use crate::scalars::decimal::register_decimal_compare_op;
use crate::scalars::string_multi_args::regexp;

pub fn register(registry: &mut FunctionRegistry) {
    register_variant_cmp(registry);
    register_string_cmp(registry);
    register_date_cmp(registry);
    register_timestamp_cmp(registry);
    register_number_cmp(registry);
    register_boolean_cmp(registry);
    register_array_cmp(registry);
    register_tuple_cmp(registry);
    register_like(registry);
}

pub const ALL_COMP_FUNC_NAMES: &[&str] = &["eq", "noteq", "lt", "lte", "gt", "gte", "contains"];

const ALL_TRUE_DOMAIN: BooleanDomain = BooleanDomain {
    has_true: true,
    has_false: false,
};

const ALL_FALSE_DOMAIN: BooleanDomain = BooleanDomain {
    has_true: false,
    has_false: true,
};

fn register_variant_cmp(registry: &mut FunctionRegistry) {
    registry.register_2_arg::<VariantType, VariantType, BooleanType, _, _>(
        "eq",
        |_, _, _| FunctionDomain::Full,
        |lhs, rhs, _| {
            jsonb::compare(lhs, rhs).expect("unable to parse jsonb value") == Ordering::Equal
        },
    );
    registry.register_2_arg::<VariantType, VariantType, BooleanType, _, _>(
        "noteq",
        |_, _, _| FunctionDomain::Full,
        |lhs, rhs, _| {
            jsonb::compare(lhs, rhs).expect("unable to parse jsonb value") != Ordering::Equal
        },
    );
    registry.register_2_arg::<VariantType, VariantType, BooleanType, _, _>(
        "gt",
        |_, _, _| FunctionDomain::Full,
        |lhs, rhs, _| {
            jsonb::compare(lhs, rhs).expect("unable to parse jsonb value") == Ordering::Greater
        },
    );
    registry.register_2_arg::<VariantType, VariantType, BooleanType, _, _>(
        "gte",
        |_, _, _| FunctionDomain::Full,
        |lhs, rhs, _| {
            jsonb::compare(lhs, rhs).expect("unable to parse jsonb value") != Ordering::Less
        },
    );
    registry.register_2_arg::<VariantType, VariantType, BooleanType, _, _>(
        "lt",
        |_, _, _| FunctionDomain::Full,
        |lhs, rhs, _| {
            jsonb::compare(lhs, rhs).expect("unable to parse jsonb value") == Ordering::Less
        },
    );
    registry.register_2_arg::<VariantType, VariantType, BooleanType, _, _>(
        "lte",
        |_, _, _| FunctionDomain::Full,
        |lhs, rhs, _| {
            jsonb::compare(lhs, rhs).expect("unable to parse jsonb value") != Ordering::Greater
        },
    );
}

macro_rules! register_simple_domain_type_cmp {
    ($registry:ident, $T:ty) => {
        $registry.register_2_arg::<$T, $T, BooleanType, _, _>(
            "eq",
            |_, d1, d2| d1.domain_eq(d2),
            |lhs, rhs, _| lhs == rhs,
        );
        $registry.register_2_arg::<$T, $T, BooleanType, _, _>(
            "noteq",
            |_, d1, d2| d1.domain_noteq(d2),
            |lhs, rhs, _| lhs != rhs,
        );
        $registry.register_2_arg::<$T, $T, BooleanType, _, _>(
            "gt",
            |_, d1, d2| d1.domain_gt(d2),
            |lhs, rhs, _| lhs > rhs,
        );
        $registry.register_2_arg::<$T, $T, BooleanType, _, _>(
            "gte",
            |_, d1, d2| d1.domain_gte(d2),
            |lhs, rhs, _| lhs >= rhs,
        );
        $registry.register_2_arg::<$T, $T, BooleanType, _, _>(
            "lt",
            |_, d1, d2| d1.domain_lt(d2),
            |lhs, rhs, _| lhs < rhs,
        );
        $registry.register_2_arg::<$T, $T, BooleanType, _, _>(
            "lte",
            |_, d1, d2| d1.domain_lte(d2),
            |lhs, rhs, _| lhs <= rhs,
        );
    };
}

fn register_string_cmp(registry: &mut FunctionRegistry) {
    register_simple_domain_type_cmp!(registry, StringType);
}

fn register_date_cmp(registry: &mut FunctionRegistry) {
    register_simple_domain_type_cmp!(registry, DateType);
}

fn register_timestamp_cmp(registry: &mut FunctionRegistry) {
    register_simple_domain_type_cmp!(registry, TimestampType);
}

fn register_boolean_cmp(registry: &mut FunctionRegistry) {
    registry.register_2_arg::<BooleanType, BooleanType, BooleanType, _, _>(
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
    registry.register_2_arg::<BooleanType, BooleanType, BooleanType, _, _>(
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
    registry.register_2_arg::<BooleanType, BooleanType, BooleanType, _, _>(
        "gt",
        |_, d1, d2| match (d1.has_true, d1.has_false, d2.has_true, d2.has_false) {
            (true, false, false, true) => FunctionDomain::Domain(ALL_TRUE_DOMAIN),
            (false, true, _, _) => FunctionDomain::Domain(ALL_FALSE_DOMAIN),
            _ => FunctionDomain::Full,
        },
        |lhs, rhs, _| lhs & !rhs,
    );
    registry.register_2_arg::<BooleanType, BooleanType, BooleanType, _, _>(
        "gte",
        |_, d1, d2| match (d1.has_true, d1.has_false, d2.has_true, d2.has_false) {
            (true, false, _, _) => FunctionDomain::Domain(ALL_TRUE_DOMAIN),
            (_, _, false, true) => FunctionDomain::Domain(ALL_TRUE_DOMAIN),
            (false, true, true, false) => FunctionDomain::Domain(ALL_FALSE_DOMAIN),
            _ => FunctionDomain::Full,
        },
        |lhs, rhs, _| lhs | !rhs,
    );
    registry.register_2_arg::<BooleanType, BooleanType, BooleanType, _, _>(
        "lt",
        |_, d1, d2| match (d1.has_true, d1.has_false, d2.has_true, d2.has_false) {
            (false, true, true, false) => FunctionDomain::Domain(ALL_TRUE_DOMAIN),
            (_, _, false, true) => FunctionDomain::Domain(ALL_FALSE_DOMAIN),
            _ => FunctionDomain::Full,
        },
        |lhs, rhs, _| !lhs & rhs,
    );
    registry.register_2_arg::<BooleanType, BooleanType, BooleanType, _, _>(
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
                register_decimal_compare_op(registry)
            }
            NumberClass::Decimal256 => {
                // already registered in Decimal128 branch
            }
        });
    }
}

fn register_array_cmp(registry: &mut FunctionRegistry) {
    registry.register_2_arg::<EmptyArrayType, EmptyArrayType, BooleanType, _, _>(
        "eq",
        |_, _, _| FunctionDomain::Domain(ALL_TRUE_DOMAIN),
        |_, _, _| true,
    );
    registry.register_2_arg::<EmptyArrayType, EmptyArrayType, BooleanType, _, _>(
        "noteq",
        |_, _, _| FunctionDomain::Domain(ALL_FALSE_DOMAIN),
        |_, _, _| false,
    );
    registry.register_2_arg::<EmptyArrayType, EmptyArrayType, BooleanType, _, _>(
        "gt",
        |_, _, _| FunctionDomain::Domain(ALL_FALSE_DOMAIN),
        |_, _, _| false,
    );
    registry.register_2_arg::<EmptyArrayType, EmptyArrayType, BooleanType, _, _>(
        "gte",
        |_, _, _| FunctionDomain::Domain(ALL_TRUE_DOMAIN),
        |_, _, _| true,
    );
    registry.register_2_arg::<EmptyArrayType, EmptyArrayType, BooleanType, _, _>(
        "lt",
        |_, _, _| FunctionDomain::Domain(ALL_FALSE_DOMAIN),
        |_, _, _| false,
    );
    registry.register_2_arg::<EmptyArrayType, EmptyArrayType, BooleanType, _, _>(
        "lte",
        |_, _, _| FunctionDomain::Domain(ALL_TRUE_DOMAIN),
        |_, _, _| true,
    );

    registry
        .register_2_arg::<ArrayType<GenericType<0>>, ArrayType<GenericType<0>>, BooleanType, _, _>(
            "eq",
            |_, _, _| FunctionDomain::Full,
            |lhs, rhs, _| lhs == rhs,
        );
    registry
        .register_2_arg::<ArrayType<GenericType<0>>, ArrayType<GenericType<0>>, BooleanType, _, _>(
            "noteq",
            |_, _, _| FunctionDomain::Full,
            |lhs, rhs, _| lhs != rhs,
        );
    registry
        .register_2_arg::<ArrayType<GenericType<0>>, ArrayType<GenericType<0>>, BooleanType, _, _>(
            "gt",
            |_, _, _| FunctionDomain::Full,
            |lhs, rhs, _| lhs > rhs,
        );
    registry
        .register_2_arg::<ArrayType<GenericType<0>>, ArrayType<GenericType<0>>, BooleanType, _, _>(
            "gte",
            |_, _, _| FunctionDomain::Full,
            |lhs, rhs, _| lhs >= rhs,
        );
    registry
        .register_2_arg::<ArrayType<GenericType<0>>, ArrayType<GenericType<0>>, BooleanType, _, _>(
            "lt",
            |_, _, _| FunctionDomain::Full,
            |lhs, rhs, _| lhs < rhs,
        );
    registry
        .register_2_arg::<ArrayType<GenericType<0>>, ArrayType<GenericType<0>>, BooleanType, _, _>(
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
        registry.register_function_factory(name, move |_, args_type| {
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
                    calc_domain: Box::new(move |_, _| FunctionDomain::Full),
                    eval: Box::new(move |args, _| {
                        let len = args.iter().find_map(|arg| match arg {
                            ValueRef::Column(col) => Some(col.len()),
                            _ => None,
                        });

                        let lhs_fields: Vec<ValueRef<AnyType>> = match &args[0] {
                            ValueRef::Scalar(ScalarRef::Tuple(fields)) => {
                                fields.iter().cloned().map(ValueRef::Scalar).collect()
                            }
                            ValueRef::Column(Column::Tuple(fields)) => {
                                fields.iter().cloned().map(ValueRef::Column).collect()
                            }
                            _ => unreachable!(),
                        };
                        let rhs_fields: Vec<ValueRef<AnyType>> = match &args[1] {
                            ValueRef::Scalar(ScalarRef::Tuple(fields)) => {
                                fields.iter().cloned().map(ValueRef::Scalar).collect()
                            }
                            ValueRef::Column(Column::Tuple(fields)) => {
                                fields.iter().cloned().map(ValueRef::Column).collect()
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
                },
            }))
        });
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
        variant_vectorize_like(|val, pat, _, pattern_type| {
            match &pattern_type {
                LikePattern::OrdinalStr => {
                    if let Some(s) = jsonb::as_str(val) {
                        LikePattern::ordinal_str(s.as_bytes(), pat)
                    } else {
                        false
                    }
                }
                LikePattern::EndOfPercent => {
                    // fast path, can use starts_with
                    if let Some(s) = jsonb::as_str(val) {
                        LikePattern::end_of_percent(s.as_bytes(), pat)
                    } else {
                        false
                    }
                }
                LikePattern::StartOfPercent => {
                    // fast path, can use ends_with
                    if let Some(s) = jsonb::as_str(val) {
                        LikePattern::start_of_percent(s.as_bytes(), pat)
                    } else {
                        false
                    }
                }
                LikePattern::SurroundByPercent => {
                    jsonb::traverse_check_string(val, |v| LikePattern::surround_by_percent(v, pat))
                }
                LikePattern::SimplePattern(simple_pattern) => {
                    jsonb::traverse_check_string(val, |v| {
                        LikePattern::simple_pattern(
                            v,
                            simple_pattern.0,
                            simple_pattern.1,
                            &simple_pattern.2,
                        )
                    })
                }
                LikePattern::ComplexPattern => {
                    jsonb::traverse_check_string(val, |v| LikePattern::complex_pattern(v, pat))
                }
            }
        }),
    );

    registry.register_passthrough_nullable_2_arg::<StringType, StringType, BooleanType, _, _>(
        "like",
        |_, lhs, rhs| {
            if rhs.max.as_ref() == Some(&rhs.min) {
                let pattern_type = generate_like_pattern(rhs.min.as_bytes());

                if pattern_type == LikePattern::OrdinalStr {
                    return lhs.domain_eq(rhs);
                }

                if pattern_type == LikePattern::EndOfPercent {
                    let mut pat_str = rhs.min.clone();
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
                    return lhs.domain_eq(&other);
                }
            }
            FunctionDomain::Full
        },
        vectorize_like(|str, pat, _, pattern_type| match &pattern_type {
            LikePattern::OrdinalStr => LikePattern::ordinal_str(str, pat),
            LikePattern::EndOfPercent => LikePattern::end_of_percent(str, pat),
            LikePattern::StartOfPercent => LikePattern::start_of_percent(str, pat),
            LikePattern::SurroundByPercent => LikePattern::surround_by_percent(str, pat),
            LikePattern::ComplexPattern => LikePattern::complex_pattern(str, pat),
            LikePattern::SimplePattern(simple_pattern) => LikePattern::simple_pattern(
                str,
                simple_pattern.0,
                simple_pattern.1,
                &simple_pattern.2,
            ),
        }),
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
}

fn vectorize_like(
    func: impl Fn(&[u8], &[u8], &mut EvalContext, &LikePattern) -> bool + Copy,
) -> impl Fn(ValueRef<StringType>, ValueRef<StringType>, &mut EvalContext) -> Value<BooleanType> + Copy
{
    move |arg1, arg2, ctx| match (arg1, arg2) {
        (ValueRef::Scalar(arg1), ValueRef::Scalar(arg2)) => {
            let pattern_type = generate_like_pattern(arg2.as_bytes());
            Value::Scalar(func(arg1.as_bytes(), arg2.as_bytes(), ctx, &pattern_type))
        }
        (ValueRef::Column(arg1), ValueRef::Scalar(arg2)) => {
            let arg1_iter = StringType::iter_column(&arg1);

            let pattern_type = generate_like_pattern(arg2.as_bytes());
            // faster path for memmem to have a single instance of Finder
            if pattern_type == LikePattern::SurroundByPercent && arg2.len() > 2 {
                let finder = memmem::Finder::new(&arg2[1..arg2.len() - 1]);
                let it = arg1_iter.map(|arg1| finder.find(arg1.as_bytes()).is_some());
                let bitmap = BooleanType::column_from_iter(it, &[]);
                return Value::Column(bitmap);
            }

            let mut builder = MutableBitmap::with_capacity(arg1.len());
            for arg1 in arg1_iter {
                builder.push(func(arg1.as_bytes(), arg2.as_bytes(), ctx, &pattern_type));
            }
            Value::Column(builder.into())
        }
        (ValueRef::Scalar(arg1), ValueRef::Column(arg2)) => {
            let arg2_iter = StringType::iter_column(&arg2);
            let mut builder = MutableBitmap::with_capacity(arg2.len());
            for arg2 in arg2_iter {
                let pattern_type = generate_like_pattern(arg2.as_bytes());
                builder.push(func(arg1.as_bytes(), arg2.as_bytes(), ctx, &pattern_type));
            }
            Value::Column(builder.into())
        }
        (ValueRef::Column(arg1), ValueRef::Column(arg2)) => {
            let arg1_iter = StringType::iter_column(&arg1);
            let arg2_iter = StringType::iter_column(&arg2);
            let mut builder = MutableBitmap::with_capacity(arg2.len());
            for (arg1, arg2) in arg1_iter.zip(arg2_iter) {
                let pattern_type = generate_like_pattern(arg2.as_bytes());
                builder.push(func(arg1.as_bytes(), arg2.as_bytes(), ctx, &pattern_type));
            }
            Value::Column(builder.into())
        }
    }
}

fn variant_vectorize_like(
    func: impl Fn(&[u8], &[u8], &mut EvalContext, &LikePattern) -> bool + Copy,
) -> impl Fn(ValueRef<VariantType>, ValueRef<StringType>, &mut EvalContext) -> Value<BooleanType> + Copy
{
    move |arg1, arg2, ctx| match (arg1, arg2) {
        (ValueRef::Scalar(arg1), ValueRef::Scalar(arg2)) => {
            let pattern_type = generate_like_pattern(arg2.as_bytes());
            Value::Scalar(func(arg1, arg2.as_bytes(), ctx, &pattern_type))
        }
        (ValueRef::Column(arg1), ValueRef::Scalar(arg2)) => {
            let arg1_iter = VariantType::iter_column(&arg1);

            let pattern_type = generate_like_pattern(arg2.as_bytes());
            // faster path for memmem to have a single instance of Finder
            if pattern_type == LikePattern::SurroundByPercent && arg2.len() > 2 {
                let finder = memmem::Finder::new(&arg2[1..arg2.len() - 1]);
                let it = arg1_iter.map(|arg1| finder.find(arg1).is_some());
                let bitmap = BooleanType::column_from_iter(it, &[]);
                return Value::Column(bitmap);
            }

            let mut builder = MutableBitmap::with_capacity(arg1.len());
            for arg1 in arg1_iter {
                builder.push(func(arg1, arg2.as_bytes(), ctx, &pattern_type));
            }
            Value::Column(builder.into())
        }
        (ValueRef::Scalar(arg1), ValueRef::Column(arg2)) => {
            let arg2_iter = StringType::iter_column(&arg2);
            let mut builder = MutableBitmap::with_capacity(arg2.len());
            for arg2 in arg2_iter {
                let pattern_type = generate_like_pattern(arg2.as_bytes());
                builder.push(func(arg1, arg2.as_bytes(), ctx, &pattern_type));
            }
            Value::Column(builder.into())
        }
        (ValueRef::Column(arg1), ValueRef::Column(arg2)) => {
            let arg1_iter = VariantType::iter_column(&arg1);
            let arg2_iter = StringType::iter_column(&arg2);
            let mut builder = MutableBitmap::with_capacity(arg2.len());
            for (arg1, arg2) in arg1_iter.zip(arg2_iter) {
                let pattern_type = generate_like_pattern(arg2.as_bytes());
                builder.push(func(arg1, arg2.as_bytes(), ctx, &pattern_type));
            }
            Value::Column(builder.into())
        }
    }
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
) -> impl Fn(ValueRef<StringType>, ValueRef<StringType>, &mut EvalContext) -> Value<BooleanType> + Copy
{
    move |arg1, arg2, ctx| {
        let mut map = HashMap::new();
        let mut string_map = HashMap::new();
        match (arg1, arg2) {
            (ValueRef::Scalar(arg1), ValueRef::Scalar(arg2)) => {
                let mut builder = MutableBitmap::with_capacity(1);
                func(arg1, arg2, &mut builder, ctx, &mut map, &mut string_map);
                Value::Scalar(BooleanType::build_scalar(builder))
            }
            (ValueRef::Column(arg1), ValueRef::Scalar(arg2)) => {
                let arg1_iter = StringType::iter_column(&arg1);
                let mut builder = MutableBitmap::with_capacity(arg1.len());
                for arg1 in arg1_iter {
                    func(arg1, arg2, &mut builder, ctx, &mut map, &mut string_map);
                }
                Value::Column(builder.into())
            }
            (ValueRef::Scalar(arg1), ValueRef::Column(arg2)) => {
                let arg2_iter = StringType::iter_column(&arg2);
                let mut builder = MutableBitmap::with_capacity(arg2.len());
                for arg2 in arg2_iter {
                    func(arg1, arg2, &mut builder, ctx, &mut map, &mut string_map);
                }
                Value::Column(builder.into())
            }
            (ValueRef::Column(arg1), ValueRef::Column(arg2)) => {
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
