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
use std::marker::PhantomData;
use std::sync::Arc;

use databend_common_column::types::months_days_micros;
use databend_common_column::types::timestamp_tz;
use databend_common_expression::Column;
use databend_common_expression::Domain;
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
use databend_common_expression::comparison::ConstantComparison;
use databend_common_expression::comparison::ConstantComparisonAdapter;
use databend_common_expression::comparison::GtOp;
use databend_common_expression::comparison::GteOp;
use databend_common_expression::comparison::LtOp;
use databend_common_expression::comparison::LteOp;
use databend_common_expression::comparison::StatComparisonOp;
use databend_common_expression::comparison::estimate_ndv_true_count;
use databend_common_expression::comparison::null_comparison_stat;
use databend_common_expression::function_stat::ReturnStat;
use databend_common_expression::generate_like_pattern;
use databend_common_expression::scalar_evaluator;
use databend_common_expression::stat_distribution::ArgStat;
use databend_common_expression::stat_distribution::StatBinaryArg;
use databend_common_expression::stat_distribution::StatEstimate;
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
use databend_common_statistics::Histogram;
use databend_common_statistics::TypedHistogram;
use databend_common_statistics::TypedHistogramBucket;
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
        register_stat_comparison_2_arg::<$T>(
            $registry,
            "eq",
            |stat, _| derive_equality_stat::<$T>(false, stat),
            |_, d1, d2| d1.domain_eq(d2),
            |lhs, rhs, _| lhs == rhs,
        );
        register_stat_comparison_2_arg::<$T>(
            $registry,
            "noteq",
            |stat, _| derive_equality_stat::<$T>(true, stat),
            |_, d1, d2| d1.domain_noteq(d2),
            |lhs, rhs, _| lhs != rhs,
        );
        register_stat_comparison_2_arg::<$T>(
            $registry,
            "gt",
            |stat, _| derive_comparison_stat::<$T, GtOp>(stat),
            |_, d1, d2| d1.domain_gt(d2),
            |lhs, rhs, _| lhs > rhs,
        );
        register_stat_comparison_2_arg::<$T>(
            $registry,
            "gte",
            |stat, _| derive_comparison_stat::<$T, GteOp>(stat),
            |_, d1, d2| d1.domain_gte(d2),
            |lhs, rhs, _| lhs >= rhs,
        );
        register_stat_comparison_2_arg::<$T>(
            $registry,
            "lt",
            |stat, _| derive_comparison_stat::<$T, LtOp>(stat),
            |_, d1, d2| d1.domain_lt(d2),
            |lhs, rhs, _| lhs < rhs,
        );
        register_stat_comparison_2_arg::<$T>(
            $registry,
            "lte",
            |stat, _| derive_comparison_stat::<$T, LteOp>(stat),
            |_, d1, d2| d1.domain_lte(d2),
            |lhs, rhs, _| lhs <= rhs,
        );
    };
}

fn register_stat_comparison_2_arg<T>(
    registry: &mut FunctionRegistry,
    name: &'static str,
    derive_stat: fn(
        StatBinaryArg,
        &databend_common_expression::FunctionContext,
    ) -> Result<Option<ReturnStat>, String>,
    calc_domain: fn(
        &databend_common_expression::FunctionContext,
        &T::Domain,
        &T::Domain,
    ) -> FunctionDomain<BooleanType>,
    func: fn(T::ScalarRef<'_>, T::ScalarRef<'_>, &mut EvalContext) -> bool,
) where
    T: ArgType,
    for<'a> T::ScalarRef<'a>: Copy,
{
    registry
        .scalar_builder(name)
        .function()
        .typed_2_arg::<T, T, BooleanType>()
        .passthrough_nullable()
        .calc_domain(calc_domain)
        .derive_stat(derive_stat)
        .vectorized(databend_common_expression::vectorize_cmp_2_arg(func))
        .register();
}

fn derive_equality_stat<T>(
    not_eq: bool,
    stat: StatBinaryArg,
) -> Result<Option<ReturnStat>, String>
where
    T: ComparisonStatType,
{
    if let Some(stat) = null_comparison_stat(&stat) {
        return Ok(Some(stat));
    }

    let Some((input, _)) = ConstantComparison::<TypedComparisonStat<T>>::from_args(&stat)? else {
        return Ok(None);
    };

    let true_count = input.equality_true_count(
        input
            .domain
            .as_ref()
            .and_then(T::domain_bounds)
            .map(|(min, max)| {
                (
                    T::compare(T::to_scalar_ref(&input.constant), min),
                    T::compare(T::to_scalar_ref(&input.constant), max),
                )
            }),
        not_eq,
    );

    Ok(Some(input.boolean_stat(true_count)))
}

fn derive_comparison_stat<T, Op: StatComparisonOp>(
    stat: StatBinaryArg,
) -> Result<Option<ReturnStat>, String>
where
    T: ComparisonStatType,
    T::Scalar: HistogramConstant,
{
    if let Some(stat) = null_comparison_stat(&stat) {
        return Ok(Some(stat));
    }

    let Some((input, reverse)) = ConstantComparison::<TypedComparisonStat<T>>::from_args(&stat)?
    else {
        return Ok(None);
    };

    let true_count = if reverse {
        ordered_comparison_true_count::<T, Op::Reverse>(&input)?
    } else {
        ordered_comparison_true_count::<T, Op>(&input)?
    };
    Ok(true_count.map(|true_count| input.boolean_stat(true_count)))
}

fn ordered_comparison_true_count<T, Op: StatComparisonOp>(
    input: &ConstantComparison<'_, '_, TypedComparisonStat<T>>,
) -> Result<Option<StatEstimate>, String>
where
    T: ComparisonStatType,
    T::Scalar: HistogramConstant,
{
    if let Some(histogram) = input.stat.histogram() {
        return Ok(Some(
            HistogramComparison::<_, Op> {
                histogram,
                constant: &input.constant,
                non_null_cardinality: input.non_null_cardinality,
                _op: PhantomData,
            }
            .true_count()?,
        ));
    }

    if T::USE_INTEGER_RANGE_COMPARISON {
        let range = IntegerRangeComparison::from_input(input)?;
        return Ok(Some(range.true_count::<Op>()));
    }

    let Some((cmp_min, cmp_max)) =
        input
            .domain
            .as_ref()
            .and_then(T::domain_bounds)
            .map(|(min, max)| {
                (
                    T::compare(T::to_scalar_ref(&input.constant), min),
                    T::compare(T::to_scalar_ref(&input.constant), max),
                )
            })
    else {
        return Ok(None);
    };

    Ok(Op::range_true_count(
        input.stat.ndv,
        input.non_null_cardinality,
        cmp_min,
        cmp_max,
    ))
}

struct TypedComparisonStat<T> {
    _marker: PhantomData<fn(T)>,
}

impl<T> Clone for TypedComparisonStat<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for TypedComparisonStat<T> {}

impl<T> ConstantComparisonAdapter for TypedComparisonStat<T>
where T: ComparisonStatType
{
    type Value = T::Scalar;
    type Domain = T::Domain;

    fn constant(scalar: Scalar) -> Result<T::Scalar, String> {
        T::try_downcast_scalar(&scalar.as_ref())
            .map(T::to_owned_scalar)
            .map_err(|e| e.to_string())
    }

    fn domain(domain: &Domain) -> Result<T::Domain, String> {
        T::try_downcast_domain(domain).map_err(|e| e.to_string())
    }
}

trait ComparisonStatType: ArgType {
    const USE_INTEGER_RANGE_COMPARISON: bool = false;

    fn domain_bounds(domain: &Self::Domain) -> Option<(Self::ScalarRef<'_>, Self::ScalarRef<'_>)>;
}

impl<T: Number> ComparisonStatType for NumberType<T> {
    const USE_INTEGER_RANGE_COMPARISON: bool = !T::FLOATING;

    fn domain_bounds(domain: &Self::Domain) -> Option<(Self::ScalarRef<'_>, Self::ScalarRef<'_>)> {
        Some((
            NumberType::<T>::to_scalar_ref(&domain.min),
            NumberType::<T>::to_scalar_ref(&domain.max),
        ))
    }
}

impl ComparisonStatType for BooleanType {
    fn domain_bounds(domain: &Self::Domain) -> Option<(Self::ScalarRef<'_>, Self::ScalarRef<'_>)> {
        Some((!domain.has_false, domain.has_true))
    }
}

impl ComparisonStatType for StringType {
    fn domain_bounds(domain: &Self::Domain) -> Option<(Self::ScalarRef<'_>, Self::ScalarRef<'_>)> {
        Some((domain.min.as_str(), domain.max.as_deref()?))
    }
}

macro_rules! impl_simple_domain_stat_type {
    ($($ty:ty => $use_integer_range:expr),* $(,)?) => {
        $(
            impl ComparisonStatType for $ty {
                const USE_INTEGER_RANGE_COMPARISON: bool = $use_integer_range;

                fn domain_bounds(
                    domain: &Self::Domain,
                ) -> Option<(Self::ScalarRef<'_>, Self::ScalarRef<'_>)> {
                    Some((Self::to_scalar_ref(&domain.min), Self::to_scalar_ref(&domain.max)))
                }
            }
        )*
    };
}

impl_simple_domain_stat_type!(
    DateType => true,
    TimestampType => true,
    TimestampTzType => false,
    IntervalType => false,
);

struct HistogramComparison<'a, T, Op> {
    histogram: &'a Histogram,
    constant: &'a T,
    non_null_cardinality: f64,
    _op: PhantomData<fn(Op)>,
}

impl<T: HistogramConstant, Op: StatComparisonOp> HistogramComparison<'_, T, Op> {
    fn true_count(&self) -> Result<StatEstimate, String> {
        let histogram_num_values = self.histogram.num_values();
        if histogram_num_values == 0.0 {
            return Ok(StatEstimate::exact(0.0));
        }

        let selected_count = self.selected_count()?;
        let factor = self.non_null_cardinality / histogram_num_values;
        Ok(scale_estimate(selected_count, factor))
    }

    fn selected_count(&self) -> Result<StatEstimate, String> {
        match self.histogram {
            Histogram::Int(histogram) => {
                let constant = self
                    .constant
                    .histogram_i64()
                    .ok_or_else(|| unexpected_histogram_constant("Int", self.constant))?;
                Ok(TypedHistogramScan::<_, Op> {
                    histogram,
                    constant: &constant,
                    selected: StatEstimate::exact(0.0),
                    row_scale: histogram.row_scale,
                    _op: PhantomData,
                }
                .selected_count(HistogramBucketComparison::number_selected_count))
            }
            Histogram::UInt(histogram) => {
                let constant = self
                    .constant
                    .histogram_u64()
                    .ok_or_else(|| unexpected_histogram_constant("UInt", self.constant))?;
                Ok(TypedHistogramScan::<_, Op> {
                    histogram,
                    constant: &constant,
                    selected: StatEstimate::exact(0.0),
                    row_scale: histogram.row_scale,
                    _op: PhantomData,
                }
                .selected_count(HistogramBucketComparison::number_selected_count))
            }
            Histogram::Float(histogram) => {
                let constant = self
                    .constant
                    .histogram_f64()
                    .ok_or_else(|| unexpected_histogram_constant("Float", self.constant))?;
                Ok(TypedHistogramScan::<_, Op> {
                    histogram,
                    constant: &constant,
                    selected: StatEstimate::exact(0.0),
                    row_scale: histogram.row_scale,
                    _op: PhantomData,
                }
                .selected_count(HistogramBucketComparison::number_selected_count))
            }
            Histogram::Bytes(histogram) => {
                let Some(constant) = self.constant.histogram_bytes() else {
                    return Err(unexpected_histogram_constant("Bytes", self.constant));
                };
                let constant = constant.to_vec();
                Ok(TypedHistogramScan::<_, Op> {
                    histogram,
                    constant: &constant,
                    selected: StatEstimate::exact(0.0),
                    row_scale: histogram.row_scale,
                    _op: PhantomData,
                }
                .selected_count(HistogramBucketComparison::bytes_selected_count))
            }
        }
    }
}

fn add_estimate(left: StatEstimate, right: StatEstimate) -> StatEstimate {
    StatEstimate::new(
        left.lower + right.lower,
        left.expected + right.expected,
        left.upper + right.upper,
    )
}

fn scale_estimate(estimate: StatEstimate, factor: f64) -> StatEstimate {
    StatEstimate::new(
        estimate.lower * factor,
        estimate.expected * factor,
        estimate.upper * factor,
    )
}

struct TypedHistogramScan<'a, T, Op> {
    histogram: &'a TypedHistogram<T>,
    constant: &'a T,
    selected: StatEstimate,
    row_scale: f64,
    _op: PhantomData<fn(Op)>,
}

impl<'a, T: Ord, Op: StatComparisonOp> TypedHistogramScan<'a, T, Op> {
    fn selected_count(
        mut self,
        estimate_partial_bucket_count: impl Fn(&HistogramBucketComparison<'a, T, Op>) -> StatEstimate,
    ) -> StatEstimate {
        for bucket in &self.histogram.buckets {
            let bucket = HistogramBucketComparison::<_, Op> {
                bucket,
                constant: self.constant,
                row_scale: self.row_scale,
                _op: PhantomData,
            };
            match bucket.overlap() {
                HistogramBucketOverlap::None => {}
                HistogramBucketOverlap::Complete => {
                    self.selected =
                        add_estimate(self.selected, StatEstimate::exact(bucket.num_values()));
                }
                HistogramBucketOverlap::Partial => {
                    self.selected =
                        add_estimate(self.selected, estimate_partial_bucket_count(&bucket));
                }
            }
        }

        self.selected
    }
}

struct HistogramBucketComparison<'a, T, Op> {
    bucket: &'a TypedHistogramBucket<T>,
    constant: &'a T,
    row_scale: f64,
    _op: PhantomData<fn(Op)>,
}

impl<T: Ord, Op: StatComparisonOp> HistogramBucketComparison<'_, T, Op> {
    fn num_values(&self) -> f64 {
        self.bucket.num_values() * self.row_scale
    }

    fn overlap(&self) -> HistogramBucketOverlap {
        let lower_cmp = self.constant.cmp(self.bucket.lower_bound());
        let upper_cmp = self.constant.cmp(self.bucket.upper_bound());
        if Op::SELECT_LESS {
            if lower_cmp == Ordering::Less || (!Op::INCLUDE_EQUAL && lower_cmp == Ordering::Equal) {
                HistogramBucketOverlap::None
            } else if upper_cmp == Ordering::Greater
                || (Op::INCLUDE_EQUAL && upper_cmp == Ordering::Equal)
            {
                HistogramBucketOverlap::Complete
            } else {
                HistogramBucketOverlap::Partial
            }
        } else if upper_cmp == Ordering::Greater
            || (!Op::INCLUDE_EQUAL && upper_cmp == Ordering::Equal)
        {
            HistogramBucketOverlap::None
        } else if lower_cmp == Ordering::Less || (Op::INCLUDE_EQUAL && lower_cmp == Ordering::Equal)
        {
            HistogramBucketOverlap::Complete
        } else {
            HistogramBucketOverlap::Partial
        }
    }
}

impl<Op: StatComparisonOp> HistogramBucketComparison<'_, Vec<u8>, Op> {
    fn bytes_selected_count(&self) -> StatEstimate {
        let selectivity = if Op::INCLUDE_EQUAL {
            // Bytes buckets only have a coarse partial-bucket model. For the
            // equality mass, a non-empty bucket still has at least one value.
            let ndv = if self.bucket.num_distinct() < 1.0 {
                1.0
            } else {
                self.bucket.num_distinct()
            };
            if ndv <= 2.0 { 1.0 } else { 0.5 + 1.0 / ndv }
        } else {
            0.5
        };
        StatEstimate::exact(self.num_values() * selectivity)
    }
}

impl<T: StatNumberValue, Op: StatComparisonOp> HistogramBucketComparison<'_, T, Op> {
    fn number_selected_count(&self) -> StatEstimate {
        let parts = if let Some(parts) = T::partial_discrete_bucket_parts(
            self.bucket.lower_bound(),
            self.bucket.upper_bound(),
            self.constant,
        ) {
            let selected_value_count = match (Op::SELECT_LESS, Op::INCLUDE_EQUAL) {
                (true, false) => parts.strict_less_count,
                (true, true) => parts.strict_less_count + parts.equality_count,
                (false, false) => parts.strict_greater_count,
                (false, true) => parts.strict_greater_count + parts.equality_count,
            };
            return StatEstimate::exact(
                selected_value_count * (self.num_values() / parts.value_count),
            );
        } else {
            let (strict_less, equality) = self.number_less_parts();
            HistogramBucketSelectivity {
                strict_less,
                equality,
                strict_greater: 1.0 - strict_less - equality,
            }
        };
        let selectivity = match (Op::SELECT_LESS, Op::INCLUDE_EQUAL) {
            (true, false) => parts.strict_less,
            (true, true) => parts.strict_less + parts.equality,
            (false, false) => parts.strict_greater,
            (false, true) => parts.strict_greater + parts.equality,
        };
        debug_assert!(
            (0.0..=1.0).contains(&selectivity),
            "invalid numeric bucket selectivity: {selectivity:?}"
        );
        let expected = self.num_values() * selectivity;
        StatEstimate::new(0.0, expected, self.num_values())
    }

    fn number_less_parts(&self) -> (f64, f64) {
        // Scaled buckets can be fractional, but equality mass is based on a
        // count of possible values and must not use a denominator below one.
        let equality = if self.bucket.num_distinct() < 1.0 {
            1.0
        } else {
            1.0 / self.bucket.num_distinct()
        };
        debug_assert!(
            (0.0..=1.0).contains(&equality),
            "invalid numeric bucket equality selectivity: {equality:?}"
        );
        let lower_bound = self.bucket.lower_bound().to_f64();
        let upper_bound = self.bucket.upper_bound().to_f64();
        let const_value = self.constant.to_f64();

        let bucket_range = upper_bound - lower_bound;
        if bucket_range <= 0.0 {
            return (0.0, equality);
        }

        let strict_less = if const_value == lower_bound {
            0.0
        } else if const_value == upper_bound {
            1.0 - equality
        } else {
            let range_selectivity = (const_value - lower_bound) / bucket_range;
            debug_assert!(
                (0.0..=1.0).contains(&range_selectivity),
                "invalid numeric bucket range selectivity: {range_selectivity:?}"
            );
            range_selectivity * (1.0 - equality)
        };

        (strict_less, equality)
    }
}

#[derive(Clone, Copy)]
struct HistogramBucketSelectivity {
    strict_less: f64,
    equality: f64,
    strict_greater: f64,
}

#[derive(Clone, Copy)]
struct DiscreteHistogramBucketParts {
    value_count: f64,
    strict_less_count: f64,
    equality_count: f64,
    strict_greater_count: f64,
}

enum IntegerRangeComparison<'s, 'a> {
    MissingMinMax,
    Bounded {
        stat: &'s ArgStat<'a>,
        min: F64,
        max: F64,
        literal: F64,
        non_null_cardinality: f64,
    },
}

impl<'s, 'a> IntegerRangeComparison<'s, 'a> {
    fn from_input<T>(
        input: &ConstantComparison<'s, 'a, TypedComparisonStat<T>>,
    ) -> Result<Self, String>
    where
        T: ComparisonStatType,
        T::Scalar: HistogramConstant,
    {
        let Some(domain) = input.domain.as_ref() else {
            return Ok(Self::MissingMinMax);
        };
        let Some((min, max)) = T::domain_bounds(domain) else {
            return Ok(Self::MissingMinMax);
        };

        let min = T::to_owned_scalar(min)
            .range_f64()
            .ok_or_else(|| "constant comparison integer range bound is not numeric".to_string())?;
        let max = T::to_owned_scalar(max)
            .range_f64()
            .ok_or_else(|| "constant comparison integer range bound is not numeric".to_string())?;

        Ok(Self::Bounded {
            stat: input.stat,
            min,
            max,
            literal: input.constant.range_f64().ok_or_else(|| {
                "constant comparison integer range literal is not numeric".to_string()
            })?,
            non_null_cardinality: input.non_null_cardinality,
        })
    }

    fn true_count<Op: StatComparisonOp>(&self) -> StatEstimate {
        use std::cmp::Ordering::*;

        let Self::Bounded {
            stat,
            min,
            max,
            literal,
            non_null_cardinality,
        } = self
        else {
            return StatEstimate::exact(0.0);
        };

        let min_value = min.0;
        let max_value = max.0;
        let numeric_literal = literal.0;
        let cmp_min = literal.total_cmp(min);
        let cmp_max = literal.total_cmp(max);

        if Op::SELECT_LESS {
            if cmp_min == Less || !Op::INCLUDE_EQUAL && cmp_min == Equal {
                return StatEstimate::exact(0.0);
            }
            if cmp_min == Equal {
                return estimate_ndv_true_count(stat.ndv, false, *non_null_cardinality);
            }
            if cmp_max == Greater {
                return StatEstimate::exact(*non_null_cardinality);
            }
            if !Op::INCLUDE_EQUAL && cmp_max == Equal {
                return estimate_ndv_true_count(stat.ndv, true, *non_null_cardinality);
            }
            let selected_values = if Op::INCLUDE_EQUAL {
                numeric_literal - min_value + 1.0
            } else {
                numeric_literal - min_value
            };
            return StatEstimate::exact(
                (selected_values / (max_value - min_value + 1.0)) * *non_null_cardinality,
            );
        }

        if Op::INCLUDE_EQUAL {
            if cmp_max == Greater {
                return StatEstimate::exact(0.0);
            }
            if cmp_min == Less || cmp_min == Equal {
                return StatEstimate::exact(*non_null_cardinality);
            }
            if cmp_max == Equal {
                return estimate_ndv_true_count(stat.ndv, false, *non_null_cardinality);
            }
            return StatEstimate::exact(
                ((max_value - numeric_literal + 1.0) / (max_value - min_value + 1.0))
                    * *non_null_cardinality,
            );
        }

        match (cmp_min, cmp_max) {
            (_, Greater | Equal) => StatEstimate::exact(0.0),
            (Less, _) => StatEstimate::exact(*non_null_cardinality),
            (Equal, _) => estimate_ndv_true_count(stat.ndv, true, *non_null_cardinality),
            _ => StatEstimate::exact(
                ((max_value - numeric_literal) / (max_value - min_value + 1.0))
                    * *non_null_cardinality,
            ),
        }
    }
}

enum HistogramBucketOverlap {
    None,
    Partial,
    Complete,
}

trait StatNumberValue: Ord {
    fn to_f64(&self) -> f64;

    fn partial_discrete_bucket_parts(
        _lower_bound: &Self,
        _upper_bound: &Self,
        _constant: &Self,
    ) -> Option<DiscreteHistogramBucketParts> {
        None
    }
}

impl StatNumberValue for i64 {
    fn to_f64(&self) -> f64 {
        *self as f64
    }

    fn partial_discrete_bucket_parts(
        lower_bound: &Self,
        upper_bound: &Self,
        constant: &Self,
    ) -> Option<DiscreteHistogramBucketParts> {
        let value_count = upper_bound.checked_sub(*lower_bound)?.checked_add(1)? as f64;
        let strict_less_count = if constant <= lower_bound {
            0.0
        } else if constant > upper_bound {
            value_count
        } else {
            constant.checked_sub(*lower_bound)? as f64
        };
        let equality_count = if constant >= lower_bound && constant <= upper_bound {
            1.0
        } else {
            0.0
        };
        let strict_greater_count = value_count - strict_less_count - equality_count;

        let strict_less = strict_less_count / value_count;
        let equality = equality_count / value_count;
        let strict_greater = strict_greater_count / value_count;
        debug_assert!(
            (0.0..=1.0).contains(&strict_less),
            "invalid i64 strict-less selectivity: {strict_less:?}"
        );
        debug_assert!(
            (0.0..=1.0).contains(&equality),
            "invalid i64 equality selectivity: {equality:?}"
        );
        debug_assert!(
            (0.0..=1.0).contains(&strict_greater),
            "invalid i64 strict-greater selectivity: {strict_greater:?}"
        );
        Some(DiscreteHistogramBucketParts {
            value_count,
            strict_less_count,
            equality_count,
            strict_greater_count,
        })
    }
}

impl StatNumberValue for u64 {
    fn to_f64(&self) -> f64 {
        *self as f64
    }

    fn partial_discrete_bucket_parts(
        lower_bound: &Self,
        upper_bound: &Self,
        constant: &Self,
    ) -> Option<DiscreteHistogramBucketParts> {
        let value_count = upper_bound.checked_sub(*lower_bound)?.checked_add(1)? as f64;
        let strict_less_count = if constant <= lower_bound {
            0.0
        } else if constant > upper_bound {
            value_count
        } else {
            constant.checked_sub(*lower_bound)? as f64
        };
        let equality_count = if constant >= lower_bound && constant <= upper_bound {
            1.0
        } else {
            0.0
        };
        let strict_greater_count = value_count - strict_less_count - equality_count;

        let strict_less = strict_less_count / value_count;
        let equality = equality_count / value_count;
        let strict_greater = strict_greater_count / value_count;
        debug_assert!(
            (0.0..=1.0).contains(&strict_less),
            "invalid u64 strict-less selectivity: {strict_less:?}"
        );
        debug_assert!(
            (0.0..=1.0).contains(&equality),
            "invalid u64 equality selectivity: {equality:?}"
        );
        debug_assert!(
            (0.0..=1.0).contains(&strict_greater),
            "invalid u64 strict-greater selectivity: {strict_greater:?}"
        );
        Some(DiscreteHistogramBucketParts {
            value_count,
            strict_less_count,
            equality_count,
            strict_greater_count,
        })
    }
}

impl StatNumberValue for F64 {
    fn to_f64(&self) -> f64 {
        self.into_inner()
    }
}

trait HistogramConstant: std::fmt::Debug {
    fn histogram_i64(&self) -> Option<i64> {
        None
    }

    fn histogram_u64(&self) -> Option<u64> {
        None
    }

    fn histogram_f64(&self) -> Option<F64> {
        None
    }

    fn histogram_bytes(&self) -> Option<&[u8]> {
        None
    }

    fn range_f64(&self) -> Option<F64> {
        None
    }
}

macro_rules! impl_signed_histogram_constant {
    ($($ty:ty),* $(,)?) => {
        $(
            impl HistogramConstant for $ty {
                fn histogram_i64(&self) -> Option<i64> {
                    Some(*self as i64)
                }

                fn histogram_f64(&self) -> Option<F64> {
                    Some(F64::from(*self as f64))
                }

                fn range_f64(&self) -> Option<F64> {
                    Some(F64::from(*self as f64))
                }
            }
        )*
    };
}

macro_rules! impl_unsigned_histogram_constant {
    ($($ty:ty),* $(,)?) => {
        $(
            impl HistogramConstant for $ty {
                fn histogram_u64(&self) -> Option<u64> {
                    Some(*self as u64)
                }

                fn histogram_f64(&self) -> Option<F64> {
                    Some(F64::from(*self as f64))
                }

                fn range_f64(&self) -> Option<F64> {
                    Some(F64::from(*self as f64))
                }
            }
        )*
    };
}

impl_signed_histogram_constant!(i8, i16, i32, i64);
impl_unsigned_histogram_constant!(u8, u16, u32, u64);

impl HistogramConstant for F64 {
    fn histogram_f64(&self) -> Option<F64> {
        Some(*self)
    }
}

impl HistogramConstant for databend_common_expression::types::F32 {
    fn histogram_f64(&self) -> Option<F64> {
        Some(F64::from(self.into_inner() as f64))
    }
}

impl HistogramConstant for String {
    fn histogram_bytes(&self) -> Option<&[u8]> {
        Some(self.as_bytes())
    }
}

impl HistogramConstant for bool {}

impl HistogramConstant for timestamp_tz {}

impl HistogramConstant for months_days_micros {}

fn unexpected_histogram_constant<T: std::fmt::Debug>(
    histogram_type: &'static str,
    constant: &T,
) -> String {
    format!("unexpected {histogram_type} histogram comparison constant: {constant:?}")
}

fn register_string_cmp(registry: &mut FunctionRegistry) {
    register_stat_string_comparison_2_arg(
        registry,
        "eq",
        |stat, _| derive_equality_stat::<StringType>(false, stat),
        |_, d1, d2| d1.domain_eq(d2),
        vectorize_string_cmp(|cmp| cmp == Ordering::Equal),
    );
    register_stat_string_comparison_2_arg(
        registry,
        "noteq",
        |stat, _| derive_equality_stat::<StringType>(true, stat),
        |_, d1, d2| d1.domain_noteq(d2),
        vectorize_string_cmp(|cmp| cmp != Ordering::Equal),
    );
    register_stat_string_comparison_2_arg(
        registry,
        "gt",
        |stat, _| derive_comparison_stat::<StringType, GtOp>(stat),
        |_, d1, d2| d1.domain_gt(d2),
        vectorize_string_cmp(|cmp| cmp == Ordering::Greater),
    );
    register_stat_string_comparison_2_arg(
        registry,
        "gte",
        |stat, _| derive_comparison_stat::<StringType, GteOp>(stat),
        |_, d1, d2| d1.domain_gte(d2),
        vectorize_string_cmp(|cmp| cmp != Ordering::Less),
    );
    register_stat_string_comparison_2_arg(
        registry,
        "lt",
        |stat, _| derive_comparison_stat::<StringType, LtOp>(stat),
        |_, d1, d2| d1.domain_lt(d2),
        vectorize_string_cmp(|cmp| cmp == Ordering::Less),
    );
    register_stat_string_comparison_2_arg(
        registry,
        "lte",
        |stat, _| derive_comparison_stat::<StringType, LteOp>(stat),
        |_, d1, d2| d1.domain_lte(d2),
        vectorize_string_cmp(|cmp| cmp != Ordering::Greater),
    );
}

fn register_stat_string_comparison_2_arg(
    registry: &mut FunctionRegistry,
    name: &'static str,
    derive_stat: fn(
        StatBinaryArg,
        &databend_common_expression::FunctionContext,
    ) -> Result<Option<ReturnStat>, String>,
    calc_domain: fn(
        &databend_common_expression::FunctionContext,
        &StringDomain,
        &StringDomain,
    ) -> FunctionDomain<BooleanType>,
    func: impl Fn(Value<StringType>, Value<StringType>, &mut EvalContext) -> Value<BooleanType>
    + Copy
    + Send
    + Sync
    + 'static,
) {
    registry
        .scalar_builder(name)
        .function()
        .typed_2_arg::<StringType, StringType, BooleanType>()
        .passthrough_nullable()
        .calc_domain(calc_domain)
        .derive_stat(derive_stat)
        .vectorized(func)
        .register();
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
    register_stat_comparison_2_arg::<BooleanType>(
        registry,
        "eq",
        |stat, _| derive_equality_stat::<BooleanType>(false, stat),
        |_, d1, d2| match (d1.has_true, d1.has_false, d2.has_true, d2.has_false) {
            (true, false, true, false) => FunctionDomain::Domain(ALL_TRUE_DOMAIN),
            (false, true, false, true) => FunctionDomain::Domain(ALL_TRUE_DOMAIN),
            (true, false, false, true) => FunctionDomain::Domain(ALL_FALSE_DOMAIN),
            (false, true, true, false) => FunctionDomain::Domain(ALL_FALSE_DOMAIN),
            _ => FunctionDomain::Full,
        },
        |lhs, rhs, _| lhs == rhs,
    );
    register_stat_comparison_2_arg::<BooleanType>(
        registry,
        "noteq",
        |stat, _| derive_equality_stat::<BooleanType>(true, stat),
        |_, d1, d2| match (d1.has_true, d1.has_false, d2.has_true, d2.has_false) {
            (true, false, true, false) => FunctionDomain::Domain(ALL_FALSE_DOMAIN),
            (false, true, false, true) => FunctionDomain::Domain(ALL_FALSE_DOMAIN),
            (true, false, false, true) => FunctionDomain::Domain(ALL_TRUE_DOMAIN),
            (false, true, true, false) => FunctionDomain::Domain(ALL_TRUE_DOMAIN),
            _ => FunctionDomain::Full,
        },
        |lhs, rhs, _| lhs != rhs,
    );
    register_stat_comparison_2_arg::<BooleanType>(
        registry,
        "gt",
        |stat, _| derive_comparison_stat::<BooleanType, GtOp>(stat),
        |_, d1, d2| match (d1.has_true, d1.has_false, d2.has_true, d2.has_false) {
            (true, false, false, true) => FunctionDomain::Domain(ALL_TRUE_DOMAIN),
            (false, true, _, _) => FunctionDomain::Domain(ALL_FALSE_DOMAIN),
            _ => FunctionDomain::Full,
        },
        |lhs, rhs, _| lhs & !rhs,
    );
    register_stat_comparison_2_arg::<BooleanType>(
        registry,
        "gte",
        |stat, _| derive_comparison_stat::<BooleanType, GteOp>(stat),
        |_, d1, d2| match (d1.has_true, d1.has_false, d2.has_true, d2.has_false) {
            (true, false, _, _) => FunctionDomain::Domain(ALL_TRUE_DOMAIN),
            (_, _, false, true) => FunctionDomain::Domain(ALL_TRUE_DOMAIN),
            (false, true, true, false) => FunctionDomain::Domain(ALL_FALSE_DOMAIN),
            _ => FunctionDomain::Full,
        },
        |lhs, rhs, _| lhs | !rhs,
    );
    register_stat_comparison_2_arg::<BooleanType>(
        registry,
        "lt",
        |stat, _| derive_comparison_stat::<BooleanType, LtOp>(stat),
        |_, d1, d2| match (d1.has_true, d1.has_false, d2.has_true, d2.has_false) {
            (false, true, true, false) => FunctionDomain::Domain(ALL_TRUE_DOMAIN),
            (_, _, false, true) => FunctionDomain::Domain(ALL_FALSE_DOMAIN),
            _ => FunctionDomain::Full,
        },
        |lhs, rhs, _| !lhs & rhs,
    );
    register_stat_comparison_2_arg::<BooleanType>(
        registry,
        "lte",
        |stat, _| derive_comparison_stat::<BooleanType, LteOp>(stat),
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

    registry.register_passthrough_nullable_2_arg::<StringType, StringType, BooleanType, _, _>(
        "ilike",
        |_, _, _| FunctionDomain::Full,
        |arg1, arg2, ctx| {
            vectorize_ilike(|str, pattern_type| pattern_type.compare(str))(
                arg1,
                arg2,
                Value::Scalar("".to_string()),
                ctx,
            )
        },
    );

    registry.register_passthrough_nullable_3_arg::<StringType, StringType, StringType, BooleanType, _, _>(
        "ilike",
        |_, _, _, _| FunctionDomain::Full,
       |arg1, arg2, arg3, ctx| {
           vectorize_ilike(|str, pattern_type| pattern_type.compare(str))(
               arg1,
               arg2,
               arg3,
               ctx,
           )
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

    registry.register_function_factory(
        "ilike_any",
        FunctionFactory::Closure(Box::new(|_, args_type: &[DataType]| {
            if args_type.len() < 2 || args_type.len() > 3 {
                return None;
            }
            let is_nullable = args_type[0].is_nullable();
            let arg_type = args_type[0].remove_nullable();
            if !arg_type.is_string() {
                return None;
            }
            let mut new_args_type = match &args_type[1] {
                DataType::Tuple(patterns_ty) => {
                    if patterns_ty.iter().any(|ty| !ty.is_string()) {
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
                name: "ilike_any".to_string(),
                args_type: new_args_type,
                return_type: DataType::Boolean,
            };
            Some(Arc::new(Function::with_passthrough_nullable(
                signature,
                FunctionDomain::Full,
                ilike_any_fn,
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
    let is_all_percent_pattern = !pattern.is_empty() && pattern.bytes().all(|c| c == b'%');
    match generate_like_pattern(pattern.as_bytes(), 1) {
        LikePattern::StartOfPercent(v) if v.is_empty() => {
            Some(FunctionDomain::Domain(ALL_TRUE_DOMAIN))
        }
        LikePattern::Constant(true) if is_all_percent_pattern => {
            Some(FunctionDomain::Domain(ALL_TRUE_DOMAIN))
        }
        LikePattern::OrdinalStr(_) => Some(lhs.domain_eq(&StringDomain {
            min: pattern.clone(),
            max: Some(pattern),
        })),
        LikePattern::EndOfPercent(v) => {
            let pat_str = std::str::from_utf8(v.as_ref()).ok()?.to_string();
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

fn vectorize_ilike(
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
                let arg1 = arg1.to_lowercase();
                let pattern = convert_escape_pattern(&escape, arg2).to_lowercase();
                let pattern_type = generate_like_pattern(pattern.as_bytes(), 1);
                Value::Scalar(func(arg1.as_bytes(), &pattern_type))
            }
            (Value::Column(arg1), Value::Scalar(arg2)) => {
                let arg1_iter = StringType::iter_column(&arg1);
                let mut builder = MutableBitmap::with_capacity(arg1.len());
                let pattern = convert_escape_pattern(&escape, arg2).to_lowercase();
                let pattern_type =
                    generate_like_pattern(pattern.as_bytes(), arg1.total_bytes_len());
                if let LikePattern::SurroundByPercent(searcher) = pattern_type {
                    for arg1 in arg1_iter {
                        let arg1 = arg1.to_lowercase();
                        builder.push(searcher.search(arg1.as_bytes()).is_some());
                    }
                } else {
                    for arg1 in arg1_iter {
                        let arg1 = arg1.to_lowercase();
                        builder.push(func(arg1.as_bytes(), &pattern_type));
                    }
                }
                Value::Column(builder.into())
            }
            (Value::Scalar(arg1), Value::Column(arg2)) => {
                let arg2_iter = StringType::iter_column(&arg2);
                let arg1 = arg1.to_lowercase();
                let mut builder = MutableBitmap::with_capacity(arg2.len());
                for arg2 in arg2_iter {
                    let pattern = convert_escape_pattern(&escape, arg2.to_string()).to_lowercase();
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
                    let arg1 = arg1.to_lowercase();
                    let pattern = convert_escape_pattern(&escape, arg2.to_string()).to_lowercase();
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

fn ilike_any_fn(args: &[Value<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
    let arg = &args[0];
    let input_all_scalars = arg.as_scalar().is_some();
    let process_rows = if input_all_scalars { 1 } else { ctx.num_rows };

    let patterns = match args[1].as_scalar() {
        Some(Scalar::Tuple(patterns)) => patterns.clone(),
        Some(Scalar::String(pattern)) => vec![Scalar::String(pattern.clone())],
        _ => {
            ctx.set_error(
                1,
                "The second parameter of `ilike_any` must be of Tuple or String type",
            );
            return Value::Scalar(Scalar::Boolean(Default::default()));
        }
    };

    let escape: Value<StringType> = args
        .get(2)
        .cloned()
        .and_then(|value| value.try_downcast().ok())
        .unwrap_or(Value::Scalar("".to_string()));

    let Ok(value) = arg.try_downcast::<StringType>() else {
        ctx.set_error(
            1,
            "The first parameter of 'ilike_any' can only be of type String",
        );
        return Value::Scalar(Scalar::Boolean(Default::default()));
    };

    let ilike = vectorize_ilike(|str, pattern_type| pattern_type.compare(str));
    let result = patterns
        .iter()
        .map(|pattern| {
            ilike(
                value.clone(),
                Value::<StringType>::Scalar(pattern.as_string().unwrap().clone()),
                escape.clone(),
                ctx,
            )
        })
        .collect::<Vec<_>>();

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

#[cfg(test)]
mod tests {
    use databend_common_expression::FunctionContext;
    use databend_common_expression::stat_distribution::BorrowedDistribution;
    use databend_common_expression::stat_distribution::NdvEstimate;
    use databend_common_expression::stat_distribution::StatCardinality;
    use databend_common_expression::stat_distribution::StatCount;
    use databend_common_expression::stat_distribution::StatEstimate;
    use databend_common_expression::types::Int64Type;
    use databend_common_expression::types::NumberDomain;
    use databend_common_expression::types::SimpleDomain;
    use databend_common_expression::types::nullable::NullableDomain;
    use databend_common_expression::types::string::StringDomain;
    use jsonb::OwnedJsonb;

    use super::*;

    #[test]
    fn test_numeric_histogram_partial_bucket_reserves_equality_mass() {
        let bucket = TypedHistogramBucket::new(F64::from(0.0), F64::from(10.0), 10.0, 2.0);
        let constant = F64::from(9.0);
        let comparison = HistogramBucketComparison::<_, GtOp> {
            bucket: &bucket,
            constant: &constant,
            row_scale: 1.0,
            _op: PhantomData,
        };

        let selected_count = comparison.number_selected_count();
        assert_eq!(selected_count.lower, 0.0);
        assert!((selected_count.expected - 0.5).abs() < 1e-12);
        assert_eq!(selected_count.upper, 10.0);
    }

    #[test]
    fn test_null_constant_comparison_returns_all_null_stat() {
        let column_stat = ArgStat {
            domain: Domain::Number(NumberDomain::Int64(SimpleDomain { min: 1, max: 10 })),
            ndv: NdvEstimate::exact(10.0),
            null_count: StatCount::exact(0),
            distribution: BorrowedDistribution::Unknown,
        };
        let constant_stat = ArgStat {
            domain: Domain::Nullable(NullableDomain {
                has_null: true,
                value: None,
            }),
            ndv: NdvEstimate::exact(0.0),
            null_count: StatCount::exact(10),
            distribution: BorrowedDistribution::Unknown,
        };
        let args = [column_stat, constant_stat];
        let stat = StatBinaryArg {
            cardinality: StatCardinality::exact(10),
            args: &args,
        };

        let output = derive_comparison_stat::<Int64Type, GtOp>(stat)
            .unwrap()
            .unwrap();

        assert_eq!(output.null_count, StatCount::exact(10));
        assert!(matches!(
            output.domain,
            Domain::Nullable(NullableDomain {
                has_null: true,
                value: None
            })
        ));
        assert!(output.boolean_distribution().is_none());
    }

    #[test]
    fn test_unbounded_string_equality_uses_ndv_estimate() {
        let column_stat = ArgStat {
            domain: Domain::String(StringDomain {
                min: "".to_string(),
                max: None,
            }),
            ndv: NdvEstimate::exact(10.0),
            null_count: StatCount::exact(0),
            distribution: BorrowedDistribution::Unknown,
        };
        let constant_stat = ArgStat {
            domain: Domain::String(StringDomain {
                min: "x".to_string(),
                max: Some("x".to_string()),
            }),
            ndv: NdvEstimate::exact(1.0),
            null_count: StatCount::exact(0),
            distribution: BorrowedDistribution::Unknown,
        };
        let args = [column_stat, constant_stat];
        let stat = StatBinaryArg {
            cardinality: StatCardinality::exact(100),
            args: &args,
        };

        let output = derive_equality_stat::<StringType>(false, stat)
            .unwrap()
            .unwrap();
        let true_count = output.boolean_distribution().unwrap().true_count;

        assert_eq!(true_count, StatEstimate::new(0.0, 10.0, 100.0));
    }

    #[test]
    fn test_calc_like_domain_repeated_trailing_percent_matches_normalized_prefix() {
        let matching = StringDomain {
            min: "ababac".to_string(),
            max: Some("ababac".to_string()),
        };
        let non_matching = StringDomain {
            min: "aba".to_string(),
            max: Some("aba".to_string()),
        };

        assert_eq!(
            calc_like_domain(&matching, "abab%".to_string()),
            calc_like_domain(&matching, "abab%%%%%".to_string()),
            "repeated trailing % should fold like a single trailing %"
        );
        assert_eq!(
            calc_like_domain(&non_matching, "abab%%%%%".to_string()),
            calc_like_domain(&non_matching, "abab%".to_string()),
            "non-matching prefixes should also stay consistent"
        );
    }

    #[test]
    fn test_calc_like_domain_all_percent_matches_single_percent() {
        let domain = StringDomain {
            min: "".to_string(),
            max: Some("zzz".to_string()),
        };

        assert_eq!(
            calc_like_domain(&domain, "%".to_string()),
            calc_like_domain(&domain, "%%%%%".to_string()),
            "repeated all-% patterns should fold like a single %"
        );
    }

    #[test]
    fn test_calc_like_domain_empty_pattern_does_not_fold_to_all_true() {
        let domain = StringDomain {
            min: "".to_string(),
            max: Some("zzz".to_string()),
        };

        assert_eq!(
            calc_like_domain(&domain, "".to_string()),
            None,
            "empty patterns should not reuse repeated all-% folding"
        );
    }

    #[test]
    fn test_variant_like_repeated_percent_preserves_simple_scalar_semantics() {
        let like = variant_vectorize_like_jsonb();
        let value = Value::<VariantType>::Scalar(
            r#"{"name":"abab"}"#.parse::<OwnedJsonb>().unwrap().to_vec(),
        );
        let escape = Value::<StringType>::Scalar("".to_string());
        let func_ctx = FunctionContext::default();
        let mut ctx = EvalContext {
            generics: &[],
            num_rows: 1,
            func_ctx: &func_ctx,
            validity: None,
            errors: None,
            suppress_error: false,
            strict_eval: false,
        };

        let leading = like(
            value.clone(),
            Value::<StringType>::Scalar("%abab".to_string()),
            escape.clone(),
            &mut ctx,
        );
        let repeated_leading = like(
            value.clone(),
            Value::<StringType>::Scalar("%%%%abab".to_string()),
            escape.clone(),
            &mut ctx,
        );
        let trailing = like(
            value.clone(),
            Value::<StringType>::Scalar("abab%".to_string()),
            escape.clone(),
            &mut ctx,
        );
        let repeated_trailing = like(
            value,
            Value::<StringType>::Scalar("abab%%%%".to_string()),
            escape,
            &mut ctx,
        );

        assert!(matches!(leading, Value::Scalar(false)));
        assert_eq!(leading, repeated_leading);
        assert!(matches!(trailing, Value::Scalar(false)));
        assert_eq!(trailing, repeated_trailing);
    }
}
