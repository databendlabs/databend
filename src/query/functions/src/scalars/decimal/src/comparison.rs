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
use std::hint::unlikely;
use std::marker::PhantomData;
use std::ops::*;
use std::sync::Arc;

use databend_common_expression::Domain;
use databend_common_expression::EvalContext;
use databend_common_expression::Function;
use databend_common_expression::FunctionContext;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionFactory;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarFunction;
use databend_common_expression::ScalarFunctionDomain;
use databend_common_expression::SimpleDomainCmp;
use databend_common_expression::Value;
use databend_common_expression::comparison::ConstantComparison;
use databend_common_expression::comparison::ConstantComparisonAdapter;
use databend_common_expression::comparison::GtOp;
use databend_common_expression::comparison::GteOp;
use databend_common_expression::comparison::LtOp;
use databend_common_expression::comparison::LteOp;
use databend_common_expression::comparison::StatComparisonOp;
use databend_common_expression::comparison::null_comparison_stat;
use databend_common_expression::function_stat::ReturnStat;
use databend_common_expression::function_stat::ScalarFunctionStat;
use databend_common_expression::function_stat::StatArgs;
use databend_common_expression::stat_distribution::StatBinaryArg;
use databend_common_expression::types::compute_view::ComputeView;
use databend_common_expression::types::decimal::*;
use databend_common_expression::types::i256;
use databend_common_expression::types::*;
use databend_common_expression::vectorize_cmp_2_arg;
use databend_common_expression::with_decimal_mapped_type;

#[inline]
fn compare_multiplier(scale_a: u8, scale_b: u8) -> (u8, u8) {
    (
        (scale_b - std::cmp::min(scale_a, scale_b)),
        (scale_a - std::cmp::min(scale_a, scale_b)),
    )
}

fn register_decimal_compare_op<Op: CmpOp>(
    registry: &mut FunctionRegistry,
    derive_stat: impl ScalarFunctionStat + Clone + 'static,
) {
    let factory = FunctionFactory::Closure(Box::new(move |_, args_type: &[DataType]| {
        if args_type.len() != 2 {
            return None;
        }

        let has_nullable = args_type.iter().any(|x| x.is_nullable_or_null());
        let args_type: Vec<DataType> = args_type.iter().map(|x| x.remove_nullable()).collect();

        // Only works for one of is decimal types
        if !args_type[0].is_decimal() && !args_type[1].is_decimal() {
            return None;
        }

        let sig_types = vec![
            DataType::Decimal(args_type[0].get_decimal_properties()?),
            DataType::Decimal(args_type[1].get_decimal_properties()?),
        ];

        // Comparison between different decimal types must be same siganature types
        let signature = FunctionSignature {
            name: Op::NAME.to_string(),
            args_type: sig_types,
            return_type: DataType::Boolean,
        };

        let eval = DecimalCmp::<Op>::default();
        Some(Arc::new(Function::with_passthrough_nullable(
            signature,
            DecimalComparisonDomain::<Op>::default(),
            eval,
            Some(Box::new(derive_stat.clone())),
            has_nullable,
        )))
    }));
    registry.register_function_factory(Op::NAME, factory);
}

#[derive(Default, Clone)]
struct DecimalEqualityStat<Op> {
    _op: PhantomData<Op>,
}

impl<Op: EqualityOp + Sync + Send> ScalarFunctionStat for DecimalEqualityStat<Op> {
    fn stat_eval(
        &self,
        _: &FunctionContext,
        args: StatArgs<'_>,
    ) -> Result<Option<ReturnStat>, String> {
        let stat = StatBinaryArg {
            cardinality: args.cardinality,
            args: args.args.as_array().unwrap(),
        };
        derive_decimal_equality_stat::<Op>(stat)
    }
}

#[derive(Default, Clone)]
struct DecimalRangeStat<Op> {
    _op: PhantomData<Op>,
}

impl<Op: StatComparisonOp + CmpOp + Send + Sync> ScalarFunctionStat for DecimalRangeStat<Op> {
    fn stat_eval(
        &self,
        _: &FunctionContext,
        args: StatArgs<'_>,
    ) -> Result<Option<ReturnStat>, String> {
        let stat = StatBinaryArg {
            cardinality: args.cardinality,
            args: args.args.as_array().unwrap(),
        };
        derive_decimal_range_stat::<Op>(stat)
    }
}

fn derive_decimal_equality_stat<Op: EqualityOp>(
    stat: StatBinaryArg,
) -> Result<Option<ReturnStat>, String> {
    if let Some(stat) = null_comparison_stat(&stat) {
        return Ok(Some(stat));
    }

    let Some((input, _)) = ConstantComparison::<DecimalComparisonStat>::from_args(&stat)? else {
        return Ok(None);
    };

    let minmax_cmp = input.domain.as_ref().map(decimal_minmax).map(|(min, max)| {
        (
            DecimalComparisonStat::compare(&input.constant, &min),
            DecimalComparisonStat::compare(&input.constant, &max),
        )
    });
    let true_count = input.equality_true_count(minmax_cmp, Op::NOT_EQ);
    Ok(Some(input.boolean_stat(true_count)))
}

fn derive_decimal_range_stat<Op: StatComparisonOp>(
    stat: StatBinaryArg,
) -> Result<Option<ReturnStat>, String> {
    if let Some(stat) = null_comparison_stat(&stat) {
        return Ok(Some(stat));
    }

    let Some((input, reverse)) = ConstantComparison::<DecimalComparisonStat>::from_args(&stat)?
    else {
        return Ok(None);
    };

    let Some((min, max)) = input.domain.as_ref().map(decimal_minmax) else {
        return Ok(None);
    };
    let cmp_min = DecimalComparisonStat::compare(&input.constant, &min);
    let cmp_max = DecimalComparisonStat::compare(&input.constant, &max);
    Ok(if reverse {
        Op::Reverse::range_true_count(input.stat.ndv, input.non_null_cardinality, cmp_min, cmp_max)
    } else {
        Op::range_true_count(input.stat.ndv, input.non_null_cardinality, cmp_min, cmp_max)
    }
    .map(|true_count| input.boolean_stat(true_count)))
}

#[derive(Clone, Copy)]
struct DecimalComparisonStat;

impl ConstantComparisonAdapter for DecimalComparisonStat {
    // TODO: Decimal statistics still inherit the existing Datum/ColumnStat
    // limitation of representing decimal bounds as f64. Preserve the current
    // behavior for now, but compare scaled decimal integers here once the
    // statistics path can carry exact decimal bounds end-to-end.
    type Value = F64;
    type Domain = DecimalDomain;

    fn constant(scalar: Scalar) -> Result<F64, String> {
        let Scalar::Decimal(value) = scalar else {
            return Err("decimal comparison constant downcast failed".to_string());
        };
        Ok(F64::from(value.to_float64()))
    }

    fn domain(domain: &Domain) -> Result<DecimalDomain, String> {
        domain
            .as_decimal()
            .cloned()
            .ok_or_else(|| "decimal comparison domain downcast failed".to_string())
    }

    fn compare(left: &F64, right: &F64) -> Ordering {
        left.cmp(right)
    }
}

fn decimal_minmax(domain: &DecimalDomain) -> (F64, F64) {
    match domain {
        DecimalDomain::Decimal64(domain, size) => (
            F64::from(domain.min.to_float64(size.scale())),
            F64::from(domain.max.to_float64(size.scale())),
        ),
        DecimalDomain::Decimal128(domain, size) => (
            F64::from(domain.min.to_float64(size.scale())),
            F64::from(domain.max.to_float64(size.scale())),
        ),
        DecimalDomain::Decimal256(domain, size) => (
            F64::from(domain.min.to_float64(size.scale())),
            F64::from(domain.max.to_float64(size.scale())),
        ),
    }
}

#[derive(Clone, Copy, Default)]
struct DecimalComparisonDomain<Op> {
    _op: PhantomData<fn(Op)>,
}

impl<Op: CmpOp> ScalarFunctionDomain for DecimalComparisonDomain<Op> {
    fn domain_eval(&self, _: &FunctionContext, domains: &[Domain]) -> FunctionDomain<AnyType> {
        let d1 = domains[0].as_decimal().unwrap();
        let d2 = domains[1].as_decimal().unwrap();

        let (s1, s2) = (d1.decimal_size().scale(), d2.decimal_size().scale());
        let (m1, m2) = compare_multiplier(s1, s2);

        let (min1, max1) = match d1 {
            DecimalDomain::Decimal64(domain, _) => (domain.min.into(), domain.max.into()),
            DecimalDomain::Decimal128(domain, _) => (domain.min.into(), domain.max.into()),
            DecimalDomain::Decimal256(domain, _) => (domain.min, domain.max),
        };

        let (min2, max2) = match d2 {
            DecimalDomain::Decimal64(domain, _) => (domain.min.into(), domain.max.into()),
            DecimalDomain::Decimal128(domain, _) => (domain.min.into(), domain.max.into()),
            DecimalDomain::Decimal256(domain, _) => (domain.min, domain.max),
        };

        let d1 = SimpleDomain {
            min: min1.checked_mul(i256::e(m1)).unwrap_or(i256::DECIMAL_MIN),
            max: max1.checked_mul(i256::e(m1)).unwrap_or(i256::DECIMAL_MAX),
        };

        let d2 = SimpleDomain {
            min: min2.checked_mul(i256::e(m2)).unwrap_or(i256::DECIMAL_MIN),
            max: max2.checked_mul(i256::e(m2)).unwrap_or(i256::DECIMAL_MAX),
        };

        let new_domain = Op::domain_op(&d1, &d2);
        new_domain.map(Domain::Boolean)
    }
}

#[derive(Clone, Copy, Default)]
struct DecimalCmp<Op> {
    _op: PhantomData<fn(Op)>,
}

impl<Op: CmpOp> ScalarFunction for DecimalCmp<Op> {
    fn eval(&self, args: &[Value<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
        let a = &args[0];
        let b = &args[1];
        let (a_type, _) = DecimalDataType::from_value(a).unwrap();
        let (b_type, _) = DecimalDataType::from_value(b).unwrap();
        let size_calc = calc_size(&a_type.size(), &b_type.size());

        with_decimal_mapped_type!(|T| match DecimalDataType::from(size_calc) {
            DecimalDataType::T(_) => {
                with_decimal_mapped_type!(|A| match a_type {
                    DecimalDataType::A(_) => {
                        with_decimal_mapped_type!(|B| match b_type {
                            DecimalDataType::B(_) => {
                                let a = a
                                    .try_downcast::<ComputeView<DecimalConvert<A, T>, _, _>>()
                                    .unwrap();
                                let b = b
                                    .try_downcast::<ComputeView<DecimalConvert<B, T>, _, _>>()
                                    .unwrap();
                                let (f_a, f_b) = (
                                    T::e(size_calc.scale() - a_type.scale()),
                                    T::e(size_calc.scale() - b_type.scale()),
                                );

                                if (f_a == f_b) {
                                    compare_decimal(a, b, |a, b, _| Op::is(a.cmp(&b)), ctx)
                                } else {
                                    compare_decimal(
                                        a,
                                        b,
                                        |a, b, _| Op::compare(a, b, f_a, f_b),
                                        ctx,
                                    )
                                }
                            }
                        })
                    }
                })
            }
        })
    }
}

fn calc_size(a: &DecimalSize, b: &DecimalSize) -> DecimalSize {
    let scale = a.scale().max(b.scale());
    let precision = a.leading_digits().max(b.leading_digits()) + scale;

    // if the args both are Decimal128, we need to clamp the precision to 38
    let precision = if a.precision() <= i128::MAX_PRECISION && b.precision() <= i128::MAX_PRECISION
    {
        precision.min(i128::MAX_PRECISION)
    } else {
        precision.min(i256::MAX_PRECISION)
    };

    DecimalSize::new(precision, scale).unwrap()
}

fn compare_decimal<A, B, F, T>(
    a: Value<A>,
    b: Value<B>,
    f: F,
    ctx: &mut EvalContext,
) -> Value<AnyType>
where
    T: Decimal,
    A: for<'a> AccessType<ScalarRef<'a> = T>,
    B: for<'a> AccessType<ScalarRef<'a> = T>,
    F: Fn(T, T, &mut EvalContext) -> bool + Copy + Send + Sync,
{
    let value = vectorize_cmp_2_arg::<A, B>(f)(a, b, ctx);
    value.upcast()
}

trait CmpOp: 'static + Default + Send + Sync + Clone {
    const NAME: &str;

    fn is(o: Ordering) -> bool;
    fn domain_op<T: SimpleDomainCmp>(a: &T, b: &T) -> FunctionDomain<BooleanType>;
    fn compare<D>(a: D, b: D, f_a: D, f_b: D) -> bool
    where D: Decimal + std::ops::Mul<Output = D> {
        if unlikely(a.signum() != b.signum()) {
            return Self::is(a.cmp(&b));
        }

        let a = if f_a == D::one() {
            a
        } else if let Some(a) = a.checked_mul(f_a) {
            a
        } else {
            return if a.signum() > D::zero() {
                Self::is(Ordering::Greater)
            } else {
                Self::is(Ordering::Less)
            };
        };
        let b = if f_b == D::one() {
            b
        } else if let Some(b) = b.checked_mul(f_b) {
            b
        } else {
            return if b.signum() > D::zero() {
                Self::is(Ordering::Less)
            } else {
                Self::is(Ordering::Greater)
            };
        };
        Self::is(a.cmp(&b))
    }
}

trait EqualityOp: CmpOp + Send + Sync + Clone {
    const NOT_EQ: bool;
}

macro_rules! define_cmp_op {
    ($name:ident, $func_name:expr, $is_fn:ident, $domain_op:ident, $not_eq:expr) => {
        #[derive(Default, Clone, Copy)]
        struct $name;

        impl CmpOp for $name {
            const NAME: &str = $func_name;

            fn is(o: Ordering) -> bool {
                o.$is_fn()
            }

            fn domain_op<T: SimpleDomainCmp>(a: &T, b: &T) -> FunctionDomain<BooleanType> {
                a.$domain_op(b)
            }
        }

        impl EqualityOp for $name {
            const NOT_EQ: bool = $not_eq;
        }
    };
}

macro_rules! impl_range_cmp_op {
    ($op:ty, $func_name:expr, $is_fn:ident, $domain_op:ident) => {
        impl CmpOp for $op {
            const NAME: &str = $func_name;

            fn is(o: Ordering) -> bool {
                o.$is_fn()
            }

            fn domain_op<T: SimpleDomainCmp>(a: &T, b: &T) -> FunctionDomain<BooleanType> {
                a.$domain_op(b)
            }
        }
    };
}

define_cmp_op!(Equal, "eq", is_eq, domain_eq, false);
define_cmp_op!(NotEqual, "noteq", is_ne, domain_noteq, true);
impl_range_cmp_op!(LtOp, "lt", is_lt, domain_lt);
impl_range_cmp_op!(GtOp, "gt", is_gt, domain_gt);
impl_range_cmp_op!(LteOp, "lte", is_le, domain_lte);
impl_range_cmp_op!(GteOp, "gte", is_ge, domain_gte);

pub fn register_decimal_compare(registry: &mut FunctionRegistry) {
    register_decimal_compare_op::<Equal>(registry, DecimalEqualityStat::<Equal>::default());
    register_decimal_compare_op::<NotEqual>(registry, DecimalEqualityStat::<NotEqual>::default());

    register_decimal_compare_op::<LtOp>(registry, DecimalRangeStat::<LtOp>::default());
    register_decimal_compare_op::<GtOp>(registry, DecimalRangeStat::<GtOp>::default());
    register_decimal_compare_op::<LteOp>(registry, DecimalRangeStat::<LteOp>::default());
    register_decimal_compare_op::<GteOp>(registry, DecimalRangeStat::<GteOp>::default());
}
