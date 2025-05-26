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
use std::ops::*;
use std::sync::Arc;

use databend_common_expression::types::decimal::*;
use databend_common_expression::types::i256;
use databend_common_expression::types::*;
use databend_common_expression::vectorize_cmp_2_arg;
use databend_common_expression::Domain;
use databend_common_expression::EvalContext;
use databend_common_expression::Function;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionFactory;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::SimpleDomainCmp;
use databend_common_expression::Value;

use super::convert_to_decimal_domain;

#[inline]
fn compare_multiplier(scale_a: u8, scale_b: u8) -> (u32, u32) {
    (
        (scale_b - std::cmp::min(scale_a, scale_b)) as u32,
        (scale_a - std::cmp::min(scale_a, scale_b)) as u32,
    )
}

fn register_decimal_compare_op<Op: CmpOp>(registry: &mut FunctionRegistry) {
    let factory = FunctionFactory::Closure(Box::new(|_, args_type: &[DataType]| {
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
        let function = Function {
            signature: FunctionSignature {
                name: Op::NAME.to_string(),
                args_type: sig_types.clone(),
                return_type: DataType::Boolean,
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|ctx, d| {
                    let (s1, s2) = (
                        d[0].as_decimal().unwrap().decimal_size().scale(),
                        d[1].as_decimal().unwrap().decimal_size().scale(),
                    );
                    let (m1, m2) = compare_multiplier(s1, s2);
                    let new_domain = match (&d[0], &d[1]) {
                        (
                            Domain::Decimal(DecimalDomain::Decimal128(d1, _)),
                            Domain::Decimal(DecimalDomain::Decimal128(d2, _)),
                        ) => {
                            let d1 = SimpleDomain {
                                min: d1.min.checked_mul(10_i128.pow(m1)).unwrap_or(i128::MIN),
                                max: d1.max.checked_mul(10_i128.pow(m1)).unwrap_or(i128::MAX),
                            };
                            let d2 = SimpleDomain {
                                min: d2.min.checked_mul(10_i128.pow(m2)).unwrap_or(i128::MIN),
                                max: d2.max.checked_mul(10_i128.pow(m2)).unwrap_or(i128::MAX),
                            };
                            Op::domain_op(&d1, &d2)
                        }
                        (
                            Domain::Decimal(DecimalDomain::Decimal256(d1, _)),
                            Domain::Decimal(DecimalDomain::Decimal256(d2, _)),
                        ) => {
                            let d1 = SimpleDomain {
                                min: d1
                                    .min
                                    .checked_mul(i256::from(10).pow(m1))
                                    .unwrap_or(i256::MIN),
                                max: d1
                                    .max
                                    .checked_mul(i256::from(10).pow(m1))
                                    .unwrap_or(i256::MAX),
                            };

                            let d2 = SimpleDomain {
                                min: d2
                                    .min
                                    .checked_mul(i256::from(10).pow(m2))
                                    .unwrap_or(i256::MIN),
                                max: d2
                                    .max
                                    .checked_mul(i256::from(10).pow(m2))
                                    .unwrap_or(i256::MAX),
                            };
                            Op::domain_op(&d1, &d2)
                        }
                        (
                            Domain::Decimal(DecimalDomain::Decimal128(_, _)),
                            Domain::Decimal(DecimalDomain::Decimal256(d2, _)),
                        ) => {
                            let d1 = convert_to_decimal_domain(
                                ctx,
                                d[0].clone(),
                                DecimalDataType::Decimal256(DecimalSize::new_unchecked(
                                    MAX_DECIMAL256_PRECISION,
                                    s1,
                                )),
                            )
                            .unwrap();

                            let d1 = d1.as_decimal256().unwrap().0;
                            let d1 = SimpleDomain {
                                min: d1
                                    .min
                                    .checked_mul(i256::from(10).pow(m1))
                                    .unwrap_or(i256::MIN),
                                max: d1
                                    .max
                                    .checked_mul(i256::from(10).pow(m1))
                                    .unwrap_or(i256::MAX),
                            };

                            let d2 = SimpleDomain {
                                min: d2
                                    .min
                                    .checked_mul(i256::from(10).pow(m2))
                                    .unwrap_or(i256::MIN),
                                max: d2
                                    .max
                                    .checked_mul(i256::from(10).pow(m2))
                                    .unwrap_or(i256::MAX),
                            };
                            Op::domain_op(&d1, &d2)
                        }
                        (
                            Domain::Decimal(DecimalDomain::Decimal256(d1, _)),
                            Domain::Decimal(DecimalDomain::Decimal128(_, _)),
                        ) => {
                            let d2 = convert_to_decimal_domain(
                                ctx,
                                d[1].clone(),
                                DecimalDataType::Decimal256(DecimalSize::new_unchecked(
                                    MAX_DECIMAL256_PRECISION,
                                    s2,
                                )),
                            )
                            .unwrap();
                            let d2 = d2.as_decimal256().unwrap().0;

                            let d1 = SimpleDomain {
                                min: d1
                                    .min
                                    .checked_mul(i256::from(10).pow(m1))
                                    .unwrap_or(i256::MIN),
                                max: d1
                                    .max
                                    .checked_mul(i256::from(10).pow(m1))
                                    .unwrap_or(i256::MAX),
                            };

                            let d2 = SimpleDomain {
                                min: d2
                                    .min
                                    .checked_mul(i256::from(10).pow(m2))
                                    .unwrap_or(i256::MIN),
                                max: d2
                                    .max
                                    .checked_mul(i256::from(10).pow(m2))
                                    .unwrap_or(i256::MAX),
                            };
                            Op::domain_op(&d1, &d2)
                        }
                        _ => unreachable!(),
                    };
                    new_domain.map(Domain::Boolean)
                }),
                eval: Box::new(move |args, ctx| {
                    op_decimal::<Op>(&args[0], &args[1], &sig_types, ctx)
                }),
            },
        };
        if has_nullable {
            Some(Arc::new(function.passthrough_nullable()))
        } else {
            Some(Arc::new(function))
        }
    }));
    registry.register_function_factory(Op::NAME, factory);
}

fn op_decimal<Op: CmpOp>(
    a: &Value<AnyType>,
    b: &Value<AnyType>,
    args_type: &[DataType],
    ctx: &mut EvalContext,
) -> Value<AnyType> {
    use DecimalDataType::*;

    let (size_a, size_b) = (
        args_type[0].as_decimal().unwrap(),
        args_type[1].as_decimal().unwrap(),
    );
    let (m_a, m_b) = compare_multiplier(size_a.scale(), size_b.scale());

    let (a_type, _) = DecimalDataType::from_value(a).unwrap();
    let (b_type, _) = DecimalDataType::from_value(b).unwrap();

    match (a_type, b_type) {
        (Decimal128(_), Decimal128(_)) => {
            let a = a.try_downcast::<Decimal128Type>().unwrap();
            let b = b.try_downcast::<Decimal128Type>().unwrap();
            let (f_a, f_b) = (10_i128.pow(m_a), 10_i128.pow(m_b));
            compare_decimal(a, b, |a, b, _| Op::compare(a, b, f_a, f_b), ctx)
        }
        (Decimal256(_), Decimal256(_)) => {
            let a = a.try_downcast::<Decimal256Type>().unwrap();
            let b = b.try_downcast::<Decimal256Type>().unwrap();
            let (f_a, f_b) = (i256::from(10).pow(m_a), i256::from(10).pow(m_b));
            compare_decimal(a, b, |a, b, _| Op::compare(a, b, f_a, f_b), ctx)
        }
        (Decimal128(_), Decimal256(_)) => {
            let a = a.try_downcast::<Decimal128As256Type>().unwrap();
            let b = b.try_downcast::<Decimal256Type>().unwrap();
            let (f_a, f_b) = (i256::from(10).pow(m_a), i256::from(10).pow(m_b));
            compare_decimal(a, b, |a, b, _| Op::compare(a, b, f_a, f_b), ctx)
        }
        (Decimal256(_), Decimal128(_)) => {
            let a = a.try_downcast::<Decimal256Type>().unwrap();
            let b = b.try_downcast::<Decimal128As256Type>().unwrap();
            let (f_a, f_b) = (i256::from(10).pow(m_a), i256::from(10).pow(m_b));
            compare_decimal(a, b, |a, b, _| Op::compare(a, b, f_a, f_b), ctx)
        }
    }
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

trait CmpOp {
    const NAME: &str;
    fn is(o: Ordering) -> bool;
    fn domain_op<T: SimpleDomainCmp>(a: &T, b: &T) -> FunctionDomain<BooleanType>;
    fn compare<D>(a: D, b: D, f_a: D, f_b: D) -> bool
    where D: Decimal + std::ops::Mul<Output = D> {
        if a.signum() != b.signum() {
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

macro_rules! define_cmp_op {
    ($name:ident, $func_name:expr, $is_fn:ident, $domain_op:ident) => {
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
    };
}

pub fn register_decimal_compare(registry: &mut FunctionRegistry) {
    define_cmp_op!(Equal, "eq", is_eq, domain_eq);
    define_cmp_op!(NotEqual, "noteq", is_ne, domain_noteq);
    define_cmp_op!(LessThan, "lt", is_lt, domain_lt);
    define_cmp_op!(GreaterThan, "gt", is_gt, domain_gt);
    define_cmp_op!(LessThanEqual, "lte", is_le, domain_lte);
    define_cmp_op!(GreaterThanEqual, "gte", is_ge, domain_gte);

    register_decimal_compare_op::<Equal>(registry);
    register_decimal_compare_op::<NotEqual>(registry);

    register_decimal_compare_op::<LessThan>(registry);
    register_decimal_compare_op::<GreaterThan>(registry);
    register_decimal_compare_op::<LessThanEqual>(registry);
    register_decimal_compare_op::<GreaterThanEqual>(registry);
}
