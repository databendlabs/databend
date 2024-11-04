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

use std::cmp::Ord;
use std::ops::*;
use std::sync::Arc;

use databend_common_expression::types::decimal::*;
use databend_common_expression::types::*;
use databend_common_expression::vectorize_2_arg;
use databend_common_expression::Domain;
use databend_common_expression::EvalContext;
use databend_common_expression::Function;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::SimpleDomainCmp;
use databend_common_expression::Value;
use databend_common_expression::ValueRef;
use ethnum::i256;

use super::convert_to_decimal;
use super::convert_to_decimal_domain;

#[inline]
fn compare_multiplier(scale_a: u8, scale_b: u8) -> (u32, u32) {
    (
        (scale_b - std::cmp::min(scale_a, scale_b)) as u32,
        (scale_a - std::cmp::min(scale_a, scale_b)) as u32,
    )
}

macro_rules! register_decimal_compare_op {
    ($registry: expr, $name: expr, $op: ident, $domain_op: tt) => {
        $registry.register_function_factory($name, |_, args_type| {
            if args_type.len() != 2 {
                return None;
            }

            let has_nullable = args_type.iter().any(|x| x.is_nullable_or_null());
            let args_type: Vec<DataType> = args_type.iter().map(|x| x.remove_nullable()).collect();

            // Only works for one of is decimal types
            if !args_type[0].is_decimal() && !args_type[1].is_decimal() {
                return None;
            }

            let decimal_a =
                DecimalDataType::from_size(args_type[0].get_decimal_properties()?).unwrap();
            let decimal_b =
                DecimalDataType::from_size(args_type[1].get_decimal_properties()?).unwrap();

            let sig_types = vec![DataType::Decimal(decimal_a), DataType::Decimal(decimal_b)];

            // Comparison between different decimal types must be same siganature types
            let function = Function {
                signature: FunctionSignature {
                    name: $name.to_string(),
                    args_type: sig_types.clone(),
                    return_type: DataType::Boolean,
                },
                eval: FunctionEval::Scalar {
                    calc_domain: Box::new(|ctx, d| {
                        let (s1, s2) = (
                            d[0].as_decimal().unwrap().decimal_size().scale,
                            d[1].as_decimal().unwrap().decimal_size().scale,
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
                                d1.$domain_op(&d2)
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
                                d1.$domain_op(&d2)
                            }
                            (
                                Domain::Decimal(DecimalDomain::Decimal128(_, _)),
                                Domain::Decimal(DecimalDomain::Decimal256(d2, _)),
                            ) => {
                                let d1 = convert_to_decimal_domain(
                                    ctx,
                                    d[0].clone(),
                                    DecimalDataType::Decimal256(DecimalSize {
                                        precision: MAX_DECIMAL256_PRECISION,
                                        scale: s1,
                                    }),
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
                                d1.$domain_op(&d2)
                            }
                            (
                                Domain::Decimal(DecimalDomain::Decimal256(d1, _)),
                                Domain::Decimal(DecimalDomain::Decimal128(_, _)),
                            ) => {
                                let d2 = convert_to_decimal_domain(
                                    ctx,
                                    d[1].clone(),
                                    DecimalDataType::Decimal256(DecimalSize {
                                        precision: MAX_DECIMAL256_PRECISION,
                                        scale: s2,
                                    }),
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
                                d1.$domain_op(&d2)
                            }
                            _ => unreachable!(),
                        };
                        new_domain.map(|d| Domain::Boolean(d))
                    }),
                    eval: Box::new(move |args, ctx| {
                        op_decimal! { &args[0], &args[1], &sig_types , $op, ctx}
                    }),
                },
            };
            if has_nullable {
                Some(Arc::new(function.passthrough_nullable()))
            } else {
                Some(Arc::new(function))
            }
        });
    };
}

macro_rules! op_decimal {
    ($a: expr, $b: expr, $args_type: expr,  $op: ident, $ctx: expr) => {
        let (dt1, dt2) = (
            $args_type[0].as_decimal().unwrap(),
            $args_type[1].as_decimal().unwrap(),
        );

        let (m1, m2) = compare_multiplier(dt1.scale(), dt2.scale());

        match (dt1, dt2) {
            (DecimalDataType::Decimal128(_), DecimalDataType::Decimal128(_)) => {
                let f = |a: i128, b: i128, _: &mut EvalContext| -> bool {
                    (a * 10_i128.pow(m1)).cmp(&(b * 10_i128.pow(m2))).$op()
                };
                compare_decimal($a, $b, f, $ctx)
            }
            (DecimalDataType::Decimal256(_), DecimalDataType::Decimal256(_)) => {
                let f = |a: i256, b: i256, _: &mut EvalContext| -> bool {
                    (a * i256::from(10).pow(m1))
                        .cmp(&(b * i256::from(10).pow(m2)))
                        .$op()
                };
                compare_decimal($a, $b, f, $ctx)
            }
            (DecimalDataType::Decimal128(s1), DecimalDataType::Decimal256(_)) => {
                let dest_type = DecimalDataType::Decimal256(DecimalSize {
                    precision: MAX_DECIMAL256_PRECISION,
                    scale: s1.scale,
                });
                let left = convert_to_decimal(
                    $a,
                    $ctx,
                    &DataType::Decimal(DecimalDataType::Decimal128(*s1)),
                    dest_type,
                );

                let f = |a: i256, b: i256, _: &mut EvalContext| -> bool {
                    (a * i256::from(10).pow(m1))
                        .cmp(&(b * i256::from(10).pow(m2)))
                        .$op()
                };
                compare_decimal(&left.as_ref(), $b, f, $ctx)
            }
            (DecimalDataType::Decimal256(_), DecimalDataType::Decimal128(s2)) => {
                let dest_type = DecimalDataType::Decimal256(DecimalSize {
                    precision: MAX_DECIMAL256_PRECISION,
                    scale: s2.scale,
                });
                let right = convert_to_decimal(
                    $b,
                    $ctx,
                    &DataType::Decimal(DecimalDataType::Decimal128(*s2)),
                    dest_type,
                );

                let f = |a: i256, b: i256, _: &mut EvalContext| -> bool {
                    (a * i256::from(10).pow(m1))
                        .cmp(&(b * i256::from(10).pow(m2)))
                        .$op()
                };
                compare_decimal($a, &right.as_ref(), f, $ctx)
            }
        }
    };
}

fn compare_decimal<T, F>(
    a: &ValueRef<AnyType>,
    b: &ValueRef<AnyType>,
    f: F,
    ctx: &mut EvalContext,
) -> Value<AnyType>
where
    T: Decimal,
    F: Fn(T, T, &mut EvalContext) -> bool + Copy + Send + Sync,
{
    let a = a.try_downcast().unwrap();
    let b = b.try_downcast().unwrap();
    let value = vectorize_2_arg::<DecimalType<T>, DecimalType<T>, BooleanType>(f)(a, b, ctx);
    value.upcast()
}

pub fn register_decimal_compare_op(registry: &mut FunctionRegistry) {
    register_decimal_compare_op!(registry, "lt", is_lt, domain_lt);
    register_decimal_compare_op!(registry, "eq", is_eq, domain_eq);
    register_decimal_compare_op!(registry, "gt", is_gt, domain_gt);
    register_decimal_compare_op!(registry, "lte", is_le, domain_lte);
    register_decimal_compare_op!(registry, "gte", is_ge, domain_gte);
    register_decimal_compare_op!(registry, "noteq", is_ne, domain_noteq);
}
