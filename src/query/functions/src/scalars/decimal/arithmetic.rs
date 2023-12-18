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

use std::ops::*;
use std::sync::Arc;

use databend_common_expression::types::decimal::*;
use databend_common_expression::types::*;
use databend_common_expression::vectorize_2_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::Domain;
use databend_common_expression::EvalContext;
use databend_common_expression::Function;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::Value;
use ethnum::i256;

#[derive(Copy, Clone, Debug)]
enum ArithmeticOp {
    Plus,
    Minus,
    Multiply,
    Divide,
}

macro_rules! op_decimal {
    ($a: expr, $b: expr, $ctx: expr, $left: expr, $right: expr, $result_type: expr, $op: ident, $arithmetic_op: expr) => {
        match $left {
            DecimalDataType::Decimal128(_) => {
                binary_decimal!(
                    $a,
                    $b,
                    $ctx,
                    $left,
                    $right,
                    $op,
                    $result_type.size(),
                    i128,
                    $arithmetic_op
                )
            }
            DecimalDataType::Decimal256(_) => {
                binary_decimal!(
                    $a,
                    $b,
                    $ctx,
                    $left,
                    $right,
                    $op,
                    $result_type.size(),
                    i256,
                    $arithmetic_op
                )
            }
        }
    };
}

macro_rules! binary_decimal {
    ($a: expr, $b: expr, $ctx: expr, $left: expr, $right: expr, $op: ident, $size: expr, $type_name: ty, $arithmetic_op: expr) => {{
        type T = $type_name;

        let overflow = $size.precision == T::default_decimal_size().precision;

        let a = $a.try_downcast().unwrap();
        let b = $b.try_downcast().unwrap();

        let zero = T::zero();
        let one = T::one();

        let result = if matches!($arithmetic_op, ArithmeticOp::Divide) {
            let scale_a = $left.scale();
            let scale_b = $right.scale();

            let (scale_mul, scale_div) = if scale_b + $size.scale > scale_a {
                (scale_b + $size.scale - scale_a, 0)
            } else {
                (0, scale_b + $size.scale - scale_a)
            };

            let multiplier = T::e(scale_mul as u32);
            let div = T::e(scale_div as u32);

            let func = |a: T, b: T, result: &mut Vec<T>, ctx: &mut EvalContext| {
                if std::intrinsics::unlikely(b == zero) {
                    ctx.set_error(result.len(), "divided by zero");
                    result.push(one);
                } else {
                    result.push((a * multiplier).div(b) / div);
                }
            };

            vectorize_with_builder_2_arg::<DecimalType<T>, DecimalType<T>, DecimalType<T>>(func)(
                a, b, $ctx,
            )
        } else {
            if overflow {
                let min_for_precision = T::min_for_precision($size.precision);
                let max_for_precision = T::max_for_precision($size.precision);

                let func = |a: T, b: T, result: &mut Vec<T>, ctx: &mut EvalContext| {
                    let t = a.$op(b);
                    if t < min_for_precision || t > max_for_precision {
                        ctx.set_error(
                            result.len(),
                            concat!("Decimal overflow at line : ", line!()),
                        );
                        result.push(one);
                    } else {
                        result.push(t);
                    }
                };

                vectorize_with_builder_2_arg::<DecimalType<T>, DecimalType<T>, DecimalType<T>>(func)(
                    a, b, $ctx
                )
            } else {
                let func = |l: T, r: T, _ctx: &mut EvalContext| l.$op(r);

                vectorize_2_arg::<DecimalType<T>, DecimalType<T>, DecimalType<T>>(func)(
                    a, b, $ctx
                )
            }
        };
        match result {
            Value::Scalar(x) => Value::Scalar(T::upcast_scalar(x, $size)),
            Value::Column(x) => Value::Column(T::upcast_column(x, $size)),
        }
    }};
}

#[inline(always)]
fn domain_plus<T: Decimal>(
    lhs: &SimpleDomain<T>,
    rhs: &SimpleDomain<T>,
    precision: u8,
) -> Option<SimpleDomain<T>> {
    // For plus, the scale of the two operands must be the same.
    let min = T::min_for_precision(precision);
    let max = T::max_for_precision(precision);
    Some(SimpleDomain {
        min: lhs
            .min
            .checked_add(rhs.min)
            .filter(|&m| m >= min && m <= max)?,
        max: lhs
            .max
            .checked_add(rhs.max)
            .filter(|&m| m >= min && m <= max)?,
    })
}

#[inline(always)]
fn domain_minus<T: Decimal>(
    lhs: &SimpleDomain<T>,
    rhs: &SimpleDomain<T>,
    precision: u8,
) -> Option<SimpleDomain<T>> {
    // For minus, the scale of the two operands must be the same.
    let min = T::min_for_precision(precision);
    let max = T::max_for_precision(precision);
    Some(SimpleDomain {
        min: lhs
            .min
            .checked_sub(rhs.max)
            .filter(|&m| m >= min && m <= max)?,
        max: lhs
            .max
            .checked_sub(rhs.min)
            .filter(|&m| m >= min && m <= max)?,
    })
}

#[inline(always)]
fn domain_mul<T: Decimal>(
    lhs: &SimpleDomain<T>,
    rhs: &SimpleDomain<T>,
    precision: u8,
) -> Option<SimpleDomain<T>> {
    let min = T::min_for_precision(precision);
    let max = T::max_for_precision(precision);

    let a = lhs
        .min
        .checked_mul(rhs.min)
        .filter(|&m| m >= min && m <= max)?;
    let b = lhs
        .min
        .checked_mul(rhs.max)
        .filter(|&m| m >= min && m <= max)?;
    let c = lhs
        .max
        .checked_mul(rhs.min)
        .filter(|&m| m >= min && m <= max)?;
    let d = lhs
        .max
        .checked_mul(rhs.max)
        .filter(|&m| m >= min && m <= max)?;

    Some(SimpleDomain {
        min: a.min(b).min(c).min(d),
        max: a.max(b).max(c).max(d),
    })
}

#[inline(always)]
fn domain_div<T: Decimal>(
    _lhs: &SimpleDomain<T>,
    _rhs: &SimpleDomain<T>,
    _precision: u8,
) -> Option<SimpleDomain<T>> {
    // For div, we cannot determine the domain.
    None
}

macro_rules! register_decimal_binary_op {
    ($registry: expr, $arithmetic_op: expr, $op: ident, $domain_op: ident, $default_domain: expr) => {
        let name = format!("{:?}", $arithmetic_op).to_lowercase();

        $registry.register_function_factory(&name, |_, args_type| {
            if args_type.len() != 2 {
                return None;
            }

            let has_nullable = args_type.iter().any(|x| x.is_nullable_or_null());
            let args_type: Vec<DataType> = args_type.iter().map(|x| x.remove_nullable()).collect();

            // number X decimal -> decimal
            // decimal X number -> decimal
            // decimal X decimal -> decimal
            if !args_type[0].is_decimal() && !args_type[1].is_decimal() {
                return None;
            }

            let decimal_a =
                DecimalDataType::from_size(args_type[0].get_decimal_properties()?).unwrap();
            let decimal_b =
                DecimalDataType::from_size(args_type[1].get_decimal_properties()?).unwrap();

            let is_multiply = matches!($arithmetic_op, ArithmeticOp::Multiply);
            let is_divide = matches!($arithmetic_op, ArithmeticOp::Divide);
            let is_plus_minus = !is_multiply && !is_divide;

            // left, right will unify to same width decimal, both 256 or both 128
            let (left, right, return_decimal_type) = DecimalDataType::binary_result_type(
                &decimal_a,
                &decimal_b,
                is_multiply,
                is_divide,
                is_plus_minus,
            )
            .ok()?;

            let function = Function {
                signature: FunctionSignature {
                    name: format!("{:?}", $arithmetic_op).to_lowercase(),
                    args_type: vec![
                        DataType::Decimal(left.clone()),
                        DataType::Decimal(right.clone()),
                    ],
                    return_type: DataType::Decimal(return_decimal_type),
                },
                eval: FunctionEval::Scalar {
                    calc_domain: Box::new(move |_ctx, d| {
                        let lhs = d[0].as_decimal();
                        let rhs = d[1].as_decimal();

                        if lhs.is_none() || rhs.is_none() {
                            return FunctionDomain::Full;
                        }

                        let lhs = lhs.unwrap();
                        let rhs = rhs.unwrap();

                        let size = return_decimal_type.size();

                        {
                            match (lhs, rhs) {
                                (
                                    DecimalDomain::Decimal128(d1, _),
                                    DecimalDomain::Decimal128(d2, _),
                                ) => $domain_op(&d1, &d2, size.precision)
                                    .map(|d| DecimalDomain::Decimal128(d, size)),
                                (
                                    DecimalDomain::Decimal256(d1, _),
                                    DecimalDomain::Decimal256(d2, _),
                                ) => $domain_op(&d1, &d2, size.precision)
                                    .map(|d| DecimalDomain::Decimal256(d, size)),
                                _ => {
                                    unreachable!("unreachable decimal domain {:?} /{:?}", lhs, rhs)
                                }
                            }
                        }
                        .map(|d| FunctionDomain::Domain(Domain::Decimal(d)))
                        .unwrap_or($default_domain)
                    }),
                    eval: Box::new(move |args, ctx| {
                        let res = op_decimal!(
                            &args[0],
                            &args[1],
                            ctx,
                            left,
                            right,
                            return_decimal_type,
                            $op,
                            $arithmetic_op
                        );

                        res
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

pub(crate) fn register_decimal_arithmetic(registry: &mut FunctionRegistry) {
    // TODO checked overflow by default
    register_decimal_binary_op!(
        registry,
        ArithmeticOp::Plus,
        add,
        domain_plus,
        FunctionDomain::Full
    );

    register_decimal_binary_op!(
        registry,
        ArithmeticOp::Minus,
        sub,
        domain_minus,
        FunctionDomain::Full
    );
    register_decimal_binary_op!(
        registry,
        ArithmeticOp::Divide,
        div,
        domain_div,
        FunctionDomain::MayThrow
    );
    register_decimal_binary_op!(
        registry,
        ArithmeticOp::Multiply,
        mul,
        domain_mul,
        FunctionDomain::Full
    );
}
