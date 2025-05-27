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
use databend_common_expression::types::i256;
use databend_common_expression::types::*;
use databend_common_expression::vectorize_2_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::Domain;
use databend_common_expression::EvalContext;
use databend_common_expression::Function;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionFactory;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::Value;

use super::convert_to_decimal;
use super::convert_to_decimal_domain;

#[derive(Copy, Clone, Debug)]
enum ArithmeticOp {
    Plus,
    Minus,
    Multiply,
    Divide,
}

impl ArithmeticOp {
    fn calc<T>(&self, a: T, b: T) -> T
    where T: Add<Output = T> + Sub<Output = T> + Mul<Output = T> {
        match self {
            ArithmeticOp::Plus => a + b,
            ArithmeticOp::Minus => a - b,
            ArithmeticOp::Multiply => a * b,
            _ => unimplemented!(),
        }
    }

    fn calc_domain<T: Decimal>(
        &self,
        lhs: &SimpleDomain<T>,
        rhs: &SimpleDomain<T>,
        precision: u8,
    ) -> Option<SimpleDomain<T>> {
        match self {
            ArithmeticOp::Plus => domain_plus(lhs, rhs, precision),
            ArithmeticOp::Minus => domain_minus(lhs, rhs, precision),
            ArithmeticOp::Multiply => domain_mul(lhs, rhs, precision),
            ArithmeticOp::Divide => {
                // For div, we cannot determine the domain.
                None
            }
        }
    }

    // Returns binded types and result type
    fn result_size(
        &self,
        a: &DecimalSize,
        b: &DecimalSize,
    ) -> Option<(DecimalSize, DecimalSize, DecimalSize)> {
        // from snowflake: https://docs.snowflake.com/sql-reference/operators-arithmetic
        let (mut precision, scale) = match self {
            ArithmeticOp::Multiply => {
                let scale = (a.scale() + b.scale()).min(a.scale().max(b.scale()).max(12));
                let l = a.leading_digits() + b.leading_digits();
                (l + scale, scale)
            }

            ArithmeticOp::Divide => {
                let scale = a.scale().max((a.scale() + 6).min(12)); // scale must be >= a.sale()
                let l = a.leading_digits() + b.scale(); // l must be >= a.leading_digits()
                (l + scale, scale) // so precision must be >= a.precision()
            }

            ArithmeticOp::Plus | ArithmeticOp::Minus => {
                let scale = a.scale().max(b.scale());
                // for addition/subtraction, we add 1 to the width to ensure we don't overflow
                let plus_min_precision = a.leading_digits().max(b.leading_digits()) + scale + 1;
                (plus_min_precision, scale)
            }
        };

        // if the args both are Decimal128, we need to clamp the precision to 38
        if a.precision() <= MAX_DECIMAL128_PRECISION && b.precision() <= MAX_DECIMAL128_PRECISION {
            precision = precision.min(MAX_DECIMAL128_PRECISION);
        } else if precision <= MAX_DECIMAL128_PRECISION && a.data_kind() != b.data_kind() {
            // lift up to decimal256
            precision = MAX_DECIMAL128_PRECISION + 1;
        }
        precision = precision.min(MAX_DECIMAL256_PRECISION);

        let result_type = DecimalSize::new(precision, scale).ok()?;
        match self {
            ArithmeticOp::Multiply => Some((
                DecimalSize::new(precision, a.scale()).ok()?,
                DecimalSize::new(precision, b.scale()).ok()?,
                result_type,
            )),

            ArithmeticOp::Divide => {
                let p = precision.max(a.precision()).max(b.precision());
                Some((
                    DecimalSize::new(p, a.scale()).ok()?,
                    DecimalSize::new(p, b.scale()).ok()?,
                    result_type,
                ))
            }

            ArithmeticOp::Plus | ArithmeticOp::Minus => {
                Some((result_type, result_type, result_type))
            }
        }
    }
}

fn op_decimal(
    a: &Value<AnyType>,
    b: &Value<AnyType>,
    ctx: &mut EvalContext,
    left: DecimalDataType,
    right: DecimalDataType,
    result_type: DecimalDataType,
    op: ArithmeticOp,
) -> Value<AnyType> {
    match left {
        DecimalDataType::Decimal64(_) => {
            binary_decimal::<i64>(a, b, ctx, left, right, result_type.size(), op)
        }
        DecimalDataType::Decimal128(_) => {
            binary_decimal::<i128>(a, b, ctx, left, right, result_type.size(), op)
        }
        DecimalDataType::Decimal256(_) => {
            binary_decimal::<i256>(a, b, ctx, left, right, result_type.size(), op)
        }
    }
}

fn binary_decimal<T>(
    a: &Value<AnyType>,
    b: &Value<AnyType>,
    ctx: &mut EvalContext,
    left: DecimalDataType,
    right: DecimalDataType,
    size: DecimalSize,
    op: ArithmeticOp,
) -> Value<AnyType>
where
    T: Decimal + Add<Output = T> + Sub<Output = T> + Mul<Output = T>,
{
    let overflow = size.precision() == T::default_decimal_size().precision();

    let a = a.try_downcast().unwrap();
    let b = b.try_downcast().unwrap();

    let zero = T::zero();
    let one = T::one();

    let result = match op {
        ArithmeticOp::Divide => {
            let scale_a = left.scale();
            let scale_b = right.scale();

            // Note: the result scale is always larger than the left scale
            let scale_mul = (scale_b + size.scale() - scale_a) as u32;
            let func = |a: T, b: T, result: &mut Vec<T>, ctx: &mut EvalContext| {
                // We are using round div here which follow snowflake's behavior: https://docs.snowflake.com/sql-reference/operators-arithmetic
                // For example:
                // round_div(5, 2) --> 3
                // round_div(-5, 2) --> -3
                // round_div(5, -2) --> -3
                // round_div(-5, -2) --> 3
                if std::intrinsics::unlikely(b == zero) {
                    ctx.set_error(result.len(), "divided by zero");
                    result.push(one);
                } else {
                    match a.do_round_div(b, scale_mul) {
                        Some(t) => result.push(t),
                        None => {
                            ctx.set_error(
                                result.len(),
                                concat!("Decimal div overflow at line : ", line!()),
                            );
                            result.push(one);
                        }
                    }
                }
            };

            vectorize_with_builder_2_arg::<DecimalType<T>, DecimalType<T>, DecimalType<T>>(func)(
                a, b, ctx,
            )
        }

        ArithmeticOp::Multiply => {
            let scale_a = left.scale();
            let scale_b = right.scale();

            let scale_mul = scale_a + scale_b - size.scale();

            if scale_mul == 0 {
                let func = |a: T, b: T, _ctx: &mut EvalContext| op.calc(a, b);
                vectorize_2_arg::<DecimalType<T>, DecimalType<T>, DecimalType<T>>(func)(a, b, ctx)
            } else {
                let func = |a: T, b: T, result: &mut Vec<T>, ctx: &mut EvalContext| match a
                    .do_round_mul(b, scale_mul as u32, overflow)
                {
                    Some(t) => result.push(t),
                    None => {
                        ctx.set_error(
                            result.len(),
                            concat!("Decimal multiply overflow at line : ", line!()),
                        );
                        result.push(one);
                    }
                };

                vectorize_with_builder_2_arg::<DecimalType<T>, DecimalType<T>, DecimalType<T>>(func)(
                    a, b, ctx,
                )
            }
        }

        ArithmeticOp::Plus | ArithmeticOp::Minus => {
            if overflow {
                let min_for_precision = T::min_for_precision(size.precision());
                let max_for_precision = T::max_for_precision(size.precision());

                let func = |a: T, b: T, result: &mut Vec<T>, ctx: &mut EvalContext| {
                    let t = op.calc(a, b);

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
                    a, b, ctx,
                )
            } else {
                let func = |l: T, r: T, _ctx: &mut EvalContext| op.calc(l, r);

                vectorize_2_arg::<DecimalType<T>, DecimalType<T>, DecimalType<T>>(func)(a, b, ctx)
            }
        }
    };

    result.upcast_decimal(size)
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

fn register_decimal_binary_op(registry: &mut FunctionRegistry, arithmetic_op: ArithmeticOp) {
    let name = format!("{:?}", arithmetic_op).to_lowercase();

    let factory = FunctionFactory::Closure(Box::new(move |_, args_type| {
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

        let decimal_a = args_type[0].get_decimal_properties()?;
        let decimal_b = args_type[1].get_decimal_properties()?;

        // left, right will unify to same width decimal, both 256 or both 128
        let (left, right, return_decimal_type) =
            arithmetic_op.result_size(&decimal_a, &decimal_b)?;
        let (left, right, return_decimal_type) = (
            DecimalDataType::from(left),
            DecimalDataType::from(right),
            DecimalDataType::from(return_decimal_type),
        );

        let function = Function {
            signature: FunctionSignature {
                name: format!("{:?}", arithmetic_op).to_lowercase(),
                args_type: args_type.clone(),
                return_type: DataType::Decimal(return_decimal_type.size()),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(move |ctx, d| {
                    let lhs = convert_to_decimal_domain(ctx, d[0].clone(), left);
                    let rhs = convert_to_decimal_domain(ctx, d[1].clone(), right);

                    let (lhs, rhs) = match (lhs, rhs) {
                        (Some(lhs), Some(rhs)) => (lhs, rhs),
                        _ => return FunctionDomain::Full,
                    };

                    let size = return_decimal_type.size();

                    let default_domain = if matches!(arithmetic_op, ArithmeticOp::Divide) {
                        FunctionDomain::MayThrow
                    } else {
                        FunctionDomain::Full
                    };
                    {
                        match (lhs, rhs) {
                            (
                                DecimalDomain::Decimal128(d1, _),
                                DecimalDomain::Decimal128(d2, _),
                            ) => arithmetic_op
                                .calc_domain(&d1, &d2, size.precision())
                                .map(|d| DecimalDomain::Decimal128(d, size)),
                            (
                                DecimalDomain::Decimal256(d1, _),
                                DecimalDomain::Decimal256(d2, _),
                            ) => arithmetic_op
                                .calc_domain(&d1, &d2, size.precision())
                                .map(|d| DecimalDomain::Decimal256(d, size)),
                            _ => {
                                unreachable!("unreachable decimal domain {:?} /{:?}", lhs, rhs)
                            }
                        }
                    }
                    .map(|d| FunctionDomain::Domain(Domain::Decimal(d)))
                    .unwrap_or(default_domain)
                }),
                eval: Box::new(move |args, ctx| {
                    let a = convert_to_decimal(&args[0], ctx, &args_type[0], left);
                    let b = convert_to_decimal(&args[1], ctx, &args_type[1], right);

                    op_decimal(&a, &b, ctx, left, right, return_decimal_type, arithmetic_op)
                }),
            },
        };
        if has_nullable {
            Some(Arc::new(function.passthrough_nullable()))
        } else {
            Some(Arc::new(function))
        }
    }));

    registry.register_function_factory(&name, factory);
}

pub fn register_decimal_arithmetic(registry: &mut FunctionRegistry) {
    // TODO checked overflow by default
    register_decimal_binary_op(registry, ArithmeticOp::Plus);
    register_decimal_binary_op(registry, ArithmeticOp::Minus);
    register_decimal_binary_op(registry, ArithmeticOp::Divide);
    register_decimal_binary_op(registry, ArithmeticOp::Multiply);
}
