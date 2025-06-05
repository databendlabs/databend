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

use databend_common_expression::types::compute_view::Compute;
use databend_common_expression::types::compute_view::ComputeView;
use databend_common_expression::types::decimal::*;
use databend_common_expression::types::SimpleDomain;
use databend_common_expression::types::*;
use databend_common_expression::vectorize_1_arg;
use databend_common_expression::vectorize_2_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::with_decimal_mapped_type;
use databend_common_expression::Domain;
use databend_common_expression::EvalContext;
use databend_common_expression::Function;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionFactory;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::Value;

use super::convert_to_decimal_domain;
use crate::decimal_to_decimal_fast;
use crate::other_to_decimal;

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
        let (precision, scale) = match self {
            ArithmeticOp::Multiply => {
                let scale = (a.scale() + b.scale()).min(a.scale().max(b.scale()).max(12));
                let leading = a.leading_digits() + b.leading_digits();
                (leading + scale, scale)
            }

            ArithmeticOp::Divide => {
                let scale = a.scale().max((a.scale() + 6).min(12)); // scale must be >= a.sale()
                let leading = a.leading_digits() + b.scale(); // leading must be >= a.leading_digits()
                (leading + scale, scale) // so precision must be >= a.precision()
            }

            ArithmeticOp::Plus | ArithmeticOp::Minus => {
                let scale = a.scale().max(b.scale());
                // for addition/subtraction, we add 1 to the width to ensure we don't overflow
                let plus_min_precision = a.leading_digits().max(b.leading_digits()) + scale + 1;
                (plus_min_precision, scale)
            }
        };

        // if the args both are Decimal128, we need to clamp the precision to 38
        let precision = if a.precision() <= MAX_DECIMAL128_PRECISION
            && b.precision() <= MAX_DECIMAL128_PRECISION
        {
            precision.min(MAX_DECIMAL128_PRECISION)
        } else {
            precision.min(MAX_DECIMAL256_PRECISION)
        };

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

            ArithmeticOp::Plus | ArithmeticOp::Minus => Some((
                if scale == a.scale() {
                    *a
                } else {
                    DecimalSize::new(a.precision(), scale).ok()?
                },
                if scale == b.scale() {
                    *b
                } else {
                    DecimalSize::new(b.precision(), scale).ok()?
                },
                result_type,
            )),
        }
    }
}

fn convert_to_decimal(
    value: &Value<AnyType>,
    data_type: &DataType,
    size: DecimalSize,
    ctx: &mut EvalContext,
) -> ((Value<AnyType>, DecimalSize), DecimalDataType) {
    if data_type.is_decimal() {
        let (value_decimal, value_type) = decimal_to_decimal_fast(value, ctx, size);
        ((value_decimal, size), value_type)
    } else {
        let value_type = size.best_type();
        let value_decimal = other_to_decimal(value, ctx, data_type, value_type);
        ((value_decimal, size), value_type)
    }
}

fn op_decimal(
    a: (&Value<AnyType>, &DataType, DecimalSize),
    b: (&Value<AnyType>, &DataType, DecimalSize),
    ctx: &mut EvalContext,
    result_type: DecimalDataType,
    op: ArithmeticOp,
) -> Value<AnyType> {
    let (a, a_type) = convert_to_decimal(a.0, a.1, a.2, ctx);
    let (b, b_type) = convert_to_decimal(b.0, b.1, b.2, ctx);

    with_decimal_mapped_type!(|T| match result_type.size().best_type() {
        DecimalDataType::T(_) => {
            with_decimal_mapped_type!(|A| match a_type {
                DecimalDataType::A(_) => {
                    with_decimal_mapped_type!(|B| match b_type {
                        DecimalDataType::B(_) => {
                            with_decimal_mapped_type!(|OUT| match result_type {
                                DecimalDataType::OUT(size) => {
                                    binary_decimal::<
                                        DecimalConvert<T, OUT>,
                                        ComputeView<DecimalConvert<A, T>, _, _>,
                                        ComputeView<DecimalConvert<B, T>, _, _>,
                                        _,
                                        _,
                                    >(a, b, ctx, size, op)
                                }
                            })
                        }
                    })
                }
            })
        }
    })
}

fn binary_decimal<C, L, R, T, U>(
    (a, a_size): (Value<AnyType>, DecimalSize),
    (b, b_size): (Value<AnyType>, DecimalSize),
    ctx: &mut EvalContext,
    return_size: DecimalSize,
    op: ArithmeticOp,
) -> Value<AnyType>
where
    T: Decimal + Add<Output = T> + Sub<Output = T> + Mul<Output = T>,
    U: Decimal,
    C: Compute<CoreDecimal<T>, CoreDecimal<U>>,
    L: for<'a> AccessType<ScalarRef<'a> = T>,
    R: for<'a> AccessType<ScalarRef<'a> = T>,
{
    let overflow = return_size.precision() == T::default_decimal_size().precision();

    let a = a.try_downcast().unwrap();
    let b = b.try_downcast().unwrap();

    let zero = T::zero();
    let one = T::one();

    match op {
        ArithmeticOp::Divide => {
            let scale_a = a_size.scale();
            let scale_b = b_size.scale();

            // Note: the result scale is always larger than the left scale
            let scale_mul = (scale_b + return_size.scale() - scale_a) as u32;
            let func = |a: T, b: T, result: &mut Vec<U>, ctx: &mut EvalContext| {
                // We are using round div here which follow snowflake's behavior: https://docs.snowflake.com/sql-reference/operators-arithmetic
                // For example:
                // round_div(5, 2) --> 3
                // round_div(-5, 2) --> -3
                // round_div(5, -2) --> -3
                // round_div(-5, -2) --> 3
                if std::intrinsics::unlikely(b == zero) {
                    ctx.set_error(result.len(), "divided by zero");
                    result.push(C::compute(&one));
                } else {
                    match a.do_round_div(b, scale_mul) {
                        Some(t) => result.push(C::compute(&t)),
                        None => {
                            ctx.set_error(
                                result.len(),
                                concat!("Decimal div overflow at line : ", line!()),
                            );
                            result.push(C::compute(&one));
                        }
                    }
                }
            };

            vectorize_with_builder_2_arg::<L, R, DecimalType<U>>(func)(a, b, ctx)
        }

        ArithmeticOp::Multiply => {
            let scale_a = a_size.scale();
            let scale_b = b_size.scale();

            let scale_mul = scale_a + scale_b - return_size.scale();

            if scale_mul == 0 {
                vectorize_2_arg::<L, R, DecimalType<U>>(|a, b, _| C::compute(&op.calc(a, b)))(
                    a, b, ctx,
                )
            } else {
                let func = |a: T, b: T, result: &mut Vec<U>, ctx: &mut EvalContext| match a
                    .do_round_mul(b, scale_mul as u32, overflow)
                {
                    Some(t) => result.push(C::compute(&t)),
                    None => {
                        ctx.set_error(
                            result.len(),
                            concat!("Decimal multiply overflow at line : ", line!()),
                        );
                        result.push(C::compute(&one));
                    }
                };

                vectorize_with_builder_2_arg::<L, R, DecimalType<U>>(func)(a, b, ctx)
            }
        }

        ArithmeticOp::Plus | ArithmeticOp::Minus => {
            if overflow {
                let min_for_precision = T::min_for_precision(return_size.precision());
                let max_for_precision = T::max_for_precision(return_size.precision());

                let func = |a: T, b: T, result: &mut Vec<U>, ctx: &mut EvalContext| {
                    let t = op.calc(a, b);

                    if t < min_for_precision || t > max_for_precision {
                        ctx.set_error(
                            result.len(),
                            concat!("Decimal overflow at line : ", line!()),
                        );
                        result.push(C::compute(&one));
                    } else {
                        result.push(C::compute(&t));
                    }
                };

                vectorize_with_builder_2_arg::<L, R, DecimalType<U>>(func)(a, b, ctx)
            } else {
                vectorize_2_arg::<L, R, DecimalType<U>>(|l, r, _| C::compute(&op.calc(l, r)))(
                    a, b, ctx,
                )
            }
        }
    }
    .upcast_with_type(&DataType::Decimal(return_size))
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

        let (left_size, right_size, return_size) =
            arithmetic_op.result_size(&decimal_a, &decimal_b)?;

        let function = Function {
            signature: FunctionSignature {
                name: format!("{:?}", arithmetic_op).to_lowercase(),
                args_type: args_type.clone(),
                return_type: DataType::Decimal(return_size),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(move |ctx, d| {
                    let (left, right) =
                        if left_size.can_carried_by_128() && right_size.can_carried_by_128() {
                            (
                                DecimalDataType::Decimal128(left_size),
                                DecimalDataType::Decimal128(right_size),
                            )
                        } else {
                            (
                                DecimalDataType::Decimal256(left_size),
                                DecimalDataType::Decimal256(right_size),
                            )
                        };
                    let lhs = convert_to_decimal_domain(ctx, d[0].clone(), left);
                    let rhs = convert_to_decimal_domain(ctx, d[1].clone(), right);

                    let (lhs, rhs) = match (lhs, rhs) {
                        (Some(lhs), Some(rhs)) => (lhs, rhs),
                        _ => return FunctionDomain::Full,
                    };

                    let size = return_size;

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
                    let return_decimal_type = if !ctx.strict_eval && return_size.can_carried_by_64()
                    {
                        DecimalDataType::Decimal64(return_size)
                    } else {
                        DecimalDataType::from(return_size)
                    };

                    op_decimal(
                        (&args[0], &args_type[0], left_size),
                        (&args[1], &args_type[1], right_size),
                        ctx,
                        return_decimal_type,
                        arithmetic_op,
                    )
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

pub fn register_decimal_minus(registry: &mut FunctionRegistry) {
    registry.register_function_factory("minus", FunctionFactory::Closure(Box::new(|_params, args_type| {
        if args_type.len() != 1 {
            return None;
        }

        let is_nullable = args_type[0].is_nullable();
        let arg_type = args_type[0].remove_nullable();
        if !arg_type.is_decimal() {
            return None;
        }

        let function = Function {
            signature: FunctionSignature {
                name: "minus".to_string(),
                args_type: vec![arg_type.clone()],
                return_type: arg_type.clone(),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, d| match &d[0] {
                    Domain::Decimal(DecimalDomain::Decimal64(d, size)) => {
                        FunctionDomain::Domain(Domain::Decimal(DecimalDomain::Decimal64(
                            SimpleDomain {
                                min: -d.max,
                                max: d.min.checked_neg().unwrap_or(i64::DECIMAL_MAX), // Only -MIN could overflow
                            },
                            *size,
                        )))
                    }
                    Domain::Decimal(DecimalDomain::Decimal128(d, size)) => {
                        FunctionDomain::Domain(Domain::Decimal(DecimalDomain::Decimal128(
                            SimpleDomain {
                                min: -d.max,
                                max: d.min.checked_neg().unwrap_or(i128::DECIMAL_MAX), // Only -MIN could overflow
                            },
                            *size,
                        )))
                    }
                    Domain::Decimal(DecimalDomain::Decimal256(d, size)) => {
                        FunctionDomain::Domain(Domain::Decimal(DecimalDomain::Decimal256(
                            SimpleDomain {
                                min: -d.max,
                                max: d.min.checked_neg().unwrap_or(i256::DECIMAL_MAX), // Only -MIN could overflow
                            },
                            *size,
                        )))
                    }
                    _ => unreachable!(),
                }),
                eval: Box::new(unary_minus_decimal),
            },
        };

        if is_nullable {
            Some(Arc::new(function.passthrough_nullable()))
        } else {
            Some(Arc::new(function))
        }
    })));
}

fn unary_minus_decimal(args: &[Value<AnyType>], ctx: &mut EvalContext) -> Value<AnyType> {
    let arg = &args[0];
    let (decimal, _) = DecimalDataType::from_value(arg).unwrap();
    match decimal {
        DecimalDataType::Decimal64(size) => {
            if ctx.strict_eval {
                let arg = arg.try_downcast().unwrap();
                vectorize_1_arg::<Decimal64As128Type, Decimal128Type>(|t, _| -t)(arg, ctx)
                    .upcast_with_type(&DataType::Decimal(size))
            } else {
                let arg = arg.try_downcast().unwrap();
                type T = DecimalType<i64>;
                vectorize_1_arg::<T, T>(|t, _| -t)(arg, ctx)
                    .upcast_with_type(&DataType::Decimal(size))
            }
        }
        DecimalDataType::Decimal128(size) => {
            let arg = arg.try_downcast().unwrap();
            type T = DecimalType<i128>;
            vectorize_1_arg::<T, T>(|t, _| -t)(arg, ctx).upcast_with_type(&DataType::Decimal(size))
        }
        DecimalDataType::Decimal256(size) => {
            let arg = arg.try_downcast().unwrap();
            type T = DecimalType<i256>;
            vectorize_1_arg::<T, T>(|t, _| -t)(arg, ctx).upcast_with_type(&DataType::Decimal(size))
        }
    }
}

pub fn register_decimal_arithmetic(registry: &mut FunctionRegistry) {
    // TODO checked overflow by default
    register_decimal_binary_op(registry, ArithmeticOp::Plus);
    register_decimal_binary_op(registry, ArithmeticOp::Minus);
    register_decimal_binary_op(registry, ArithmeticOp::Divide);
    register_decimal_binary_op(registry, ArithmeticOp::Multiply);
}
