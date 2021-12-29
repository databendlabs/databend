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

use std::marker::PhantomData;
use std::ops::Add;
use std::ops::Mul;
use std::ops::Sub;

use common_datavalues::prelude::*;
use common_datavalues::DataTypeAndNullable;
use common_exception::ErrorCode;
use common_exception::Result;
use num::cast::AsPrimitive;
use num_traits::WrappingAdd;
use num_traits::WrappingMul;
use num_traits::WrappingSub;

use super::arithmetic::ArithmeticTrait;
use super::result_type::ResultTypeOfBinaryArith;
use crate::binary_arithmetic;
use crate::binary_arithmetic_helper;
use crate::impl_binary_arith;
use crate::impl_wrapping_binary_arith;
use crate::scalars::function_factory::ArithmeticDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::BinaryArithmeticFunction;
use crate::scalars::BinaryArithmeticOperator;
use crate::scalars::Function;
use crate::scalars::Monotonicity;
use crate::with_match_primitive_type;

impl_wrapping_binary_arith!(ArithmeticWrappingMul, wrapping_mul);

impl_binary_arith!(ArithmeticMul, *);

pub struct ArithmeticMulFunction;

impl ArithmeticMulFunction {
    pub fn try_create_func(
        _display_name: &str,
        args: &[DataTypeAndNullable],
    ) -> Result<Box<dyn Function>> {
        let left_type = &args[0].data_type();
        let right_type = &args[1].data_type();
        let op = BinaryArithmeticOperator::Mul;

        let error_fn = || -> Result<Box<dyn Function>> {
            Err(ErrorCode::BadDataValueType(format!(
                "DataValue Error: Unsupported arithmetic ({:?}) {} ({:?})",
                left_type, op, right_type
            )))
        };

        if !left_type.is_numeric() || !right_type.is_numeric() {
            return error_fn();
        };

        with_match_primitive_type!(left_type, |$T| {
            with_match_primitive_type!(right_type, |$D| {
                let result_type = <($T, $D) as ResultTypeOfBinaryArith>::AddMul::data_type();
                match result_type {
                    DataType::UInt64 => BinaryArithmeticFunction::<ArithmeticWrappingMul<$T, $D, u64>>::try_create_func(
                        op,
                        result_type,
                    ),
                    DataType::Int64 => BinaryArithmeticFunction::<ArithmeticWrappingMul<$T, $D, i64>>::try_create_func(
                        op,
                        result_type,
                    ),
                    _ => BinaryArithmeticFunction::<ArithmeticMul<$T, $D, <($T, $D) as ResultTypeOfBinaryArith>::AddMul>>::try_create_func(
                        op,
                        result_type,
                    ),
                }
            }, {
                error_fn()
            })
        }, {
            error_fn()
        })
    }

    pub fn desc() -> ArithmeticDescription {
        ArithmeticDescription::creator(Box::new(Self::try_create_func)).features(
            FunctionFeatures::default()
                .deterministic()
                .monotonicity()
                .num_arguments(2),
        )
    }

    pub fn get_monotonicity(args: &[Monotonicity]) -> Result<Monotonicity> {
        arithmetic_mul_div_monotonicity(args, BinaryArithmeticOperator::Mul)
    }
}

pub fn arithmetic_mul_div_monotonicity(
    args: &[Monotonicity],
    op: BinaryArithmeticOperator,
) -> Result<Monotonicity> {
    if args.len() != 2 {
        return Err(ErrorCode::BadArguments(format!(
            "Invalid argument lengths {} for get_monotonicity",
            args.len()
        )));
    }

    if !matches!(
        op,
        BinaryArithmeticOperator::Mul | BinaryArithmeticOperator::Div
    ) {
        return Err(ErrorCode::BadArguments(format!(
            "Invalid operator '{}' for get_monotonicity",
            op
        )));
    }

    let f_x = &args[0];
    let g_x = &args[1];

    match (f_x.is_constant, g_x.is_constant) {
        // both f(x) and g(x) are constant
        (true, true) => Ok(Monotonicity::create_constant()),

        //f(x) is constant
        (true, false) => {
            match f_x.compare_with_zero()? {
                1 => {
                    match op {
                        // 12 * g(x)
                        BinaryArithmeticOperator::Mul => Ok(Monotonicity::create(
                            g_x.is_monotonic,
                            g_x.is_positive,
                            g_x.is_constant,
                        )),
                        // 12 / g(x)
                        _ => {
                            if g_x.compare_with_zero()? == 0 {
                                // unknown monotonicity
                                Ok(Monotonicity::default())
                            } else {
                                Ok(Monotonicity::create(
                                    g_x.is_monotonic,
                                    !g_x.is_positive, // flip the is_positive
                                    g_x.is_constant,
                                ))
                            }
                        }
                    }
                }
                -1 => {
                    match op {
                        // -12 * g(x)
                        BinaryArithmeticOperator::Mul => Ok(Monotonicity::create(
                            g_x.is_monotonic,
                            !g_x.is_positive, // flip the is_positive
                            g_x.is_constant,
                        )),
                        // -12 / g(x)
                        _ => {
                            if g_x.compare_with_zero()? == 0 {
                                // unknown monotonicity
                                Ok(Monotonicity::default())
                            } else {
                                Ok(Monotonicity::create(
                                    g_x.is_monotonic,
                                    g_x.is_positive,
                                    g_x.is_constant,
                                ))
                            }
                        }
                    }
                }
                _ => unreachable!(),
            }
        }

        // g(x) is constant
        (false, true) => {
            match g_x.compare_with_zero()? {
                1 => {
                    // f(x) *|/ 12
                    Ok(Monotonicity::create(
                        f_x.is_monotonic,
                        f_x.is_positive,
                        f_x.is_constant,
                    ))
                }
                -1 => {
                    // f(x) *|/ (-12), need to flip the is_positive for negative constant value.
                    Ok(Monotonicity::create(
                        f_x.is_monotonic,
                        !f_x.is_positive,
                        f_x.is_constant,
                    ))
                }
                _ => unreachable!(),
            }
        }

        // neither f(x) nor g(x) are constant, like abs(x) *|- (x+12)
        (false, false) => {
            if f_x.is_monotonic && g_x.is_monotonic {
                let f_x_compare_with_zero = f_x.compare_with_zero()?;
                let g_x_compare_with_zero = g_x.compare_with_zero()?;
                match (f_x_compare_with_zero, g_x_compare_with_zero) {
                    (1, 1) | (-1, -1) => {
                        // For case f(x) >= 0 && g(x) >= 0, we have following results. (f(x) <= 0 && g(x) <= 0 just need to flip the is_positive)
                        // f(x)⭡ * g(x)⭡ => ⭡
                        // f(x)⭡ / g(x)⭡ => unknown
                        // f(x)⭣ * g(x)⭣ => ⭣
                        // f(x)⭣ / g(x)⭣ => unknown
                        // f(x)⭡ * g(x)⭣ => unknown
                        // f(x)⭡ / g(x)⭣ => ⭡
                        // f(x)⭣ * g(x)⭡ => unknown
                        // f(x)⭣ / g(x)⭡ => ⭣
                        let (is_monotonic, mut is_positive) =
                            match (f_x.is_positive, g_x.is_positive, op) {
                                (true, true, BinaryArithmeticOperator::Mul)
                                | (true, false, BinaryArithmeticOperator::Div) => (true, true),
                                (false, false, BinaryArithmeticOperator::Mul)
                                | (false, true, BinaryArithmeticOperator::Div) => (true, false),
                                _ => (false, false),
                            };
                        // if f(x) <= 0 && g(x) <= 0 flip the is_positive
                        if f_x_compare_with_zero == -1 {
                            is_positive = !is_positive;
                        }
                        Ok(Monotonicity::create(is_monotonic, is_positive, false))
                    }
                    (-1, 1) | (1, -1) => {
                        // For case f(x) >= 0 && g(x) <= 0, we have following results. (f(x) <= 0 && g(x) >= 0 just need to flip the is_positive)
                        // f(x)⭡ * g(x)⭡ => unknown
                        // f(x)⭡ / g(x)⭡ => ⭣
                        // f(x)⭣ * g(x)⭣ => unknown
                        // f(x)⭣ / g(x)⭣ => ⭡
                        // f(x)⭡ * g(x)⭣ => ⭣
                        // f(x)⭡ / g(x)⭣ => unknown
                        // f(x)⭣ * g(x)⭡ => ⭡
                        // f(x)⭣ / g(x)⭡ => unknown
                        let (is_monotonic, mut is_positive) =
                            match (f_x.is_positive, g_x.is_positive, op) {
                                (true, true, BinaryArithmeticOperator::Div)
                                | (true, false, BinaryArithmeticOperator::Mul) => (true, false),
                                (false, false, BinaryArithmeticOperator::Div)
                                | (false, true, BinaryArithmeticOperator::Mul) => (true, true),
                                _ => (false, false),
                            };
                        // if f(x) <= 0 && g(x) >= 0 flip the is_positive
                        if f_x_compare_with_zero == -1 {
                            is_positive = !is_positive;
                        }
                        Ok(Monotonicity::create(is_monotonic, is_positive, false))
                    }
                    _ => Ok(Monotonicity::default()),
                }
            } else {
                // unknown monotonicity
                Ok(Monotonicity::default())
            }
        }
    }
}
