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

use common_datavalues::DataValueArithmeticOperator;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::ArithmeticFunction;
use crate::scalars::Function;
use crate::scalars::Monotonicity;

pub struct ArithmeticMulFunction;

impl ArithmeticMulFunction {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        ArithmeticFunction::try_create_func(DataValueArithmeticOperator::Mul)
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func)).features(
            FunctionFeatures::default()
                .deterministic()
                .monotonicity()
                .num_arguments(2),
        )
    }

    pub fn get_monotonicity(args: &[Monotonicity]) -> Result<Monotonicity> {
        arithmetic_mul_div_monotonicity(args, DataValueArithmeticOperator::Mul)
    }
}

pub fn arithmetic_mul_div_monotonicity(
    args: &[Monotonicity],
    op: DataValueArithmeticOperator,
) -> Result<Monotonicity> {
    if args.len() != 2 {
        return Err(ErrorCode::BadArguments(format!(
            "Invalid argument lengths {} for get_monotonicity",
            args.len()
        )));
    }

    if !matches!(
        op,
        DataValueArithmeticOperator::Mul | DataValueArithmeticOperator::Div
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
                        DataValueArithmeticOperator::Mul => Ok(Monotonicity::create(
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
                        DataValueArithmeticOperator::Mul => Ok(Monotonicity::create(
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
                                (true, true, DataValueArithmeticOperator::Mul)
                                | (true, false, DataValueArithmeticOperator::Div) => (true, true),
                                (false, false, DataValueArithmeticOperator::Mul)
                                | (false, true, DataValueArithmeticOperator::Div) => (true, false),
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
                                (true, true, DataValueArithmeticOperator::Div)
                                | (true, false, DataValueArithmeticOperator::Mul) => (true, false),
                                (false, false, DataValueArithmeticOperator::Div)
                                | (false, true, DataValueArithmeticOperator::Mul) => (true, true),
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
