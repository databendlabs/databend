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
        FunctionDescription::creator(Box::new(Self::try_create_func))
            .features(FunctionFeatures::default().deterministic().monotonicity())
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

    let err_opt = if let DataValueArithmeticOperator::Mul | DataValueArithmeticOperator::Div = op {
        None
    } else {
        Some(Err(ErrorCode::BadArguments(format!(
            "Invalid operator '{}' for get_monotonicity",
            op
        ))))
    };
    if let Some(err) = err_opt {
        return err;
    }

    let flip_positive_by_operator = |positive: bool| -> bool {
        match op {
            DataValueArithmeticOperator::Mul => positive,
            DataValueArithmeticOperator::Div => !positive,
            _ => unreachable!(),
        }
    };

    let f_x = &args[0];
    let g_x = &args[1];

    match (f_x.is_constant, g_x.is_constant) {
        // both f(x) and g(x) are constant
        (true, true) => Ok(Monotonicity::create_constant()),

        //f(x) is constant
        (true, false) => {
            if f_x.gt_eq_zero()? {
                match op {
                    // 12 * g(x)
                    DataValueArithmeticOperator::Mul => Ok(Monotonicity::create(
                        g_x.is_monotonic,
                        flip_positive_by_operator(g_x.is_positive),
                        g_x.is_constant,
                    )),
                    // 12 / g(x)
                    _ => {
                        if g_x.gt_eq_zero()? || g_x.lt_eq_zero()? {
                            Ok(Monotonicity::create(
                                g_x.is_monotonic,
                                !g_x.is_positive, // flip the is_positive
                                g_x.is_constant,
                            ))
                        } else {
                            // unknown monotonicity
                            Ok(Monotonicity::default())
                        }
                    }
                }
            } else if f_x.lt_eq_zero()? {
                match op {
                    // -12 * g(x)
                    DataValueArithmeticOperator::Mul => Ok(Monotonicity::create(
                        g_x.is_monotonic,
                        !g_x.is_positive, // flip the is_positive
                        g_x.is_constant,
                    )),
                    // -12 / g(x)
                    _ => {
                        if g_x.gt_eq_zero()? || g_x.lt_eq_zero()? {
                            Ok(Monotonicity::create(
                                g_x.is_monotonic,
                                g_x.is_positive,
                                g_x.is_constant,
                            ))
                        } else {
                            // unknown monotonicity
                            Ok(Monotonicity::default())
                        }
                    }
                }
            } else {
                // unknown monotonicity
                Ok(Monotonicity::default())
            }
        }

        // g(x) is constant
        (false, true) => {
            if g_x.gt_eq_zero()? {
                // f(x) *|/ 12
                Ok(Monotonicity::create(
                    f_x.is_monotonic,
                    f_x.is_positive,
                    f_x.is_constant,
                ))
            } else if g_x.lt_eq_zero()? {
                // f(x) *|/ (-12), need to flip the is_positive for negative constant value.
                Ok(Monotonicity::create(
                    f_x.is_monotonic,
                    !f_x.is_positive,
                    f_x.is_constant,
                ))
            } else {
                // unknown monotonicity
                Ok(Monotonicity::default())
            }
        }

        // neither f(x) nor g(x) are constant, like abs(x) *|- (x+12)
        (false, false) => {
            if f_x.is_monotonic && g_x.is_monotonic {
                let f_x_gt_eq_zero = f_x.gt_eq_zero()?;
                let f_x_lt_eq_zero = f_x.lt_eq_zero()?;
                let g_x_gt_eq_zero = g_x.gt_eq_zero()?;
                let g_x_lt_eq_zero = g_x.lt_eq_zero()?;

                if f_x_gt_eq_zero && g_x_gt_eq_zero || f_x_lt_eq_zero && g_x_lt_eq_zero {
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
                    if f_x_lt_eq_zero {
                        is_positive = !is_positive;
                    }
                    Ok(Monotonicity::create(is_monotonic, is_positive, false))
                } else if f_x_gt_eq_zero && g_x_lt_eq_zero || f_x_lt_eq_zero && g_x_gt_eq_zero {
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
                    if f_x_lt_eq_zero {
                        is_positive = !is_positive;
                    }
                    Ok(Monotonicity::create(is_monotonic, is_positive, false))
                } else {
                    Ok(Monotonicity::default())
                }
            } else {
                // unknown monotonicity
                Ok(Monotonicity::default())
            }
        }
    }
}
