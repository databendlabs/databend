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
/*
use std::marker::PhantomData;
use std::ops::Mul;

use common_datavalues::prelude::*;
use common_datavalues::DataValueArithmeticOperator;
use common_exception::ErrorCode;
use common_exception::Result;
use num::cast::AsPrimitive;

use super::arithmetic::ArithmeticTrait;
use super::utils::assert_binary_arguments;
use crate::binary_arithmetic;
use crate::binary_arithmetic_helper;
use crate::scalars::function_factory::ArithmeticDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::BinaryArithmeticFunction;
use crate::scalars::Function;
use crate::scalars::Monotonicity;
use crate::with_match_arithmetic_type;

pub struct ArithmeticMulFunction;

impl ArithmeticMulFunction {
    pub fn desc() -> ArithmeticDescription {
        ArithmeticDescription::creator(Box::new(Self::try_create_func))
            .features(FunctionFeatures::default().deterministic().monotonicity())
    }

    pub fn try_create_func(
        _display_name: &str,
        arguments: Vec<DataField>,
    ) -> Result<Box<dyn Function>> {
        let op = DataValueArithmeticOperator::Mul;
        assert_binary_arguments(op.clone(), arguments.len())?;

        let left_type = arguments[0].data_type();
        let right_type = arguments[1].data_type();
        let result_type = numerical_arithmetic_coercion(&op, left_type, right_type)?;

        let e = Result::Err(ErrorCode::BadDataValueType(format!(
            "DataValue Error: Unsupported arithmetic ({:?}) {} ({:?})",
            left_type, op, right_type
        )));

        with_match_arithmetic_type!(left_type, |$T| {
            with_match_arithmetic_type!(right_type, |$D| {
                with_match_arithmetic_type!(result_type, |$R| {
                    BinaryArithmeticFunction::<PrimitiveMul::<$T,$D,$R>>::try_create_func(
                        op,
                        result_type.clone(),
                    )
                }, e)
            }, e)
        }, e)
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

#[derive(Clone)]
pub struct PrimitiveMul<T, D, R> {
    t: PhantomData<T>,
    d: PhantomData<D>,
    r: PhantomData<R>,
}

impl<T, D, R> ArithmeticTrait for PrimitiveMul<T, D, R>
where
    T: DFPrimitiveType + AsPrimitive<R>,
    T: AsPrimitive<u64>,
    T: AsPrimitive<i64>,
    D: DFPrimitiveType + AsPrimitive<R>,
    D: AsPrimitive<u64>,
    D: AsPrimitive<i64>,
    R: DFPrimitiveType + Mul<Output = R>,
    DFPrimitiveArray<R>: IntoSeries,
{
    fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn> {
        match R::data_type() {
            DataType::UInt64 => binary_arithmetic!(
                columns[0].column(),
                columns[1].column(),
                u64,
                |l: u64, r: u64| l.wrapping_mul(r)
            ),
            DataType::Int64 => binary_arithmetic!(
                columns[0].column(),
                columns[1].column(),
                i64,
                |l: i64, r: i64| l.wrapping_mul(r)
            ),
            _ => binary_arithmetic!(columns[0].column(), columns[1].column(), R, |l: R, r: R| l
                * r),
        }
    }
}
*/