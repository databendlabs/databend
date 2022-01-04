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
use super::interval::*;
use crate::binary_arithmetic;
use crate::impl_binary_arith;
use crate::impl_try_create_datetime;
use crate::impl_wrapping_binary_arith;
use crate::scalars::function_factory::ArithmeticDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::BinaryArithmeticFunction;
use crate::scalars::Function;
use crate::scalars::Monotonicity;
use crate::with_match_date_type;
use crate::with_match_primitive_type;

impl_wrapping_binary_arith!(ArithmeticWrappingAdd, wrapping_add);

impl_binary_arith!(ArithmeticAdd, +);

pub struct ArithmeticPlusFunction;

impl ArithmeticPlusFunction {
    pub fn try_create_func(
        _display_name: &str,
        args: &[DataTypeAndNullable],
    ) -> Result<Box<dyn Function>> {
        let left_type = &args[0].data_type();
        let right_type = &args[1].data_type();
        let op = DataValueBinaryOperator::Plus;
        if left_type.is_interval() || right_type.is_interval() {
            return Self::try_create_interval(left_type, right_type);
        }
        if left_type.is_date_or_date_time() || right_type.is_date_or_date_time() {
            return Self::try_create_datetime(left_type, right_type);
        }

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
                let result_type = <($T, $D) as ResultTypeOfBinary>::AddMul::data_type();
                match result_type {
                    DataType::UInt64 => BinaryArithmeticFunction::<ArithmeticWrappingAdd<$T, $D, u64>>::try_create_func(
                        op,
                        result_type,
                    ),
                    DataType::Int64 => BinaryArithmeticFunction::<ArithmeticWrappingAdd<$T, $D, i64>>::try_create_func(
                        op,
                        result_type,
                    ),
                    _ => BinaryArithmeticFunction::<ArithmeticAdd<$T, $D, <($T, $D) as ResultTypeOfBinary>::AddMul>>::try_create_func(
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

    fn try_create_interval(lhs_type: &DataType, rhs_type: &DataType) -> Result<Box<dyn Function>> {
        let op = DataValueBinaryOperator::Plus;
        let (interval, result_type) = if rhs_type.is_date_or_date_time() && lhs_type.is_interval() {
            (lhs_type, rhs_type)
        } else if lhs_type.is_date_or_date_time() && rhs_type.is_interval() {
            (rhs_type, lhs_type)
        } else {
            return Err(ErrorCode::BadDataValueType(format!(
                "DataValue Error: Unsupported date coercion ({:?}) {} ({:?})",
                lhs_type, op, rhs_type
            )));
        };

        let interval = interval.remove_nullable();
        let result_type = result_type.remove_nullable();
        match interval {
            DataType::Interval(IntervalUnit::YearMonth) => match result_type.clone() {
                DataType::Date16 => {
                    BinaryArithmeticFunction::<IntervalMonthAddDate16>::try_create_func(
                        op,
                        result_type.clone(),
                    )
                }
                DataType::Date32 => {
                    BinaryArithmeticFunction::<IntervalMonthAddDate32>::try_create_func(
                        op,
                        result_type.clone(),
                    )
                }
                DataType::DateTime32(_) => {
                    BinaryArithmeticFunction::<IntervalMonthAddDatetime32>::try_create_func(
                        op,
                        result_type.clone(),
                    )
                }
                _ => unreachable!(),
            },
            DataType::Interval(IntervalUnit::DayTime) => match result_type.clone() {
                DataType::Date16 => {
                    BinaryArithmeticFunction::<IntervalDaytimeAddDate16>::try_create_func(
                        op,
                        result_type.clone(),
                    )
                }
                DataType::Date32 => {
                    BinaryArithmeticFunction::<IntervalDaytimeAddDate32>::try_create_func(
                        op,
                        result_type.clone(),
                    )
                }
                DataType::DateTime32(_) => {
                    BinaryArithmeticFunction::<IntervalDaytimeAddDatetime32>::try_create_func(
                        op,
                        result_type.clone(),
                    )
                }
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }

    impl_try_create_datetime!(DataValueBinaryOperator::Plus, ArithmeticAdd, true);

    pub fn desc() -> ArithmeticDescription {
        ArithmeticDescription::creator(Box::new(Self::try_create_func)).features(
            FunctionFeatures::default()
                .deterministic()
                .monotonicity()
                .num_arguments(2),
        )
    }

    pub fn get_monotonicity(args: &[Monotonicity]) -> Result<Monotonicity> {
        if args.len() != 2 {
            return Err(ErrorCode::BadArguments(format!(
                "Invalid argument lengths {} for get_monotonicity",
                args.len()
            )));
        }

        // For expression f(x) + g(x), only when both f(x) and g(x) are monotonic and have
        // same 'is_positive' can we get a monotonic expression.
        let f_x = &args[0];
        let g_x = &args[1];

        // if either one is non-monotonic, return non-monotonic
        if !f_x.is_monotonic || !g_x.is_monotonic {
            return Ok(Monotonicity::default());
        }

        // if f(x) is a constant value, return the monotonicity of g(x)
        if f_x.is_constant {
            return Ok(Monotonicity::create(
                g_x.is_monotonic,
                g_x.is_positive,
                g_x.is_constant,
            ));
        }

        // if g(x) is a constant value, return the monotonicity of f(x)
        if g_x.is_constant {
            return Ok(Monotonicity::create(
                f_x.is_monotonic,
                f_x.is_positive,
                f_x.is_constant,
            ));
        }

        // Now we have f(x) and g(x) both are non-constant.
        // When both are monotonic, but have different 'is_positive', we can't determine the monotonicity
        if f_x.is_positive != g_x.is_positive {
            return Ok(Monotonicity::default());
        }

        Ok(Monotonicity::create(true, f_x.is_positive, false))
    }
}
