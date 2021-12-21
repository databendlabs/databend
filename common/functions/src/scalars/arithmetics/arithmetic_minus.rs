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
use std::ops::Sub;

use common_datavalues::prelude::*;
use common_datavalues::DataValueArithmeticOperator;
use common_exception::ErrorCode;
use common_exception::Result;
use num::cast::AsPrimitive;

use super::arithmetic::ArithmeticTrait;
use super::utils::assert_binary_arguments;
use super::utils::validate_input;
use crate::binary_arithmetic;
use crate::binary_arithmetic_helper;
use crate::interval_arithmetic;
use crate::scalars::dates::IntervalFunctionFactory;
use crate::scalars::function_factory::ArithmeticDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::BinaryArithmeticFunction;
use crate::scalars::Function;
use crate::scalars::Monotonicity;
use crate::with_match_arithmetic_type;

pub struct ArithmeticMinusFunction;

impl ArithmeticMinusFunction {
    pub fn try_create_func(
        _display_name: &str,
        arguments: Vec<DataField>,
    ) -> Result<Box<dyn Function>> {
        let op = DataValueArithmeticOperator::Minus;
        assert_binary_arguments(op.clone(), arguments.len())?;

        let left_type = arguments[0].data_type();
        let right_type = arguments[1].data_type();
        let result_type = if left_type.is_interval() || right_type.is_interval() {
            return Self::try_create_interval(left_type, right_type);
        } else if left_type.is_date_or_date_time() || right_type.is_date_or_date_time() {
            datetime_arithmetic_coercion(&op, left_type, right_type)?
        } else {
            numerical_arithmetic_coercion(&op, left_type, right_type)?
        };

        let e = Result::Err(ErrorCode::BadDataValueType(format!(
            "DataValue Error: Unsupported arithmetic ({:?}) {} ({:?})",
            left_type, op, right_type
        )));

        with_match_arithmetic_type!(left_type, |$T| {
            with_match_arithmetic_type!(right_type, |$D| {
                with_match_arithmetic_type!(result_type, |$R| {
                    BinaryArithmeticFunction::<PrimitiveSub::<$T, $D, $R>>::try_create_func(
                        op,
                        result_type.clone(),
                    )
                }, e)
            }, e)
        }, e)
    }

    pub fn try_create_interval(
        lhs_type: &DataType,
        rhs_type: &DataType,
    ) -> Result<Box<dyn Function>> {
        let op = DataValueArithmeticOperator::Minus;
        let e = Result::Err(ErrorCode::BadDataValueType(format!(
            "DataValue Error: Unsupported date coercion ({:?}) {} ({:?})",
            lhs_type, op, rhs_type
        )));

        // only allow date/datetime [+/-] interval
        let (interval, result_type) = if rhs_type.is_date_or_date_time() && lhs_type.is_interval() {
            (lhs_type, rhs_type)
        } else if lhs_type.is_date_or_date_time() && rhs_type.is_interval() {
            (rhs_type, lhs_type)
        } else {
            return e;
        };

        match interval {
            DataType::Interval(IntervalUnit::YearMonth) => match result_type.clone() {
                DataType::Date16 => {
                    BinaryArithmeticFunction::<IntervalMonthSubDate16>::try_create_func(
                        op,
                        result_type.clone(),
                    )
                }
                DataType::Date32 => {
                    BinaryArithmeticFunction::<IntervalMonthSubDate32>::try_create_func(
                        op,
                        result_type.clone(),
                    )
                }
                DataType::DateTime32(_) => {
                    BinaryArithmeticFunction::<IntervalMonthSubDatetime32>::try_create_func(
                        op,
                        result_type.clone(),
                    )
                }
                _ => unreachable!(),
            },
            DataType::Interval(IntervalUnit::DayTime) => match result_type.clone() {
                DataType::Date16 => {
                    BinaryArithmeticFunction::<IntervalDaytimeSubDate16>::try_create_func(
                        op,
                        result_type.clone(),
                    )
                }
                DataType::Date32 => {
                    BinaryArithmeticFunction::<IntervalDaytimeSubDate32>::try_create_func(
                        op,
                        result_type.clone(),
                    )
                }
                DataType::DateTime32(_) => {
                    BinaryArithmeticFunction::<IntervalDaytimeSubDatetime32>::try_create_func(
                        op,
                        result_type.clone(),
                    )
                }
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }

    pub fn desc() -> ArithmeticDescription {
        ArithmeticDescription::creator(Box::new(Self::try_create_func))
            .features(FunctionFeatures::default().deterministic().monotonicity())
    }

    pub fn get_monotonicity(args: &[Monotonicity]) -> Result<Monotonicity> {
        if args.is_empty() || args.len() > 2 {
            return Err(ErrorCode::BadArguments(format!(
                "Invalid argument lengths {} for get_monotonicity",
                args.len()
            )));
        }

        // unary operation like '-f(x)', just flip the is_positive.
        // also pass the is_constant, in case the input is a constant value.
        if args.len() == 1 {
            return Ok(Monotonicity::create(
                args[0].is_monotonic || args[0].is_constant,
                !args[0].is_positive,
                args[0].is_constant,
            ));
        }

        // For expression f(x) - g(x), only when both f(x) and g(x) are monotonic and have
        // opposite 'is_positive' can we get a monotonic expression.
        let f_x = &args[0];
        let g_x = &args[1];

        // case of 12 - g(x)
        if f_x.is_constant {
            return Ok(Monotonicity::create(
                g_x.is_monotonic || g_x.is_constant,
                !g_x.is_positive,
                g_x.is_constant,
            ));
        }

        // case of f(x) - 12
        if g_x.is_constant {
            return Ok(Monotonicity::create(
                f_x.is_monotonic,
                f_x.is_positive,
                f_x.is_constant,
            ));
        }

        // if either one is non-monotonic, return non-monotonic
        if !f_x.is_monotonic || !g_x.is_monotonic {
            return Ok(Monotonicity::default());
        }

        // when both are monotonic, and have same 'is_positive', we can't determine the monotonicity
        if f_x.is_positive == g_x.is_positive {
            return Ok(Monotonicity::default());
        }

        Ok(Monotonicity::create(true, f_x.is_positive, false))
    }
}

#[derive(Clone)]
pub struct PrimitiveSub<T, D, R> {
    t: PhantomData<T>,
    d: PhantomData<D>,
    r: PhantomData<R>,
}

impl<T, D, R> ArithmeticTrait for PrimitiveSub<T, D, R>
where
    T: DFPrimitiveType + AsPrimitive<R>,
    T: AsPrimitive<u64>,
    T: AsPrimitive<i64>,
    D: DFPrimitiveType + AsPrimitive<R>,
    D: AsPrimitive<u64>,
    D: AsPrimitive<i64>,
    R: DFPrimitiveType + Sub<Output = R>,
    DFPrimitiveArray<R>: IntoSeries,
{
    fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn> {
        match R::data_type() {
            DataType::UInt64 => binary_arithmetic!(
                columns[0].column(),
                columns[1].column(),
                u64,
                |l: u64, r: u64| l.wrapping_sub(r)
            ),
            DataType::Int64 => binary_arithmetic!(
                columns[0].column(),
                columns[1].column(),
                i64,
                |l: i64, r: i64| l.wrapping_sub(r)
            ),
            _ => binary_arithmetic!(columns[0].column(), columns[1].column(), R, |l: R, r: R| l
                - r),
        }
    }
}

#[derive(Clone)]
pub struct IntervalDaytimeSubDate16 {}

impl ArithmeticTrait for IntervalDaytimeSubDate16 {
    fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn> {
        let milliseconds_per_day = 24 * 3600 * 1000;
        interval_arithmetic! {&columns[0], &columns[1], u16, |l: i64, r: i64| (l - r/milliseconds_per_day) as u16}
    }
}

#[derive(Clone)]
pub struct IntervalDaytimeSubDate32 {}

impl ArithmeticTrait for IntervalDaytimeSubDate32 {
    fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn> {
        let milliseconds_per_day = 24 * 3600 * 1000;
        interval_arithmetic! {&columns[0], &columns[1], i32, |l: i64, r: i64| (l - r/milliseconds_per_day) as i32}
    }
}

#[derive(Clone)]
pub struct IntervalDaytimeSubDatetime32 {}

impl ArithmeticTrait for IntervalDaytimeSubDatetime32 {
    fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn> {
        let div = 1000;
        interval_arithmetic! {&columns[0], &columns[1], u32, |l: i64, r: i64| (l - r/div) as u32}
    }
}

#[derive(Clone)]
pub struct IntervalMonthSubDate16 {}

impl ArithmeticTrait for IntervalMonthSubDate16 {
    fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn> {
        IntervalFunctionFactory::interval_month_plus_minus_date16(
            &DataValueArithmeticOperator::Minus,
            &columns[0],
            &columns[1],
        )
    }
}

#[derive(Clone)]
pub struct IntervalMonthSubDate32 {}

impl ArithmeticTrait for IntervalMonthSubDate32 {
    fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn> {
        IntervalFunctionFactory::interval_month_plus_minus_date32(
            &DataValueArithmeticOperator::Minus,
            &columns[0],
            &columns[1],
        )
    }
}

#[derive(Clone)]
pub struct IntervalMonthSubDatetime32 {}

impl ArithmeticTrait for IntervalMonthSubDatetime32 {
    fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn> {
        IntervalFunctionFactory::interval_month_plus_minus_datetime32(
            &DataValueArithmeticOperator::Minus,
            &columns[0],
            &columns[1],
        )
    }
}
*/