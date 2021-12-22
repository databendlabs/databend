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

use common_datavalues::prelude::*;
use common_datavalues::DataValueArithmeticOperator;
use common_exception::ErrorCode;
use common_exception::Result;
use num::cast::AsPrimitive;
use num_traits::WrappingAdd;
use num_traits::WrappingSub;

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
use crate::with_match_date_type;
use crate::with_match_integer_16;
use crate::with_match_integer_32;
use crate::with_match_integer_64;
use crate::with_match_integer_8;
use crate::with_match_primitive_type;
use crate::impl_wrapping_arithmetic;
use crate::impl_arithmetic;

pub struct ArithmeticPlusFunction;

impl ArithmeticPlusFunction {
    pub fn try_create_func(
        _display_name: &str,
        arguments: Vec<DataField>,
    ) -> Result<Box<dyn Function>> {
        let op = DataValueArithmeticOperator::Plus;
        assert_binary_arguments(op.clone(), arguments.len())?;

        let left_type = arguments[0].data_type();
        let right_type = arguments[1].data_type();
        if left_type.is_interval() || right_type.is_interval() {
            return Self::try_create_interval(left_type, right_type);
        }
        if left_type.is_date_or_date_time() || right_type.is_date_or_date_time() {
            return try_create_datatime(&op, left_type, right_type);
        }

        let e = Result::Err(ErrorCode::BadDataValueType(format!(
            "DataValue Error: Unsupported arithmetic ({:?}) {} ({:?})",
            left_type, op, right_type
        )));

        if !left_type.is_numeric() || !right_type.is_numeric() {
            return e;
        };
        let has_signed = left_type.is_signed_numeric() || right_type.is_signed_numeric();
        match (left_type, right_type) {
            (DataType::Float64, _) => with_match_primitive_type!(right_type, |$D| {
                BinaryArithmeticFunction::<ArithmeticAdd<f64, $D, f64>>::try_create_func(
                    op,
                    DataType::Float64,
                )
            }, e),
            (DataType::Float32, _) => with_match_primitive_type!(right_type, |$D| {
                BinaryArithmeticFunction::<ArithmeticAdd<f32, $D, f64>>::try_create_func(
                    op,
                    DataType::Float64,
                )
            }, e),
            (_, DataType::Float64) => with_match_integer_64!(left_type, |$T| {
                BinaryArithmeticFunction::<ArithmeticAdd<$T, f64, f64>>::try_create_func(
                    op,
                    DataType::Float64,
                )
            }, e),
            (_, DataType::Float32) => with_match_integer_64!(left_type, |$T| {
                BinaryArithmeticFunction::<ArithmeticAdd<$T, f32, f64>>::try_create_func(
                    op,
                    DataType::Float64,
                )
            }, e),
            (DataType::Int64, _) => with_match_integer_64!(right_type, |$D| {
                BinaryArithmeticFunction::<ArithmeticWrappingAdd<i64, $D, i64>>::try_create_func(
                    op,
                    DataType::Int64,
                )
            }, e),
            (DataType::UInt64, _) => with_match_integer_64!(right_type, |$D| {
                if has_signed {
                    BinaryArithmeticFunction::<ArithmeticWrappingAdd<u64, $D, i64>>::try_create_func(
                        op,
                        DataType::Int64,
                    )
                } else {
                    BinaryArithmeticFunction::<ArithmeticWrappingAdd<u64, $D, u64>>::try_create_func(
                        op,
                        DataType::UInt64,
                    )
                }
            }, e),
            (_, DataType::UInt64) => with_match_integer_32!(left_type, |$T| {
                if has_signed {
                    BinaryArithmeticFunction::<ArithmeticWrappingAdd<$T, u64, i64>>::try_create_func(
                        op,
                        DataType::Int64,
                    )
                } else {
                    BinaryArithmeticFunction::<ArithmeticWrappingAdd<$T, u64, u64>>::try_create_func(
                        op,
                        DataType::UInt64,
                    )
                }
            }, e),
            (_, DataType::Int64) => with_match_integer_32!(left_type, |$T| {
                BinaryArithmeticFunction::<ArithmeticWrappingAdd<$T, i64, i64>>::try_create_func(
                    op,
                    DataType::Int64,
                )
            }, e),
            (DataType::UInt32, _) => with_match_integer_32!(right_type, |$D| {
                if has_signed {
                    BinaryArithmeticFunction::<ArithmeticWrappingAdd<u32, $D, i64>>::try_create_func(
                        op,
                        DataType::Int64,
                    )
                } else {
                    BinaryArithmeticFunction::<ArithmeticWrappingAdd<u32, $D, u64>>::try_create_func(
                        op,
                        DataType::UInt64,
                    )
                }
            }, e),
            (DataType::Int32, _) => with_match_integer_32!(right_type, |$D| {
                BinaryArithmeticFunction::<ArithmeticWrappingAdd<i32, $D, i64>>::try_create_func(
                    op,
                    DataType::Int64,
                )
            }, e),
            (_, DataType::UInt32) => with_match_integer_16!(left_type, |$T| {
                if has_signed {
                    BinaryArithmeticFunction::<ArithmeticWrappingAdd<$T, u32, i64>>::try_create_func(
                        op,
                        DataType::Int64,
                    )
                } else {
                    BinaryArithmeticFunction::<ArithmeticWrappingAdd<$T, u32, u64>>::try_create_func(
                        op,
                        DataType::UInt64,
                    )
                }
            }, e),
            (_, DataType::Int32) => with_match_integer_16!(left_type, |$T| {
                BinaryArithmeticFunction::<ArithmeticWrappingAdd<$T, i32, i64>>::try_create_func(
                    op,
                    DataType::Int64,
                )
            }, e),
            (DataType::UInt16, _) => with_match_integer_16!(right_type, |$D| {
                if has_signed {
                    BinaryArithmeticFunction::<ArithmeticAdd<u16, $D, i32>>::try_create_func(
                        op,
                        DataType::Int32,
                    )
                } else {
                    BinaryArithmeticFunction::<ArithmeticAdd<u16, $D, u32>>::try_create_func(
                        op,
                        DataType::UInt32,
                    )
                }
            }, e),
            (DataType::Int16, _) => with_match_integer_16!(right_type, |$D| {
                BinaryArithmeticFunction::<ArithmeticAdd<i16, $D, i32>>::try_create_func(
                    op,
                    DataType::Int32,
                )
            }, e),
            (_, DataType::UInt16) => match left_type {
                DataType::UInt8 => {
                    BinaryArithmeticFunction::<ArithmeticAdd<u8, u16, u32>>::try_create_func(
                        op,
                        DataType::UInt32,
                    )
                }
                DataType::Int8 => {
                    BinaryArithmeticFunction::<ArithmeticAdd<i8, u16, i32>>::try_create_func(
                        op,
                        DataType::Int32,
                    )
                }
                _ => unreachable!(),
            },
            (_, DataType::Int16) => with_match_integer_8!(left_type, |$T| {
                BinaryArithmeticFunction::<ArithmeticAdd<$T, i16, i32>>::try_create_func(
                    op,
                    DataType::Int32,
                )
            }, e),
            (DataType::UInt8, _) => match right_type {
                DataType::UInt8 => {
                    BinaryArithmeticFunction::<ArithmeticAdd<u8, u8, u16>>::try_create_func(
                        op,
                        DataType::UInt16,
                    )
                }
                DataType::Int8 => {
                    BinaryArithmeticFunction::<ArithmeticAdd<i8, i8, i16>>::try_create_func(
                        op,
                        DataType::Int16,
                    )
                }
                _ => unreachable!(),
            },
            (DataType::Int8, _) => with_match_integer_8!(right_type, |$D| {
                BinaryArithmeticFunction::<ArithmeticAdd<i8, $D, i16>>::try_create_func(
                    op,
                    DataType::Int16,
                )
            }, e),
            _ => unreachable!(),
        }
    }

    fn try_create_interval(lhs_type: &DataType, rhs_type: &DataType) -> Result<Box<dyn Function>> {
        let op = DataValueArithmeticOperator::Plus;
        let e = Result::Err(ErrorCode::BadDataValueType(format!(
            "DataValue Error: Unsupported date coercion ({:?}) {} ({:?})",
            lhs_type, op, rhs_type
        )));

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

    pub fn desc() -> ArithmeticDescription {
        ArithmeticDescription::creator(Box::new(Self::try_create_func)).features(
            FunctionFeatures::default()
                .deterministic()
                .monotonicity()
                .num_arguments(2),
        )
    }

    pub fn get_monotonicity(args: &[Monotonicity]) -> Result<Monotonicity> {
        if args.is_empty() || args.len() > 2 {
            return Err(ErrorCode::BadArguments(format!(
                "Invalid argument lengths {} for get_monotonicity",
                args.len()
            )));
        }

        if args.len() == 1 {
            return Ok(args[0].clone());
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

impl_wrapping_arithmetic!(ArithmeticWrappingAdd, wrapping_add);

impl_arithmetic!(ArithmeticAdd, +);

#[derive(Clone)]
pub struct IntervalDaytimeAddDate16 {}

impl ArithmeticTrait for IntervalDaytimeAddDate16 {
    fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn> {
        let milliseconds_per_day = 24 * 3600 * 1000;
        interval_arithmetic! {&columns[0], &columns[1], u16, |l: i64, r: i64| (l + r/milliseconds_per_day) as u16}
    }
}

#[derive(Clone)]
pub struct IntervalDaytimeAddDate32 {}

impl ArithmeticTrait for IntervalDaytimeAddDate32 {
    fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn> {
        let milliseconds_per_day = 24 * 3600 * 1000;
        interval_arithmetic! {&columns[0], &columns[1], i32, |l: i64, r: i64| (l + r/milliseconds_per_day) as i32}
    }
}

#[derive(Clone)]
pub struct IntervalDaytimeAddDatetime32 {}

impl ArithmeticTrait for IntervalDaytimeAddDatetime32 {
    fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn> {
        let div = 1000;
        interval_arithmetic! {&columns[0], &columns[1], u32, |l: i64, r: i64| (l + r/div) as u32}
    }
}

#[derive(Clone)]
pub struct IntervalMonthAddDate16 {}

impl ArithmeticTrait for IntervalMonthAddDate16 {
    fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn> {
        IntervalFunctionFactory::interval_month_plus_minus_date16(
            &DataValueArithmeticOperator::Plus,
            &columns[0],
            &columns[1],
        )
    }
}

#[derive(Clone)]
pub struct IntervalMonthAddDate32 {}

impl ArithmeticTrait for IntervalMonthAddDate32 {
    fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn> {
        IntervalFunctionFactory::interval_month_plus_minus_date32(
            &DataValueArithmeticOperator::Plus,
            &columns[0],
            &columns[1],
        )
    }
}

#[derive(Clone)]
pub struct IntervalMonthAddDatetime32 {}

impl ArithmeticTrait for IntervalMonthAddDatetime32 {
    fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn> {
        IntervalFunctionFactory::interval_month_plus_minus_datetime32(
            &DataValueArithmeticOperator::Plus,
            &columns[0],
            &columns[1],
        )
    }
}

fn try_create_datatime(
    op: &DataValueArithmeticOperator,
    lhs_type: &DataType,
    rhs_type: &DataType,
) -> Result<Box<dyn Function>> {
    let e = Result::Err(ErrorCode::BadDataValueType(format!(
        "DataValue Error: Unsupported date coercion ({:?}) {} ({:?})",
        lhs_type, op, rhs_type
    )));

    if lhs_type.is_date_or_date_time() {
        with_match_date_type!(lhs_type, |$T| {
            with_match_primitive_type!(rhs_type, |$D| {
                BinaryArithmeticFunction::<ArithmeticAdd<$T, $D, $T>>::try_create_func(
                    op.clone(),
                    lhs_type.clone(),
                )
            },{
                if !rhs_type.is_date_or_date_time() {
                    return e;
                }
                with_match_date_type!(rhs_type, |$D| {
                    match op {
                        DataValueArithmeticOperator::Plus => BinaryArithmeticFunction::<ArithmeticAdd<$T, $D, $T>>::try_create_func(
                            op.clone(),
                            lhs_type.clone(),
                        ),
                        DataValueArithmeticOperator::Minus => BinaryArithmeticFunction::<ArithmeticAdd<$T, $D, i32>>::try_create_func(
                            op.clone(),
                            DataType::Int32,
                        ),
                        _ => e,
                    }
                }, e)
            })
        },e)
    } else {
        with_match_primitive_type!(lhs_type, |$T| {
            with_match_date_type!(rhs_type, |$D| {
                BinaryArithmeticFunction::<ArithmeticAdd<$T, $D, $D>>::try_create_func(
                    op.clone(),
                    rhs_type.clone(),
                )
            },e)
        },e)
    }
}
