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

use common_datavalues::prelude::*;
use common_exception::Result;
use num::cast::AsPrimitive;

use super::arithmetic::ArithmeticTrait;
use super::utils::validate_input;
use crate::binary_arithmetic_helper;
use crate::interval_arithmetic;
use crate::scalars::dates::IntervalFunctionFactory;

// Interval(DayTime) + Date16
#[derive(Clone)]
pub struct IntervalDaytimeAddDate16 {}

impl ArithmeticTrait for IntervalDaytimeAddDate16 {
    fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn> {
        let milliseconds_per_day = 24 * 3600 * 1000;
        interval_arithmetic! {&columns[0], &columns[1], u16, |l: i64, r: i64| (l + r/milliseconds_per_day) as u16}
    }
}

// Interval(DayTime) + Date32
#[derive(Clone)]
pub struct IntervalDaytimeAddDate32 {}

impl ArithmeticTrait for IntervalDaytimeAddDate32 {
    fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn> {
        let milliseconds_per_day = 24 * 3600 * 1000;
        interval_arithmetic! {&columns[0], &columns[1], i32, |l: i64, r: i64| (l + r/milliseconds_per_day) as i32}
    }
}

// Interval(DayTime) + DateTime32
#[derive(Clone)]
pub struct IntervalDaytimeAddDatetime32 {}

impl ArithmeticTrait for IntervalDaytimeAddDatetime32 {
    fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn> {
        let div = 1000;
        interval_arithmetic! {&columns[0], &columns[1], u32, |l: i64, r: i64| (l + r/div) as u32}
    }
}

// Interval(YearMonth) + Date16
#[derive(Clone)]
pub struct IntervalMonthAddDate16 {}

impl ArithmeticTrait for IntervalMonthAddDate16 {
    fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn> {
        IntervalFunctionFactory::interval_month_plus_minus_date16(
            &DataValueBinaryOperator::Plus,
            &columns[0],
            &columns[1],
        )
    }
}

// Interval(YearMonth) + Date32
#[derive(Clone)]
pub struct IntervalMonthAddDate32 {}

impl ArithmeticTrait for IntervalMonthAddDate32 {
    fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn> {
        IntervalFunctionFactory::interval_month_plus_minus_date32(
            &DataValueBinaryOperator::Plus,
            &columns[0],
            &columns[1],
        )
    }
}

// Interval(YearMonth) + DateTime32
#[derive(Clone)]
pub struct IntervalMonthAddDatetime32 {}

impl ArithmeticTrait for IntervalMonthAddDatetime32 {
    fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn> {
        IntervalFunctionFactory::interval_month_plus_minus_datetime32(
            &DataValueBinaryOperator::Plus,
            &columns[0],
            &columns[1],
        )
    }
}

// Interval(DayTime) - Date16
#[derive(Clone)]
pub struct IntervalDaytimeSubDate16 {}

impl ArithmeticTrait for IntervalDaytimeSubDate16 {
    fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn> {
        let milliseconds_per_day = 24 * 3600 * 1000;
        interval_arithmetic! {&columns[0], &columns[1], u16, |l: i64, r: i64| (l - r/milliseconds_per_day) as u16}
    }
}

// Interval(DayTime) - Date32
#[derive(Clone)]
pub struct IntervalDaytimeSubDate32 {}

impl ArithmeticTrait for IntervalDaytimeSubDate32 {
    fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn> {
        let milliseconds_per_day = 24 * 3600 * 1000;
        interval_arithmetic! {&columns[0], &columns[1], i32, |l: i64, r: i64| (l - r/milliseconds_per_day) as i32}
    }
}

// Interval(DayTime) - DateTime32
#[derive(Clone)]
pub struct IntervalDaytimeSubDatetime32 {}

impl ArithmeticTrait for IntervalDaytimeSubDatetime32 {
    fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn> {
        let div = 1000;
        interval_arithmetic! {&columns[0], &columns[1], u32, |l: i64, r: i64| (l - r/div) as u32}
    }
}

// Interval(YearMonth) - Date16
#[derive(Clone)]
pub struct IntervalMonthSubDate16 {}

impl ArithmeticTrait for IntervalMonthSubDate16 {
    fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn> {
        IntervalFunctionFactory::interval_month_plus_minus_date16(
            &DataValueBinaryOperator::Minus,
            &columns[0],
            &columns[1],
        )
    }
}

// Interval(YearMonth) - Date32
#[derive(Clone)]
pub struct IntervalMonthSubDate32 {}

impl ArithmeticTrait for IntervalMonthSubDate32 {
    fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn> {
        IntervalFunctionFactory::interval_month_plus_minus_date32(
            &DataValueBinaryOperator::Minus,
            &columns[0],
            &columns[1],
        )
    }
}

// Interval(YearMonth) - DateTime32
#[derive(Clone)]
pub struct IntervalMonthSubDatetime32 {}

impl ArithmeticTrait for IntervalMonthSubDatetime32 {
    fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn> {
        IntervalFunctionFactory::interval_month_plus_minus_datetime32(
            &DataValueBinaryOperator::Minus,
            &columns[0],
            &columns[1],
        )
    }
}
