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

use crate::scalars::dates::IntervalFunctionFactory;

#[macro_export]
macro_rules! interval_arithmetic {
    ($self: expr, $rhs: expr, $R:ty, $op: expr) => {{
        let (interval, datetime) = validate_input($self, $rhs);
        let result: DataColumn = match (datetime.column(), interval.column()) {
            (DataColumn::Array(left), DataColumn::Array(right)) => {
                let lhs: &DFPrimitiveArray<$R> = left.static_cast();
                let rhs = right.i64()?;
                binary(lhs, rhs, |l, r| $op(l.as_(), r)).into()
            }
            (DataColumn::Array(left), DataColumn::Constant(right, _)) => {
                let lhs: &DFPrimitiveArray<$R> = left.static_cast();
                let r: i64 = DFTryFrom::try_from(right.clone()).unwrap_or(0i64);
                unary(lhs, |l| $op(l.as_(), r)).into()
            }
            (DataColumn::Constant(left, _), DataColumn::Array(right)) => {
                let lhs: $R = DFTryFrom::try_from(left.clone()).unwrap_or(<$R>::default());
                let l: i64 = lhs.as_();
                let rhs = right.i64()?;
                unary(rhs, |r| $op(l, r)).into()
            }
            (DataColumn::Constant(left, size), DataColumn::Constant(right, _)) => {
                let l: $R = DFTryFrom::try_from(left.clone()).unwrap_or(<$R>::default());
                let r: i64 = DFTryFrom::try_from(right.clone()).unwrap_or(0i64);
                DataColumn::Constant($op(l.as_(), r).into(), size.clone())
            }
        };

        Ok(result)
    }};
}

pub trait ArithmeticTrait {
    fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn>;
}

pub fn validate_input<'a>(
    col0: &'a DataColumnWithField,
    col1: &'a DataColumnWithField,
) -> (&'a DataColumnWithField, &'a DataColumnWithField) {
    if col0.data_type().is_integer() || col0.data_type().is_interval() {
        (col0, col1)
    } else {
        (col1, col0)
    }
}

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
