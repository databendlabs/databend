// Copyright 2020 Datafuse Labs.
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

use std::fmt;
use std::marker::PhantomData;

use common_arrow::arrow::array::Array;
use common_datavalues::chrono::DateTime;
use common_datavalues::chrono::Datelike;
use common_datavalues::chrono::Duration;
use common_datavalues::chrono::NaiveDate;
use common_datavalues::chrono::NaiveDateTime;
use common_datavalues::chrono::Timelike;
use common_datavalues::chrono::Utc;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::Function;

pub trait IntegerTypedArithmetic {
    fn get_func(
        a: &DataType,
        b: &DataType,
    ) -> fn(
        &DataValueArithmeticOperator,
        &DataColumnWithField,
        &DataColumnWithField,
        i64,
    ) -> Result<DataColumn>;
}

#[derive(Clone, Debug)]
pub struct IntegerTypedIntervalFunction<T> {
    t: PhantomData<T>,
    display_name: String,
    factor: i64,
    op: DataValueArithmeticOperator,
}

impl<T> IntegerTypedIntervalFunction<T>
where T: IntegerTypedArithmetic + Clone + Sync + Send + 'static
{
    pub fn try_create(
        display_name: &str,
        op: DataValueArithmeticOperator,
        factor: i64,
    ) -> Result<Box<dyn Function>> {
        Ok(Box::new(IntegerTypedIntervalFunction::<T> {
            t: PhantomData,
            display_name: display_name.to_string(),
            factor,
            op,
        }))
    }
}

impl<T> Function for IntegerTypedIntervalFunction<T>
where T: IntegerTypedArithmetic + Clone + Sync + Send + 'static
{
    fn name(&self) -> &str {
        self.display_name.as_str()
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if is_date_or_date_time(&args[0]) {
            Ok(args[0].clone())
        } else {
            Ok(args[1].clone())
        }
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn num_arguments(&self) -> usize {
        2
    }

    fn eval(&self, columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn> {
        if !(matches!(self.op, DataValueArithmeticOperator::Plus)
            || matches!(self.op, DataValueArithmeticOperator::Minus))
        {
            return Err(ErrorCode::IllegalDataType(format!(
                "Illegal operation {:?} between interval and date time.",
                self.op,
            )));
        }

        let date_col: &DataColumnWithField;
        let integer_col: &DataColumnWithField;
        if is_date_or_date_time(columns[0].data_type()) && is_integer(columns[1].data_type()) {
            date_col = &columns[0];
            integer_col = &columns[1];
        } else if is_date_or_date_time(columns[1].data_type()) && is_integer(columns[0].data_type())
        {
            date_col = &columns[1];
            integer_col = &columns[0];
        } else {
            return Err(ErrorCode::IllegalDataType(format!(
				"Illegal arguments for function {}. Should be a date or dateTime plus or minus an integer.",
				self.name())));
        }

        let f = T::get_func(integer_col.data_type(), date_col.data_type());
        let result = f(&self.op, integer_col, date_col, self.factor)?;

        // cast to date type date16 date32 or datetime32
        let args = columns
            .iter()
            .map(|f| f.data_type().clone())
            .collect::<Vec<_>>();
        let data_type = self.return_type(&args)?;
        result.cast_with_type(&data_type)
    }
}

impl<T> fmt::Display for IntegerTypedIntervalFunction<T>
where T: IntegerTypedArithmetic + Clone + Sync + Send + 'static
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}()", self.display_name)
    }
}

// Implement MonthsArithmetic
#[derive(Clone, Debug)]
pub struct MonthsArithmetic;

impl IntegerTypedArithmetic for MonthsArithmetic {
    fn get_func(a: &DataType, b: &DataType) -> IntegerMonthsArithmeticFunction {
        IntervalFunctionFactory::get_integer_months_arithmetic_func(a, b)
    }
}
pub type MonthsArithmeticFunction = IntegerTypedIntervalFunction<MonthsArithmetic>;

// Implement SecondsArithmetic
#[derive(Clone, Debug)]
pub struct SecondsArithmetic;

impl IntegerTypedArithmetic for SecondsArithmetic {
    fn get_func(a: &DataType, b: &DataType) -> IntegerMonthsArithmeticFunction {
        IntervalFunctionFactory::get_integer_seconds_arithmetic_func(a, b)
    }
}
pub type SecondsArithmeticFunction = IntegerTypedIntervalFunction<SecondsArithmetic>;

//////////////////////////////////////////////////////////////////////////////////////////////

// The function type for handling Interval typed arithmetic operation
pub type IntervalArithmeticFunction = fn(
    &DataValueArithmeticOperator,
    &DataColumnWithField,
    &DataColumnWithField,
) -> Result<DataColumn>;

// The function type for handling arithmetic operation of integer column representing number of months
pub type IntegerMonthsArithmeticFunction = fn(
    &DataValueArithmeticOperator,
    &DataColumnWithField,
    &DataColumnWithField,
    weight: i64, /* for year please pass in 12 */
) -> Result<DataColumn>;

// The function type for handling arithmetic operation of integer column representing number of seconds
pub type IntegerSecondsArithmeticFunction = fn(
    &DataValueArithmeticOperator,
    &DataColumnWithField,
    &DataColumnWithField,
    weight: i64, /* for minute pass in 60, for hour pass in 3600, etc */
) -> Result<DataColumn>;

pub struct IntervalFunctionFactory;

impl IntervalFunctionFactory {
    pub fn try_get_arithmetic_func(
        columns: &DataColumnsWithField,
    ) -> Option<IntervalArithmeticFunction> {
        if columns.len() != 2 {
            return None;
        }

        let mut interval_opt = None;
        let mut date_datetime_opt = None;
        columns.iter().for_each(|column| match column.data_type() {
            DataType::Interval(_) => interval_opt = Some(column),
            DataType::Date16 | DataType::Date32 | DataType::DateTime32(_) => {
                date_datetime_opt = Some(column)
            }
            _ => {}
        });

        if interval_opt.is_none() || date_datetime_opt.is_none() {
            return None;
        }
        Some(Self::get_interval_arithmetic_func(
            interval_opt.unwrap().data_type(),
            date_datetime_opt.unwrap().data_type(),
        ))
    }

    //////////////////////////////////////////////////////////////////////////////////////////////
    //  Starting from here is interval typed arithmetic functions, including:
    //   1. interval_daytime_plus_minus_date -------- Interval(DayTime) +/- Date16/Date32
    //   2. interval_daytime_plus_minus_datetime32 -- Interval(DayTime) +/ DateTime32
    //   3. interval_month_plus_minus_date16 -------- Interval(YearMonth) +/ Date16
    //   4. interval_month_plus_minus_date32 -------- Interval(YearMonth) +/ Date32
    //   5. interval_month_plus_minus_datetime32 ---- Interval(YearMonth) +/ DateTime32

    fn get_interval_arithmetic_func(
        interval: &DataType,
        date_datetime: &DataType,
    ) -> IntervalArithmeticFunction {
        match interval {
            DataType::Interval(IntervalUnit::YearMonth) => match date_datetime {
                DataType::Date16 => Self::interval_month_plus_minus_date16,
                DataType::Date32 => Self::interval_month_plus_minus_date32,
                DataType::DateTime32(_) => Self::interval_month_plus_minus_datetime32,
                _ => unreachable!(),
            },
            DataType::Interval(IntervalUnit::DayTime) => match date_datetime {
                DataType::Date32 | DataType::Date16 => Self::interval_daytime_plus_minus_date,
                DataType::DateTime32(_) => Self::interval_daytime_plus_minus_datetime32,
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }

    fn interval_daytime_plus_minus_date(
        op: &DataValueArithmeticOperator,
        a: &DataColumnWithField,
        b: &DataColumnWithField,
    ) -> Result<DataColumn> {
        let (interval, date) = Self::validate_input(op, a, b)?;
        let milliseconds_per_day = 24 * 3600 * 1000;

        let days: DataColumn = interval
            .column()
            .to_array()?
            .i64()?
            .apply(|ms| ms / milliseconds_per_day)
            .into();
        date.column().arithmetic(op.clone(), &days)
    }

    fn interval_daytime_plus_minus_datetime32(
        op: &DataValueArithmeticOperator,
        a: &DataColumnWithField,
        b: &DataColumnWithField,
    ) -> Result<DataColumn> {
        let (interval, datetime) = Self::validate_input(op, a, b)?;
        let seconds: DataColumn = interval
            .column()
            .to_array()?
            .i64()?
            .apply(|ms| ms / 1000)
            .into();
        datetime.column().arithmetic(op.clone(), &seconds)
    }

    fn interval_month_plus_minus_date16(
        op: &DataValueArithmeticOperator,
        a: &DataColumnWithField,
        b: &DataColumnWithField,
    ) -> Result<DataColumn> {
        let (interval, date16) = Self::validate_input(op, a, b)?;

        // DataType::Interval type is Int64
        Self::month_i64_plus_minus_date16(op, interval, date16, 1)
    }

    fn interval_month_plus_minus_date32(
        op: &DataValueArithmeticOperator,
        a: &DataColumnWithField,
        b: &DataColumnWithField,
    ) -> Result<DataColumn> {
        let (interval, date32) = Self::validate_input(op, a, b)?;

        // DataType::Interval type is Int64
        Self::month_i64_plus_minus_date32(op, interval, date32, 1)
    }

    fn interval_month_plus_minus_datetime32(
        op: &DataValueArithmeticOperator,
        a: &DataColumnWithField,
        b: &DataColumnWithField,
    ) -> Result<DataColumn> {
        let (interval, datetime) = Self::validate_input(op, a, b)?;

        // DataType::Interval type is Int64
        Self::month_i64_plus_minus_datetime32(op, interval, datetime, 1)
    }

    //  End of interval typed arithmetic functions.
    //////////////////////////////////////////////////////////////////////////////////

    //////////////////////////////////////////////////////////////////////////////////
    //  Starting from here is integer month arithmetic functions. It handles plus/minus
    //  operations between integer column and date|datetime column. The integer column
    //  represents number of months.

    pub fn get_integer_months_arithmetic_func(
        integer: &DataType,
        date_datetime: &DataType,
    ) -> IntegerMonthsArithmeticFunction {
        match date_datetime {
            DataType::Date16 => match integer {
                DataType::UInt8 => Self::month_u8_plus_minus_date16,
                DataType::UInt16 => Self::month_u16_plus_minus_date16,
                DataType::UInt32 => Self::month_u32_plus_minus_date16,
                DataType::UInt64 => Self::month_u64_plus_minus_date16,
                DataType::Int8 => Self::month_i8_plus_minus_date16,
                DataType::Int16 => Self::month_i16_plus_minus_date16,
                DataType::Int32 => Self::month_i32_plus_minus_date16,
                DataType::Int64 => Self::month_i64_plus_minus_date16,
                _ => unreachable!(),
            },
            DataType::Date32 => match integer {
                DataType::UInt8 => Self::month_u8_plus_minus_date32,
                DataType::UInt16 => Self::month_u16_plus_minus_date32,
                DataType::UInt32 => Self::month_u32_plus_minus_date32,
                DataType::UInt64 => Self::month_u64_plus_minus_date32,
                DataType::Int8 => Self::month_i8_plus_minus_date32,
                DataType::Int16 => Self::month_i16_plus_minus_date32,
                DataType::Int32 => Self::month_i32_plus_minus_date32,
                DataType::Int64 => Self::month_i64_plus_minus_date32,
                _ => unreachable!(),
            },
            DataType::DateTime32(_) => match integer {
                DataType::UInt8 => Self::month_u8_plus_minus_datetime32,
                DataType::UInt16 => Self::month_u16_plus_minus_datetime32,
                DataType::UInt32 => Self::month_u32_plus_minus_datetime32,
                DataType::UInt64 => Self::month_u64_plus_minus_datetime32,
                DataType::Int8 => Self::month_i8_plus_minus_datetime32,
                DataType::Int16 => Self::month_i16_plus_minus_datetime32,
                DataType::Int32 => Self::month_i32_plus_minus_datetime32,
                DataType::Int64 => Self::month_i64_plus_minus_datetime32,
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }

    crate::define_month_plus_minus_date16!(month_i64_plus_minus_date16, i64);
    crate::define_month_plus_minus_date16!(month_i32_plus_minus_date16, i32);
    crate::define_month_plus_minus_date16!(month_i16_plus_minus_date16, i16);
    crate::define_month_plus_minus_date16!(month_i8_plus_minus_date16, i8);
    crate::define_month_plus_minus_date16!(month_u64_plus_minus_date16, u64);
    crate::define_month_plus_minus_date16!(month_u32_plus_minus_date16, u32);
    crate::define_month_plus_minus_date16!(month_u16_plus_minus_date16, u16);
    crate::define_month_plus_minus_date16!(month_u8_plus_minus_date16, u8);

    crate::define_month_plus_minus_date32!(month_i64_plus_minus_date32, i64);
    crate::define_month_plus_minus_date32!(month_i32_plus_minus_date32, i32);
    crate::define_month_plus_minus_date32!(month_i16_plus_minus_date32, i16);
    crate::define_month_plus_minus_date32!(month_i8_plus_minus_date32, i8);
    crate::define_month_plus_minus_date32!(month_u64_plus_minus_date32, u64);
    crate::define_month_plus_minus_date32!(month_u32_plus_minus_date32, u32);
    crate::define_month_plus_minus_date32!(month_u16_plus_minus_date32, u16);
    crate::define_month_plus_minus_date32!(month_u8_plus_minus_date32, u8);

    crate::define_month_plus_minus_datetime32!(month_i64_plus_minus_datetime32, i64);
    crate::define_month_plus_minus_datetime32!(month_i32_plus_minus_datetime32, i32);
    crate::define_month_plus_minus_datetime32!(month_i16_plus_minus_datetime32, i16);
    crate::define_month_plus_minus_datetime32!(month_i8_plus_minus_datetime32, i8);
    crate::define_month_plus_minus_datetime32!(month_u64_plus_minus_datetime32, u64);
    crate::define_month_plus_minus_datetime32!(month_u32_plus_minus_datetime32, u32);
    crate::define_month_plus_minus_datetime32!(month_u16_plus_minus_datetime32, u16);
    crate::define_month_plus_minus_datetime32!(month_u8_plus_minus_datetime32, u8);

    //  End of months integer arithmetic functions
    //////////////////////////////////////////////////////////////////////////////////

    //////////////////////////////////////////////////////////////////////////////////
    //  Starting from here is integer seconds arithmetic functions. It handles plus/minus
    //  operations between integer column and date|datetime column. The integer column
    //  represents number of seconds.

    pub fn get_integer_seconds_arithmetic_func(
        integer: &DataType,
        date_datetime: &DataType,
    ) -> IntegerSecondsArithmeticFunction {
        match date_datetime {
            DataType::Date16 | DataType::Date32 => match integer {
                DataType::UInt8 => Self::time_secs_u8_plus_minus_date,
                DataType::UInt16 => Self::time_secs_u16_plus_minus_date,
                DataType::UInt32 => Self::time_secs_u32_plus_minus_date,
                DataType::UInt64 => Self::time_secs_u64_plus_minus_date,
                DataType::Int8 => Self::time_secs_i8_plus_minus_date,
                DataType::Int16 => Self::time_secs_i16_plus_minus_date,
                DataType::Int32 => Self::time_secs_i32_plus_minus_date,
                DataType::Int64 => Self::time_secs_i64_plus_minus_date,
                _ => unreachable!(),
            },
            DataType::DateTime32(_) => match integer {
                DataType::UInt8 => Self::time_secs_u8_plus_minus_datetime32,
                DataType::UInt16 => Self::time_secs_u16_plus_minus_datetime32,
                DataType::UInt32 => Self::time_secs_u32_plus_minus_datetime32,
                DataType::UInt64 => Self::time_secs_u64_plus_minus_datetime32,
                DataType::Int8 => Self::time_secs_i8_plus_minus_datetime32,
                DataType::Int16 => Self::time_secs_i16_plus_minus_datetime32,
                DataType::Int32 => Self::time_secs_i32_plus_minus_datetime32,
                DataType::Int64 => Self::time_secs_i64_plus_minus_datetime32,
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }

    crate::define_time_secs_plus_minus_date!(time_secs_i64_plus_minus_date, i64);
    crate::define_time_secs_plus_minus_date!(time_secs_i32_plus_minus_date, i32);
    crate::define_time_secs_plus_minus_date!(time_secs_i16_plus_minus_date, i16);
    crate::define_time_secs_plus_minus_date!(time_secs_i8_plus_minus_date, i8);
    crate::define_time_secs_plus_minus_date!(time_secs_u64_plus_minus_date, u64);
    crate::define_time_secs_plus_minus_date!(time_secs_u32_plus_minus_date, u32);
    crate::define_time_secs_plus_minus_date!(time_secs_u16_plus_minus_date, u16);
    crate::define_time_secs_plus_minus_date!(time_secs_u8_plus_minus_date, u8);

    crate::define_time_secs_plus_minus_datetime32!(time_secs_i64_plus_minus_datetime32, i64);
    crate::define_time_secs_plus_minus_datetime32!(time_secs_i32_plus_minus_datetime32, i32);
    crate::define_time_secs_plus_minus_datetime32!(time_secs_i16_plus_minus_datetime32, i16);
    crate::define_time_secs_plus_minus_datetime32!(time_secs_i8_plus_minus_datetime32, i8);
    crate::define_time_secs_plus_minus_datetime32!(time_secs_u64_plus_minus_datetime32, u64);
    crate::define_time_secs_plus_minus_datetime32!(time_secs_u32_plus_minus_datetime32, u32);
    crate::define_time_secs_plus_minus_datetime32!(time_secs_u16_plus_minus_datetime32, u16);
    crate::define_time_secs_plus_minus_datetime32!(time_secs_u8_plus_minus_datetime32, u8);

    // End of seconds integer arithmetic functions
    //////////////////////////////////////////////////////////////////////////////////

    // A private helper function for validate operator, returns a tuple of
    // (interval|integer, date16|date32|datetime32)
    fn validate_input<'a>(
        op: &DataValueArithmeticOperator,
        col0: &'a DataColumnWithField,
        col1: &'a DataColumnWithField,
    ) -> Result<(&'a DataColumnWithField, &'a DataColumnWithField)> {
        match op {
            DataValueArithmeticOperator::Plus | DataValueArithmeticOperator::Minus => {
                if is_integer(col0.data_type()) || matches!(col0.data_type(), DataType::Interval(_))
                {
                    Ok((col0, col1))
                } else {
                    Ok((col1, col0))
                }
            }
            _ => Result::Err(ErrorCode::IllegalDataType(format!(
                "Illegal operation {:?} between interval and date time.",
                op
            ))),
        }
    }

    // A private helper function to add/subtract month to/from days
    fn days_plus_signed_months(days: i64, months: i64) -> Result<u32> {
        let naive = NaiveDateTime::from_timestamp(0, 0).checked_add_signed(Duration::days(days));
        if naive.is_none() {
            return Err(ErrorCode::Overflow(format!(
                "Overflow on date with days {}.",
                days,
            )));
        }
        let dt = DateTime::<Utc>::from_utc(naive.unwrap(), Utc);
        let dt = Self::datetime_plus_signed_months(&dt, months)?;
        let seconds_per_day = 24 * 3600;
        Ok((dt.timestamp() / seconds_per_day) as u32)
    }

    // A private helper function to add/subtract month to/from chrono datetime object
    fn datetime_plus_signed_months(dt: &DateTime<Utc>, months: i64) -> Result<DateTime<Utc>> {
        let total_months = (dt.month0() as i64) + months;
        let mut new_year = dt.year() + (total_months / 12) as i32;
        let mut new_month0 = total_months % 12;
        if new_month0 < 0 {
            new_year -= 1;
            new_month0 += 12;
        }

        let (_y, _m, d, h, m, s) = (
            dt.year(),
            dt.month(),
            dt.day(),
            dt.hour(),
            dt.minute(),
            dt.second(),
        );

        // Handle month last day overflow, "2020-2-29" + "1 year" should be "2021-2-28", or "1990-1-31" + "3 month" should be "1990-4-30".
        let new_day = std::cmp::min::<u32>(
            d,
            Self::last_day_of_year_month(new_year, (new_month0 + 1) as u32),
        );

        let new_date = NaiveDate::from_ymd_opt(new_year, (new_month0 + 1) as u32, new_day);
        if new_date.is_none() {
            return Err(ErrorCode::Overflow(format!(
                "Overflow on date YMD {}-{}-{}.",
                new_year,
                new_month0 + 1,
                new_day
            )));
        }
        Ok(DateTime::<Utc>::from_utc(
            new_date.unwrap().and_hms(h, m, s),
            Utc,
        ))
    }

    // Get the last day of the year month, could be 28(non leap Feb), 29(leap year Feb), 30 or 31
    fn last_day_of_year_month(year: i32, month: u32) -> u32 {
        let is_leap_year = NaiveDate::from_ymd_opt(year, 2, 29).is_some();
        if month == 2 && is_leap_year {
            return 29;
        }
        let last_day_lookup = [0u32, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
        last_day_lookup[month as usize]
    }

    // A private helper function to convert seconds (since Unix epoch) to chrono DateTime
    fn seconds_to_datetime(seconds: i64) -> Result<DateTime<Utc>> {
        let naive = NaiveDateTime::from_timestamp_opt(seconds, 0);
        if naive.is_none() {
            return Err(ErrorCode::Overflow(format!(
                "Overflow on datetime with seconds {}.",
                seconds
            )));
        }
        Ok(DateTime::<Utc>::from_utc(naive.unwrap(), Utc))
    }
}

#[macro_export]
macro_rules! define_month_plus_minus_datetime32 {
    ($fn_name:ident, $type:ident) => {
        fn $fn_name(
            op: &DataValueArithmeticOperator,
            a: &DataColumnWithField,
            b: &DataColumnWithField,
            mul: i64,
        ) -> Result<DataColumn> {
            let (interval_month, datetime) = Self::validate_input(op, a, b)?;

            let timestamps = datetime
                .column()
                .to_array()?
                .u32()?
                .into_no_null_iter()
                .zip(
                    interval_month
                        .column()
                        .to_array()?
                        .$type()?
                        .into_no_null_iter(),
                )
                .map(|(seconds, months)| {
                    let dt = Self::seconds_to_datetime(*seconds as i64)?;
                    let new_dt = match op {
                        DataValueArithmeticOperator::Plus => {
                            Self::datetime_plus_signed_months(&dt, (*months as i64) * mul)?
                        }
                        DataValueArithmeticOperator::Minus => {
                            Self::datetime_plus_signed_months(&dt, -(*months as i64) * mul)?
                        }
                        _ => unreachable!(),
                    };
                    Ok(new_dt.timestamp() as u32)
                })
                .collect::<Result<AlignedVec<u32>>>()?;

            let validity = combine_validities(
                datetime.column().to_array()?.u32()?.inner().validity(),
                interval_month
                    .column()
                    .to_array()?
                    .$type()?
                    .inner()
                    .validity(),
            );
            Ok(DFUInt32Array::new_from_owned_with_null_bitmap(timestamps, validity).into())
        }
    };
}

#[macro_export]
macro_rules! define_month_plus_minus_date16 {
    ($fn_name:ident, $type:ident) => {
        fn $fn_name(
            op: &DataValueArithmeticOperator,
            interval_month: &DataColumnWithField,
            date16: &DataColumnWithField,
            mul: i64,
        ) -> Result<DataColumn> {
            let days = date16
                .column()
                .to_array()?
                .u16()?
                .into_no_null_iter()
                .zip(
                    interval_month
                        .column()
                        .to_array()?
                        .$type()?
                        .into_no_null_iter(),
                )
                .map(|(days, months)| match op {
                    DataValueArithmeticOperator::Plus => {
                        Self::days_plus_signed_months(*days as i64, (*months as i64) * mul)
                    }
                    DataValueArithmeticOperator::Minus => {
                        Self::days_plus_signed_months(*days as i64, -(*months as i64) * mul)
                    }
                    _ => unreachable!(),
                })
                .collect::<Result<AlignedVec<u32>>>()?;
            let validity = combine_validities(
                date16.column().to_array()?.u16()?.inner().validity(),
                interval_month
                    .column()
                    .to_array()?
                    .$type()?
                    .inner()
                    .validity(),
            );
            Ok(DFUInt32Array::new_from_owned_with_null_bitmap(days, validity).into())
        }
    };
}

#[macro_export]
macro_rules! define_month_plus_minus_date32 {
    ($fn_name:ident, $type:ident) => {
        fn $fn_name(
            op: &DataValueArithmeticOperator,
            interval_month: &DataColumnWithField,
            date32: &DataColumnWithField,
            mul: i64,
        ) -> Result<DataColumn> {
            let days = date32
                .column()
                .to_array()?
                .u32()?
                .into_no_null_iter()
                .zip(
                    interval_month
                        .column()
                        .to_array()?
                        .$type()?
                        .into_no_null_iter(),
                )
                .map(|(days, months)| match op {
                    DataValueArithmeticOperator::Plus => {
                        Self::days_plus_signed_months(*days as i64, (*months as i64) * mul)
                    }
                    DataValueArithmeticOperator::Minus => {
                        Self::days_plus_signed_months(*days as i64, -(*months as i64) * mul)
                    }
                    _ => unreachable!(),
                })
                .collect::<Result<AlignedVec<u32>>>()?;
            let validity = combine_validities(
                date32.column().to_array()?.u32()?.inner().validity(),
                interval_month
                    .column()
                    .to_array()?
                    .$type()?
                    .inner()
                    .validity(),
            );
            Ok(DFUInt32Array::new_from_owned_with_null_bitmap(days, validity).into())
        }
    };
}

#[macro_export]
macro_rules! define_time_secs_plus_minus_datetime32 {
    ($fn_name:ident, $type:ident) => {
        fn $fn_name(
            op: &DataValueArithmeticOperator,
            interval: &DataColumnWithField,
            datetime: &DataColumnWithField,
            mul: i64,
        ) -> Result<DataColumn> {
            let seconds: DataColumn = interval
                .column()
                .to_array()?
                .$type()?
                .apply_cast_numeric(|seconds| (seconds as i64) * mul)
                .into();
            datetime.column().arithmetic(op.clone(), &seconds)
        }
    };
}

#[macro_export]
macro_rules! define_time_secs_plus_minus_date {
    ($fn_name:ident, $type:ident) => {
        fn $fn_name(
            op: &DataValueArithmeticOperator,
            interval: &DataColumnWithField,
            date: &DataColumnWithField,
            mul: i64,
        ) -> Result<DataColumn> {
            let seconds_per_day = 24 * 3600_i64;

            let days: DataColumn = interval
                .column()
                .to_array()?
                .$type()?
                .apply_cast_numeric(|seconds| (seconds as i64) * mul / seconds_per_day)
                .into();
            date.column().arithmetic(op.clone(), &days)
        }
    };
}
