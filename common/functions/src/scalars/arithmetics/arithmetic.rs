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

use common_datavalues::chrono::*;
use common_datavalues::columns::DataColumn;
use common_datavalues::prelude::*;
use common_datavalues::DataSchema;
use common_datavalues::DataValueArithmeticOperator;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::ArithmeticDivFunction;
use crate::scalars::ArithmeticMinusFunction;
use crate::scalars::ArithmeticModuloFunction;
use crate::scalars::ArithmeticMulFunction;
use crate::scalars::ArithmeticPlusFunction;
use crate::scalars::FactoryFuncRef;
use crate::scalars::Function;

#[derive(Clone)]
pub struct ArithmeticFunction {
    op: DataValueArithmeticOperator,
}

impl ArithmeticFunction {
    pub fn register(map: FactoryFuncRef) -> Result<()> {
        let mut map = map.write();
        map.insert("+".into(), ArithmeticPlusFunction::try_create_func);
        map.insert("plus".into(), ArithmeticPlusFunction::try_create_func);
        map.insert("-".into(), ArithmeticMinusFunction::try_create_func);
        map.insert("minus".into(), ArithmeticMinusFunction::try_create_func);
        map.insert("*".into(), ArithmeticMulFunction::try_create_func);
        map.insert("multiply".into(), ArithmeticMulFunction::try_create_func);
        map.insert("/".into(), ArithmeticDivFunction::try_create_func);
        map.insert("divide".into(), ArithmeticDivFunction::try_create_func);
        map.insert("%".into(), ArithmeticModuloFunction::try_create_func);
        map.insert("modulo".into(), ArithmeticModuloFunction::try_create_func);
        Ok(())
    }

    pub fn try_create_func(op: DataValueArithmeticOperator) -> Result<Box<dyn Function>> {
        Ok(Box::new(ArithmeticFunction { op }))
    }
}

impl Function for ArithmeticFunction {
    fn name(&self) -> &str {
        "ArithmeticFunction"
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if args.len() == 1 {
            return numerical_unary_arithmetic_coercion(&self.op, &args[0]);
        }

        if is_interval(&args[0]) || is_interval(&args[1]) {
            return interval_arithmetic_coercion(&self.op, &args[0], &args[1]);
        }
        if is_date_or_date_time(&args[0]) || is_date_or_date_time(&args[1]) {
            return datetime_arithmetic_coercion(&self.op, &args[0], &args[1]);
        }
        numerical_arithmetic_coercion(&self.op, &args[0], &args[1])
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn> {
        let result: DataColumn = {
            // Some logic type need DateType information, try arithmetic on column with field first.
            if let Some(r) = self.try_evaluate_on_column_field(columns) {
                r?
            } else {
                match columns.len() {
                    1 => columns[0].column().unary_arithmetic(self.op.clone()),
                    _ => columns[0]
                        .column()
                        .arithmetic(self.op.clone(), columns[1].column()),
                }?
            }
        };

        let has_date_or_date_time = columns.iter().any(|c| is_date_or_date_time(c.data_type()));

        if has_date_or_date_time {
            let args = columns
                .iter()
                .map(|f| f.data_type().clone())
                .collect::<Vec<_>>();
            let data_type = self.return_type(&args)?;
            result.cast_with_type(&data_type)
        } else {
            Ok(result)
        }
    }

    fn num_arguments(&self) -> usize {
        0
    }

    fn variadic_arguments(&self) -> Option<(usize, usize)> {
        Some((1, 2))
    }
}

impl fmt::Display for ArithmeticFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.op)
    }
}

impl ArithmeticFunction {
    // This is an arithmetic support for DataColumnWithField. Maybe we should move it into to "impl DataColumnWithField",
    // thus we can do it like "columns[0].arithmetic(columns[1])".
    // Currently only apply for Plus/Minus operation between Date/DateTime and Interval
    pub fn try_evaluate_on_column_field(
        &self,
        columns: &DataColumnsWithField,
    ) -> Option<Result<DataColumn>> {
        if columns.len() != 2 {
            return None;
        }

        match (columns[0].data_type(), columns[1].data_type()) {
            (&DataType::Interval(_), &DataType::Date16)
            | (&DataType::Interval(_), &DataType::Date32)
            | (&DataType::Interval(_), &DataType::DateTime32(_))
            | (&DataType::Date16, &DataType::Interval(_))
            | (&DataType::Date32, &DataType::Interval(_))
            | (&DataType::DateTime32(_), &DataType::Interval(_)) => match self.op {
                DataValueArithmeticOperator::Plus | DataValueArithmeticOperator::Minus => {
                    Some(self.datetime_plus_minus_interval(columns))
                }
                _ => None,
            },
            _ => None,
        }
    }

    pub fn datetime_plus_minus_interval(
        &self,
        columns: &DataColumnsWithField,
    ) -> Result<DataColumn> {
        let days_to_datetime = |days: i64| -> Result<DateTime<Utc>> {
            let naive =
                NaiveDateTime::from_timestamp(0, 0).checked_add_signed(Duration::days(days));
            if naive.is_none() {
                return Err(ErrorCode::Overflow(format!(
                    "Overflow on date with days {}.",
                    days,
                )));
            }
            Ok(DateTime::<Utc>::from_utc(naive.unwrap(), Utc))
        };

        let seconds_to_datetime = |seconds: i64| -> Result<DateTime<Utc>> {
            let naive = NaiveDateTime::from_timestamp_opt(seconds, 0);
            if naive.is_none() {
                return Err(ErrorCode::Overflow(format!(
                    "Overflow on datetime with seconds {}.",
                    seconds
                )));
            }
            Ok(DateTime::<Utc>::from_utc(naive.unwrap(), Utc))
        };

        let (interval_column, date_column) = match columns[0].data_type() {
            DataType::Interval(_) => (&columns[0], &columns[1]),
            _ => (&columns[1], &columns[0]),
        };

        let interval_series = interval_column.column().to_array()?;
        let date_series = date_column.column().to_array()?;
        let len = date_series.len();
        let mut dt_vec = Vec::<DateTime<Utc>>::with_capacity(len);

        for i in 0..len {
            // Convert Date16, Date32 or DateTime32 to chrono::DateTime
            let date = date_series.try_get(i)?;
            let dt = match date_column.data_type() {
                DataType::DateTime32(_) => {
                    let seconds = date.as_i64()?;
                    seconds_to_datetime(seconds)
                }
                DataType::Date32 | DataType::Date16 => {
                    let days = date.as_u64()?;
                    days_to_datetime(days as i64)
                }
                _ => unreachable!(),
            }?;

            // Add interval to DateTime, the interval could be YearMonth or DayTime
            let interval = interval_series.try_get(i)?.as_i64()?;
            let new_dt = match interval_column.data_type() {
                DataType::Interval(IntervalUnit::YearMonth) => {
                    let months = match self.op {
                        DataValueArithmeticOperator::Plus => interval,
                        DataValueArithmeticOperator::Minus => -interval,
                        _ => unreachable!(),
                    };
                    Self::datetime_add_signed_months(&dt, months)?
                }
                DataType::Interval(IntervalUnit::DayTime) => {
                    let mut days: i64 = interval.abs() >> 32; //higher 32 bits as number of days
                    let mut milliseconds: i64 = interval.abs() & 0x0000_0000_FFFF_FFFF; //lower 32 bits as milliseconds
                    if interval < 0 {
                        days = -days;
                        milliseconds = -milliseconds;
                    }

                    let updated = match self.op {
                        DataValueArithmeticOperator::Plus => dt.checked_add_signed(
                            Duration::days(days) + Duration::milliseconds(milliseconds as i64),
                        ),
                        DataValueArithmeticOperator::Minus => dt.checked_sub_signed(
                            Duration::days(days) + Duration::milliseconds(milliseconds as i64),
                        ),
                        _ => unreachable!(),
                    };
                    if updated.is_none() {
                        return Err(ErrorCode::Overflow(format!(
                            "Overflow on datetime with days {}, milliseconds {}.",
                            days, milliseconds
                        )));
                    }
                    updated.unwrap()
                }
                _ => unreachable!(),
            };

            dt_vec.push(new_dt);
        }

        match date_column.data_type() {
            DataType::Date16 | DataType::Date32 => {
                // convert datetime to elapsed days with UInt32 type
                let arr = DFUInt32Array::new_from_iter(
                    dt_vec
                        .iter()
                        .map(|dt| (dt.timestamp() / (24 * 3600)) as u32),
                );
                Ok(arr.into())
            }
            DataType::DateTime32(_) => {
                // convert datetime to elapsed seconds with UInt32 type
                let arr =
                    DFUInt32Array::new_from_iter(dt_vec.iter().map(|dt| dt.timestamp() as u32));
                Ok(arr.into())
            }
            _ => unreachable!(),
        }
    }

    fn datetime_add_signed_months(dt: &DateTime<Utc>, months: i64) -> Result<DateTime<Utc>> {
        let total_months = (dt.month() as i64) + months;
        let mut new_year = dt.year() + (total_months / 12) as i32;
        let mut new_month = total_months % 12;
        if new_month < 0 {
            new_year -= 1;
            new_month += 12;
        }

        let (_y, _m, d, h, m, s) = (
            dt.year(),
            dt.month(),
            dt.day(),
            dt.hour(),
            dt.minute(),
            dt.second(),
        );

        // Handle month last day overflow, "2020-2-29" + "1 year" should be "2020-2-28", or "1990-1-31" + "3 month" should be "1990-4-31".
        let new_day =
            std::cmp::min::<u32>(d, Self::last_day_of_year_month(new_year, new_month as u32));

        let new_date = NaiveDate::from_ymd_opt(new_year, new_month as u32, new_day);
        if new_date.is_none() {
            return Err(ErrorCode::Overflow(format!(
                "Overflow on date YMD {}-{}-{}.",
                new_year, new_month, new_day
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
}
