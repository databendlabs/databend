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
        if let Some((interval, date_datetime)) = self.check_interval_date_columns(columns) {
            let col = match interval.data_type() {
                DataType::Interval(IntervalUnit::YearMonth) => match date_datetime.data_type() {
                    DataType::DateTime32(_) => {
                        self.datetime_plus_minus_year_month(date_datetime, interval)
                    }
                    DataType::Date16 | DataType::Date32 => {
                        self.date_plus_minus_year_month(date_datetime, interval)
                    }
                    _ => unreachable!(),
                },
                DataType::Interval(IntervalUnit::DayTime) => match date_datetime.data_type() {
                    DataType::DateTime32(_) => {
                        self.datetime_plus_minus_day_time(date_datetime, interval)
                    }
                    DataType::Date16 | DataType::Date32 => {
                        self.date_plus_minus_day_time(date_datetime, interval)
                    }
                    _ => unreachable!(),
                },
                _ => unreachable!(),
            };
            Some(col)
        } else {
            None
        }
    }

    pub fn check_interval_date_columns<'a>(
        &self,
        columns: &'a DataColumnsWithField,
    ) -> Option<(&'a DataColumnWithField, &'a DataColumnWithField)> {
        if columns.len() != 2
            || !matches!(
                self.op,
                DataValueArithmeticOperator::Plus | DataValueArithmeticOperator::Minus
            )
        {
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

        Some((interval_opt.unwrap(), date_datetime_opt.unwrap()))
    }

    pub fn date_plus_minus_day_time(
        &self,
        date: &DataColumnWithField,
        daytime: &DataColumnWithField,
    ) -> Result<DataColumn> {
        let days: DataColumn = daytime
            .column()
            .to_array()?
            .i64()?
            .apply(|v| {
                // Right shift on negative value is implementation-defined. For example -1_i64 >> 40 will return -1, not zero.
                // In case of a negative value, do the division instead of right shift.
                v / 0x1_0000_0000_i64
                // Ignore the lower bits because the parser layer should make sure the milliseconds parts(lower bits) not exceed 24 hours.
            })
            .into();
        date.column().arithmetic(self.op.clone(), &days)
    }

    pub fn datetime_plus_minus_day_time(
        &self,
        datetime: &DataColumnWithField,
        daytime: &DataColumnWithField,
    ) -> Result<DataColumn> {
        let seconds_per_day = 24 * 3600_i64;
        let seconds: DataColumn = daytime
            .column()
            .to_array()?
            .i64()?
            .apply(|v| {
                let secs = (v / 0x1_0000_0000_i64) * seconds_per_day;
                secs + ((v as i32) / 1000) as i64
            })
            .into();
        datetime.column().arithmetic(self.op.clone(), &seconds)
    }

    pub fn datetime_plus_minus_year_month(
        &self,
        datetime: &DataColumnWithField,
        year_month: &DataColumnWithField,
    ) -> Result<DataColumn> {
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

        let timestamps = datetime
            .column()
            .to_array()?
            .u32()?
            .into_no_null_iter()
            .zip(year_month.column().to_array()?.i64()?.into_no_null_iter())
            .map(|(seconds, months)| {
                let dt = seconds_to_datetime(*seconds as i64)?;
                let new_dt = match self.op {
                    DataValueArithmeticOperator::Plus => {
                        Self::datetime_plus_signed_months(&dt, *months)?
                    }
                    DataValueArithmeticOperator::Minus => {
                        Self::datetime_plus_signed_months(&dt, -*months)?
                    }
                    _ => unreachable!(),
                };
                Ok(new_dt.timestamp() as u32)
            })
            .collect::<Result<Vec<u32>>>()?;

        Ok(DFUInt32Array::new_from_iter(timestamps.into_iter()).into())
    }

    fn date_plus_minus_year_month(
        &self,
        date: &DataColumnWithField,
        year_month: &DataColumnWithField,
    ) -> Result<DataColumn> {
        let date16_plus_minus_months =
            |date: &DataColumnWithField, year_month: &DataColumnWithField| -> Result<DataColumn> {
                let days = date
                    .column()
                    .to_array()?
                    .u16()?
                    .into_no_null_iter()
                    .zip(year_month.column().to_array()?.i64()?.into_no_null_iter())
                    .map(|(days, months)| self.days_plus_minus_months(*days as i64, *months))
                    .collect::<Result<Vec<u32>>>()?;
                Ok(DFUInt32Array::new_from_iter(days.into_iter()).into())
            };

        let date32_plus_minus_months =
            |date: &DataColumnWithField, year_month: &DataColumnWithField| -> Result<DataColumn> {
                let days = date
                    .column()
                    .to_array()?
                    .u32()?
                    .into_no_null_iter()
                    .zip(year_month.column().to_array()?.i64()?.into_no_null_iter())
                    .map(|(days, months)| self.days_plus_minus_months(*days as i64, *months))
                    .collect::<Result<Vec<u32>>>()?;
                Ok(DFUInt32Array::new_from_iter(days.into_iter()).into())
            };

        match date.data_type() {
            DataType::Date16 => date16_plus_minus_months(date, year_month),
            DataType::Date32 => date32_plus_minus_months(date, year_month),
            _ => unreachable!(),
        }
    }

    fn days_plus_minus_months(&self, days: i64, months: i64) -> Result<u32> {
        let naive = NaiveDateTime::from_timestamp(0, 0).checked_add_signed(Duration::days(days));
        if naive.is_none() {
            return Err(ErrorCode::Overflow(format!(
                "Overflow on date with days {}.",
                days,
            )));
        }
        let dt = DateTime::<Utc>::from_utc(naive.unwrap(), Utc);

        let seconds_per_day = 24 * 3600;
        match self.op {
            DataValueArithmeticOperator::Plus => {
                let dt = Self::datetime_plus_signed_months(&dt, months)?;
                Ok((dt.timestamp() / seconds_per_day) as u32)
            }
            _ => {
                let dt = Self::datetime_plus_signed_months(&dt, -months)?;
                Ok((dt.timestamp() / seconds_per_day) as u32)
            }
        }
    }

    fn datetime_plus_signed_months(dt: &DateTime<Utc>, months: i64) -> Result<DateTime<Utc>> {
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

        // Handle month last day overflow, "2020-2-29" + "1 year" should be "2020-2-28", or "1990-1-31" + "3 month" should be "1990-4-30".
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
