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

use common_datavalues::chrono::DateTime;
use common_datavalues::chrono::Datelike;
use common_datavalues::chrono::TimeZone;
use common_datavalues::chrono::Timelike;
use common_datavalues::chrono::Utc;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::Function;

#[derive(Clone, Debug)]
pub struct NumberFunction<T, R> {
    display_name: String,
    t: PhantomData<T>,
    r: PhantomData<R>,
}

pub trait NumberResultFunction<R> {
    fn return_type() -> Result<DataType>;
    fn to_number(_value: DateTime<Utc>) -> R;
    fn to_constant_value(_value: DateTime<Utc>) -> DataValue;
}

#[derive(Clone)]
pub struct ToYYYYMM;

impl NumberResultFunction<u32> for ToYYYYMM {
    fn return_type() -> Result<DataType> {
        Ok(DataType::UInt32)
    }
    fn to_number(value: DateTime<Utc>) -> u32 {
        value.year() as u32 * 100 + value.month()
    }

    fn to_constant_value(value: DateTime<Utc>) -> DataValue {
        DataValue::UInt32(Some(Self::to_number(value)))
    }
}

#[derive(Clone)]
pub struct ToYYYYMMDD;

impl NumberResultFunction<u32> for ToYYYYMMDD {
    fn return_type() -> Result<DataType> {
        Ok(DataType::UInt32)
    }
    fn to_number(value: DateTime<Utc>) -> u32 {
        value.year() as u32 * 10000 + value.month() * 100 + value.day()
    }

    fn to_constant_value(value: DateTime<Utc>) -> DataValue {
        DataValue::UInt32(Some(Self::to_number(value)))
    }
}

#[derive(Clone)]
pub struct ToYYYYMMDDhhmmss;

impl NumberResultFunction<u64> for ToYYYYMMDDhhmmss {
    fn return_type() -> Result<DataType> {
        Ok(DataType::UInt64)
    }

    fn to_number(value: DateTime<Utc>) -> u64 {
        value.year() as u64 * 10000000000
            + value.month() as u64 * 100000000
            + value.day() as u64 * 1000000
            + value.hour() as u64 * 10000
            + value.minute() as u64 * 100
            + value.second() as u64
    }

    fn to_constant_value(value: DateTime<Utc>) -> DataValue {
        DataValue::UInt64(Some(Self::to_number(value)))
    }
}

#[derive(Clone)]
pub struct ToStartOfYear;

impl NumberResultFunction<u32> for ToStartOfYear {
    fn return_type() -> Result<DataType> {
        Ok(DataType::Date16)
    }
    fn to_number(value: DateTime<Utc>) -> u32 {
        let start: DateTime<Utc> = Utc.ymd(1970, 1, 1).and_hms(0, 0, 0);
        let end: DateTime<Utc> = Utc.ymd(value.year(), 1, 1).and_hms(0, 0, 0);
        let duration = end.signed_duration_since(start);
        duration.num_days() as u32
    }

    fn to_constant_value(value: DateTime<Utc>) -> DataValue {
        DataValue::UInt16(Some(Self::to_number(value) as u16))
    }
}

#[derive(Clone)]
pub struct ToStartOfISOYear;

impl NumberResultFunction<u32> for ToStartOfISOYear {
    fn return_type() -> Result<DataType> {
        Ok(DataType::Date16)
    }
    fn to_number(value: DateTime<Utc>) -> u32 {
        let start: DateTime<Utc> = Utc.ymd(1970, 1, 1).and_hms(0, 0, 0);
        let week_day = value.weekday().num_days_from_monday();
        let iso_week = value.iso_week();
        let iso_week_num = iso_week.week();
        let sub_days = (iso_week_num - 1) * 7 + week_day;
        let result = value.timestamp_millis() - sub_days as i64 * 24 * 3600 * 1000;
        let end: DateTime<Utc> = Utc.timestamp_millis(result);
        let duration = end.signed_duration_since(start);
        duration.num_days() as u32
    }

    fn to_constant_value(value: DateTime<Utc>) -> DataValue {
        DataValue::UInt16(Some(Self::to_number(value) as u16))
    }
}

impl<T, R> NumberFunction<T, R>
where
    T: NumberResultFunction<R> + Clone + Sync + Send + 'static,
    R: DFPrimitiveType + Clone,
    DFPrimitiveArray<R>: IntoSeries,
{
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(NumberFunction::<T, R> {
            display_name: display_name.to_string(),
            t: PhantomData,
            r: PhantomData,
        }))
    }
}

impl<T, R> Function for NumberFunction<T, R>
where
    T: NumberResultFunction<R> + Clone + Sync + Send,
    R: DFPrimitiveType + Clone,
    DFPrimitiveArray<R>: IntoSeries,
{
    fn name(&self) -> &str {
        self.display_name.as_str()
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        T::return_type()
    }

    fn num_arguments(&self) -> usize {
        1
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        let data_type = columns[0].data_type();
        let number_array: DataColumn = match data_type {
            DataType::Date16 => {
                if let DataColumn::Constant(v, _) = columns[0].column() {
                    let date_time = Utc.timestamp(v.as_u64().unwrap() as i64 * 24 * 3600, 0_u32);
                    let constant_result = T::to_constant_value(date_time);
                    Ok(DataColumn::Constant(constant_result, input_rows))
                } else {
                    let result: DFPrimitiveArray<R> = columns[0].column()
                        .to_array()?
                        .u16()?
                        .apply_cast_numeric(|v| {
                            let date_time = Utc.timestamp(v as i64 * 24 * 3600, 0_u32);
                            T::to_number(date_time)
                        }
                        );
                    Ok(result.into())
                }
            },
            DataType::Date32 => {
                if let DataColumn::Constant(v, _) = columns[0].column() {
                    let date_time = Utc.timestamp(v.as_u64().unwrap() as i64 * 24 * 3600, 0_u32);
                    let constant_result = T::to_constant_value(date_time);
                    Ok(DataColumn::Constant(constant_result, input_rows))
                } else {
                    let result = columns[0].column()
                        .to_array()?
                        .u32()?
                        .apply_cast_numeric(|v| {
                            let date_time = Utc.timestamp(v as i64 * 24 * 3600, 0_u32);
                            T::to_number(date_time)
                        }
                        );
                    Ok(result.into())
                }
            },
            DataType::DateTime32(_) => {
                if let DataColumn::Constant(v, _) = columns[0].column() {
                    let date_time = Utc.timestamp(v.as_u64().unwrap() as i64, 0_u32);
                    let constant_result = T::to_constant_value(date_time);
                    Ok(DataColumn::Constant(constant_result, input_rows))
                } else {
                    let result = columns[0].column()
                        .to_array()?
                        .u32()?
                        .apply_cast_numeric(|v| {
                            let date_time = Utc.timestamp(v as i64, 0_u32);
                            T::to_number(date_time)
                        }
                        );
                    Ok(result.into())
                }
            },
            other => Result::Err(ErrorCode::IllegalDataType(format!(
                "Illegal type {:?} of argument of function {}.Should be a date16/data32 or a dateTime32",
                other,
                self.name()))),
        }?;
        Ok(number_array)
    }
}

impl<T, R> fmt::Display for NumberFunction<T, R> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}()", self.display_name)
    }
}

pub type ToYYYYMMFunction = NumberFunction<ToYYYYMM, u32>;
pub type ToYYYYMMDDFunction = NumberFunction<ToYYYYMMDD, u32>;
pub type ToYYYYMMDDhhmmssFunction = NumberFunction<ToYYYYMMDDhhmmss, u64>;
pub type ToStartOfISOYearFunction = NumberFunction<ToStartOfISOYear, u32>;
pub type ToStartOfYearFunction = NumberFunction<ToStartOfYear, u32>;
