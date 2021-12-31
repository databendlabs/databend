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

use std::fmt;
use std::marker::PhantomData;
use std::ops::Sub;

use common_datavalues::chrono::DateTime;
use common_datavalues::chrono::Datelike;
use common_datavalues::chrono::Duration;
use common_datavalues::chrono::TimeZone;
use common_datavalues::chrono::Utc;
use common_datavalues::prelude::*;
use common_datavalues::DataTypeAndNullable;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;
use crate::scalars::Monotonicity;

#[derive(Clone, Debug)]
pub struct WeekFunction<T, R> {
    display_name: String,
    t: PhantomData<T>,
    r: PhantomData<R>,
}

pub trait WeekResultFunction<R> {
    const IS_DETERMINISTIC: bool;

    fn return_type() -> Result<DataType>;
    fn to_number(_value: DateTime<Utc>, mode: Option<u64>) -> R;
    fn to_constant_value(_value: DateTime<Utc>, mode: Option<u64>) -> DataValue;
    fn factor_function() -> Result<Box<dyn Function>> {
        Err(ErrorCode::UnknownException(
            "Always monotonous, has no factor function",
        ))
    }
}

#[derive(Clone)]
pub struct ToStartOfWeek;

impl WeekResultFunction<u32> for ToStartOfWeek {
    const IS_DETERMINISTIC: bool = true;

    fn return_type() -> Result<DataType> {
        Ok(DataType::Date16)
    }
    fn to_number(value: DateTime<Utc>, mode: Option<u64>) -> u32 {
        let week_mode = mode.unwrap_or(0);
        let mut weekday = value.weekday().number_from_sunday();
        if week_mode & 1 == 1 {
            weekday = value.weekday().number_from_monday();
        }
        weekday -= 1;
        let duration = Duration::days(weekday as i64);
        let result = value.sub(duration);
        get_day(result)
    }

    fn to_constant_value(value: DateTime<Utc>, mode: Option<u64>) -> DataValue {
        DataValue::UInt16(Some(Self::to_number(value, mode) as u16))
    }
}

impl<T, R> WeekFunction<T, R>
where
    T: WeekResultFunction<R> + Clone + Sync + Send + 'static,
    R: DFPrimitiveType + Clone,
    DFPrimitiveArray<R>: IntoSeries,
{
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(WeekFunction::<T, R> {
            display_name: display_name.to_string(),
            t: PhantomData,
            r: PhantomData,
        }))
    }

    pub fn desc() -> FunctionDescription {
        let mut features = FunctionFeatures::default()
            .monotonicity()
            .variadic_arguments(1, 2);

        if T::IS_DETERMINISTIC {
            features = features.deterministic();
        }

        FunctionDescription::creator(Box::new(Self::try_create)).features(features)
    }
}

impl<T, R> Function for WeekFunction<T, R>
where
    T: WeekResultFunction<R> + Clone + Sync + Send,
    R: DFPrimitiveType + Clone,
    DFPrimitiveArray<R>: IntoSeries,
{
    fn name(&self) -> &str {
        self.display_name.as_str()
    }

    fn return_type(&self, args: &[DataTypeAndNullable]) -> Result<DataTypeAndNullable> {
        let dt = T::return_type()?;
        let nullable = args.iter().any(|arg| arg.is_nullable());
        Ok(DataTypeAndNullable::create(&dt, nullable))
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        let data_type = columns[0].data_type();
        let mut mode: Option<u64> = None;
        if columns.len() == 2 && !columns[1].column().is_empty() {
            let week_mode = columns[1].column().to_values()?[0].clone().as_u64()?;
            if !(0..=9).contains(&week_mode) {
                return Err(ErrorCode::BadArguments(format!(
                    "The parameter:{} range is abnormal, it should be between 0-9",
                    week_mode
                )));
            }
            mode = Some(week_mode);
        }
        let number_array: DataColumn = match data_type {
            DataType::Date16 => {
                if let DataColumn::Constant(v, _) = columns[0].column() {
                    let date_time = Utc.timestamp(v.as_u64()? as i64 * 24 * 3600, 0_u32);
                    let constant_result = T::to_constant_value(date_time, mode);
                    Ok(DataColumn::Constant(constant_result, input_rows))
                } else {
                    let result: DFPrimitiveArray<R> = columns[0].column()
                        .to_array()?
                        .u16()?
                        .apply_cast_numeric(|v| {
                            let date_time = Utc.timestamp(v as i64 * 24 * 3600, 0_u32);
                            T::to_number(date_time, mode)
                        }
                        );
                    Ok(result.into())
                }
            },
            DataType::Date32 => {
                if let DataColumn::Constant(v, _) = columns[0].column() {
                    let date_time = Utc.timestamp(v.as_i64()?  * 24 * 3600, 0_u32);
                    let constant_result = T::to_constant_value(date_time, mode);
                    Ok(DataColumn::Constant(constant_result, input_rows))
                } else {
                    let result = columns[0].column()
                        .to_array()?
                        .i32()?
                        .apply_cast_numeric(|v| {
                            let date_time = Utc.timestamp(v as i64 * 24 * 3600, 0_u32);
                            T::to_number(date_time, mode)
                        }
                        );
                    Ok(result.into())
                }
            },
            DataType::DateTime32(_) => {
                if let DataColumn::Constant(v, _) = columns[0].column() {
                    let date_time = Utc.timestamp(v.as_i64()?, 0_u32);
                    let constant_result = T::to_constant_value(date_time, mode);
                    Ok(DataColumn::Constant(constant_result, input_rows))
                } else {
                    let result = columns[0].column()
                        .to_array()?
                        .u32()?
                        .apply_cast_numeric(|v| {
                            let date_time = Utc.timestamp(v as i64, 0_u32);
                            T::to_number(date_time, mode)
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

    fn get_monotonicity(&self, args: &[Monotonicity]) -> Result<Monotonicity> {
        let func = match T::factor_function() {
            Ok(f) => f,
            Err(_) => return Ok(Monotonicity::clone_without_range(&args[0])),
        };

        if args[0].left.is_none() || args[0].right.is_none() {
            return Ok(Monotonicity::default());
        }

        let left_val = func.eval(&[args[0].left.clone().unwrap()], 1)?.try_get(0)?;
        let right_val = func
            .eval(&[args[0].right.clone().unwrap()], 1)?
            .try_get(0)?;
        // The function is monotonous, if the factor eval returns the same values for them.
        if left_val == right_val {
            return Ok(Monotonicity::clone_without_range(&args[0]));
        }

        Ok(Monotonicity::default())
    }
}

impl<T, R> fmt::Display for WeekFunction<T, R> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}()", self.display_name)
    }
}

fn get_day(date: DateTime<Utc>) -> u32 {
    let start: DateTime<Utc> = Utc.ymd(1970, 1, 1).and_hms(0, 0, 0);
    let duration = date.signed_duration_since(start);
    duration.num_days() as u32
}

pub type ToStartOfWeekFunction = WeekFunction<ToStartOfWeek, u32>;
