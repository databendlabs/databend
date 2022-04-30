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
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::assert_date_or_timestamp;
use crate::scalars::assert_numeric;
use crate::scalars::Function;
use crate::scalars::FunctionAdapter;
use crate::scalars::FunctionContext;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFeatures;
use crate::scalars::Monotonicity;

#[derive(Clone, Debug)]
pub struct WeekFunction<T, R> {
    display_name: String,
    t: PhantomData<T>,
    r: PhantomData<R>,
}

pub trait WeekResultFunction<R> {
    const IS_DETERMINISTIC: bool;

    fn return_type() -> DataTypeImpl;
    fn to_number(_value: DateTime<Utc>, mode: u64) -> R;
    fn factor_function() -> Option<Box<dyn Function>> {
        None
    }
}

#[derive(Clone)]
pub struct ToStartOfWeek;

impl WeekResultFunction<i32> for ToStartOfWeek {
    const IS_DETERMINISTIC: bool = true;

    fn return_type() -> DataTypeImpl {
        DateType::new_impl()
    }
    fn to_number(value: DateTime<Utc>, week_mode: u64) -> i32 {
        let mut weekday = value.weekday().number_from_sunday();
        if week_mode & 1 == 1 {
            weekday = value.weekday().number_from_monday();
        }
        weekday -= 1;
        let duration = Duration::days(weekday as i64);
        let result = value.sub(duration);
        get_day(result)
    }
}

impl<T, R> WeekFunction<T, R>
where
    T: WeekResultFunction<R> + Clone + Sync + Send + 'static,
    R: PrimitiveType + Clone,
    R: Scalar<ColumnType = PrimitiveColumn<R>>,
    for<'a> R: Scalar<RefType<'a> = R>,
    for<'a> R: ScalarRef<'a, ScalarType = R, ColumnType = PrimitiveColumn<R>>,
{
    pub fn try_create(display_name: &str, args: &[&DataTypeImpl]) -> Result<Box<dyn Function>> {
        assert_date_or_timestamp(args[0])?;
        if args.len() > 1 {
            assert_numeric(args[1])?;
        }

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
    R: PrimitiveType + Clone,
    R: Scalar<ColumnType = PrimitiveColumn<R>>,
    for<'a> R: Scalar<RefType<'a> = R>,
    for<'a> R: ScalarRef<'a, ScalarType = R, ColumnType = PrimitiveColumn<R>>,
{
    fn name(&self) -> &str {
        self.display_name.as_str()
    }

    fn return_type(&self) -> DataTypeImpl {
        T::return_type()
    }

    fn eval(
        &self,
        _func_ctx: FunctionContext,
        columns: &ColumnsWithField,
        input_rows: usize,
    ) -> Result<ColumnRef> {
        let mut mode = 0;
        if columns.len() > 1 {
            if input_rows != 1 && !columns[1].column().is_const() {
                return Err(ErrorCode::BadArguments(
                    "Expected constant column for the second argument, a constant mode from 0-9"
                        .to_string(),
                ));
            }

            let week_mode = columns[1].column().get_u64(0)?;
            if !(0..=9).contains(&week_mode) {
                return Err(ErrorCode::BadArguments(format!(
                    "The parameter:{} range is abnormal, it should be between 0-9",
                    week_mode
                )));
            }
            mode = week_mode;
        }

        match columns[0].data_type().data_type_id() {
            TypeID::Date => {
                let col: &Int32Column = Series::check_get(columns[0].column())?;
                let iter = col.scalar_iter().map(|v| {
                    let date_time = Utc.timestamp(v as i64 * 24 * 3600, 0_u32);
                    T::to_number(date_time, mode)
                });
                let col = PrimitiveColumn::<R>::from_owned_iterator(iter).arc();
                let viewer = i32::try_create_viewer(&col)?;
                for days in viewer.iter() {
                    let _ = check_date(days)?;
                }
                Ok(col)
            }
            TypeID::Timestamp => {
                let col: &Int64Column = Series::check_get(columns[0].column())?;
                let iter = col.scalar_iter().map(|v| {
                    let date_time = Utc.timestamp(v / 1_000_000, 0_u32);
                    T::to_number(date_time, mode)
                });
                let col = PrimitiveColumn::<R>::from_owned_iterator(iter).arc();
                let viewer = i32::try_create_viewer(&col)?;
                for days in viewer.iter() {
                    let _ = check_date(days)?;
                }
                Ok(col)
            }
            other => Result::Err(ErrorCode::IllegalDataType(format!(
                "Illegal type {:?} of argument of function {}.Should be a Date or Timestamp",
                other,
                self.name()
            ))),
        }
    }

    fn get_monotonicity(&self, args: &[Monotonicity]) -> Result<Monotonicity> {
        let func = match T::factor_function() {
            Some(f) => f,
            None => return Ok(Monotonicity::clone_without_range(&args[0])),
        };

        if args[0].left.is_none() || args[0].right.is_none() {
            return Ok(Monotonicity::default());
        }

        let func = FunctionAdapter::create(func, true);
        let left_val = func
            .eval(
                FunctionContext::default(),
                &[args[0].left.clone().unwrap()],
                1,
            )?
            .get(0);
        let right_val = func
            .eval(
                FunctionContext::default(),
                &[args[0].right.clone().unwrap()],
                1,
            )?
            .get(0);
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

fn get_day(date: DateTime<Utc>) -> i32 {
    let start: DateTime<Utc> = Utc.ymd(1970, 1, 1).and_hms(0, 0, 0);
    let duration = date.signed_duration_since(start);
    duration.num_days() as i32
}

pub type ToStartOfWeekFunction = WeekFunction<ToStartOfWeek, i32>;
