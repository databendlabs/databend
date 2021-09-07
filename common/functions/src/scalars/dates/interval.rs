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

use common_datavalues::prelude::*;
use common_exception::Result;

use crate::scalars::Function;

macro_rules! define_interval_struct {
    ($interval_type: ident) => {
        #[derive(Clone)]
        pub struct $interval_type {
            display_name: String,
        }
    };
}

define_interval_struct!(IntervalYearFunction);
define_interval_struct!(IntervalMonthFunction);
define_interval_struct!(IntervalDayFunction);
define_interval_struct!(IntervalHourFunction);
define_interval_struct!(IntervalMinuteFunction);
define_interval_struct!(IntervalSecondFunction);

macro_rules! impl_function {
    ($interval_type: ident, $interval_unit: expr) => {
        impl Function for $interval_type {
            fn name(&self) -> &str {
                self.display_name.as_str()
            }

            fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
                Ok(DataType::Interval($interval_unit))
            }

            fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
                Ok(false)
            }

            fn num_arguments(&self) -> usize {
                1
            }

            fn eval(
                &self,
                columns: &DataColumnsWithField,
                input_rows: usize,
            ) -> Result<DataColumn> {
                let interval_num = columns[0].column();
                let seconds_per_unit = IntervalUnit::avg_seconds($interval_unit);
                let seconds = DataColumn::Constant(
                    DataValue::Float64(Some(seconds_per_unit as f64)),
                    input_rows,
                );
                let total_seconds =
                    interval_num.arithmetic(DataValueArithmeticOperator::Mul, &seconds);
                println!("total seconds: {:?}", total_seconds);
                total_seconds
            }
        }
    };
}

impl_function!(IntervalYearFunction, IntervalUnit::Year);
impl_function!(IntervalMonthFunction, IntervalUnit::Month);
impl_function!(IntervalDayFunction, IntervalUnit::Day);
impl_function!(IntervalHourFunction, IntervalUnit::Hour);
impl_function!(IntervalMinuteFunction, IntervalUnit::Minute);
impl_function!(IntervalSecondFunction, IntervalUnit::Second);

macro_rules! define_try_create {
    ($interval_type: ident) => {
        impl $interval_type {
            pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
                Ok(Box::new($interval_type {
                    display_name: display_name.to_string(),
                }))
            }
        }
    };
}

define_try_create!(IntervalYearFunction);
define_try_create!(IntervalMonthFunction);
define_try_create!(IntervalDayFunction);
define_try_create!(IntervalHourFunction);
define_try_create!(IntervalMinuteFunction);
define_try_create!(IntervalSecondFunction);

macro_rules! define_display {
    ($interval_type: ident, $content: expr) => {
        impl fmt::Display for $interval_type {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, $content)
            }
        }
    };
}

define_display!(IntervalYearFunction, "toIntervalYear()");
define_display!(IntervalMonthFunction, "toIntervalMonth()");
define_display!(IntervalDayFunction, "toIntervalDay()");
define_display!(IntervalHourFunction, "toIntervalHour()");
define_display!(IntervalMinuteFunction, "toIntervalMinute()");
define_display!(IntervalSecondFunction, "toIntervalSecond()");
