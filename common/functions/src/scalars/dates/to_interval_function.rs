// Copyright 2022 Datafuse Labs.
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
use common_datavalues::IntervalKind;
use common_datavalues::IntervalType;
use common_exception::Result;

use crate::scalars::Function;
use crate::scalars::FunctionContext;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFeatures;

macro_rules! impl_to_interval_function {
    ($FUNCTION_NAME:ident, $INTERVAL_KIND: ident) => {
        #[derive(Clone)]
        pub struct $FUNCTION_NAME {
            display_name: String,
        }

        impl $FUNCTION_NAME {
            pub fn try_create(
                display_name: &str,
                _args: &[&DataTypeImpl],
            ) -> Result<Box<dyn Function>> {
                Ok(Box::new($FUNCTION_NAME {
                    display_name: display_name.to_string(),
                }))
            }

            pub fn desc() -> FunctionDescription {
                FunctionDescription::creator(Box::new(Self::try_create))
                    .features(FunctionFeatures::default().num_arguments(1))
            }
        }

        impl Function for $FUNCTION_NAME {
            fn name(&self) -> &str {
                self.display_name.as_str()
            }

            fn return_type(&self) -> DataTypeImpl {
                IntervalType::new_impl(IntervalKind::$INTERVAL_KIND)
            }

            fn eval(
                &self,
                _func_ctx: FunctionContext,
                columns: &ColumnsWithField,
                _input_rows: usize,
            ) -> Result<ColumnRef> {
                IntervalType::new_impl(IntervalKind::$INTERVAL_KIND)
                    .create_column(&columns[0].column().to_values())
            }
        }

        impl fmt::Display for $FUNCTION_NAME {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "{}()", self.display_name)
            }
        }
    };
}

impl_to_interval_function!(ToIntervalYearFunction, Year);
impl_to_interval_function!(ToIntervalMonthFunction, Month);
impl_to_interval_function!(ToIntervalDayFunction, Day);
impl_to_interval_function!(ToIntervalHourFunction, Hour);
impl_to_interval_function!(ToIntervalMinuteFunction, Minute);
impl_to_interval_function!(ToIntervalSecondFunction, Second);
impl_to_interval_function!(ToIntervalDoyFunction, Doy);
impl_to_interval_function!(ToIntervalDowFunction, Dow);
