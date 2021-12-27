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
use common_datavalues::DataTypeAndNullable;

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

const MAX_REPEAT_TIMES: u64 = 1000000;

#[derive(Clone)]
pub struct RepeatFunction {
    _display_name: String,
}

impl RepeatFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(RepeatFunction {
            _display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(2))
    }
}

impl Function for RepeatFunction {
    fn name(&self) -> &str {
        "repeat"
    }

    fn return_type(&self, args: &[DataTypeAndNullable]) -> Result<DataType> {
        if !args[0].is_string() && !args[0].is_null() {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected parameter 1 is string, but got {}",
                args[0]
            )));
        }

        if !args[1].is_unsigned_integer()
            && !args[1].is_string()
            && !args[1].is_null()
        {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected parameter 2 is unsigned integer or string or null, but got {}",
                args[1]
            )));
        }

        Ok(DataType::String)
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        match (
            columns[0].column().cast_with_type(&DataType::String)?,
            columns[1].column().cast_with_type(&DataType::UInt64)?,
        ) {
            (
                DataColumn::Constant(DataValue::String(input_string), _),
                DataColumn::Constant(DataValue::UInt64(times), _),
            ) => Ok(DataColumn::Constant(
                DataValue::String(repeat(input_string, times)?),
                input_rows,
            )),
            (
                DataColumn::Constant(DataValue::String(input_string), _),
                DataColumn::Array(times),
            ) => {
                let mut string_builder = StringArrayBuilder::with_capacity(input_rows);
                for times in times.u64()? {
                    string_builder.append_option(repeat(input_string.as_ref(), times.copied())?);
                }
                Ok(string_builder.finish().into())
            }
            (
                DataColumn::Array(input_string),
                DataColumn::Constant(DataValue::UInt64(times), _),
            ) => {
                let mut string_builder = StringArrayBuilder::with_capacity(input_rows);
                for input_string in input_string.string()? {
                    string_builder.append_option(repeat(input_string, times)?);
                }
                Ok(string_builder.finish().into())
            }
            (DataColumn::Array(input_string), DataColumn::Array(times)) => {
                let mut string_builder = StringArrayBuilder::with_capacity(input_rows);
                for (input_string, times) in input_string.string()?.into_iter().zip(times.u64()?) {
                    string_builder.append_option(repeat(input_string, times.copied())?);
                }
                Ok(string_builder.finish().into())
            }
            _ => Ok(DataColumn::Constant(DataValue::Null, input_rows)),
        }
    }
}

impl fmt::Display for RepeatFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "REPEAT")
    }
}

#[inline]
fn repeat(string: Option<impl AsRef<[u8]>>, times: Option<u64>) -> Result<Option<Vec<u8>>> {
    if let (Some(string), Some(times)) = (string, times) {
        if times > MAX_REPEAT_TIMES {
            return Err(ErrorCode::BadArguments(format!(
                "Too many times to repeat: ({}), maximum is: {}",
                times, MAX_REPEAT_TIMES
            )));
        }
        Ok(Some(string.as_ref().repeat(times as usize)))
    } else {
        Ok(None)
    }
}
