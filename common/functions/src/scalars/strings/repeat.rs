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

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::cast_column_field;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;

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

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        if !args[0].data_type_id().is_string() && !args[0].data_type_id().is_null() {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected parameter 1 is string, but got {}",
                args[0].data_type_id()
            )));
        }

        if !args[1].data_type_id().is_unsigned_integer() && !args[1].data_type_id().is_null() {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected parameter 2 is unsigned integer or null, but got {}",
                args[1].data_type_id()
            )));
        }

        Ok(StringType::arc())
    }

    fn eval(&self, columns: &ColumnsWithField, input_rows: usize) -> Result<ColumnRef> {
        let col1 = cast_column_field(&columns[0], &StringType::arc())?;
        let col1_viewer = Vu8::try_create_viewer(&col1)?;

        let col2 = cast_column_field(&columns[1], &UInt64Type::arc())?;
        let col2_viewer = u64::try_create_viewer(&col2)?;

        let mut builder = ColumnBuilder::<Vu8>::with_capacity(input_rows);

        let iter = col1_viewer.iter().zip(col2_viewer.iter());
        for (string, times) in iter {
            let val = repeat(string, times)?;
            builder.append(&val);
        }

        Ok(builder.build(input_rows))
    }
}

impl fmt::Display for RepeatFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "REPEAT")
    }
}

#[inline]
fn repeat(string: impl AsRef<[u8]>, times: u64) -> Result<Vec<u8>> {
    if times > MAX_REPEAT_TIMES {
        return Err(ErrorCode::BadArguments(format!(
            "Too many times to repeat: ({}), maximum is: {}",
            times, MAX_REPEAT_TIMES
        )));
    }
    Ok(string.as_ref().repeat(times as usize))
}
