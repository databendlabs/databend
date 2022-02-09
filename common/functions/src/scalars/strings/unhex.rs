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
use common_datavalues::DataTypeAndNullable;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

#[derive(Clone)]
pub struct UnhexFunction {
    _display_name: String,
}

impl UnhexFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(UnhexFunction {
            _display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(1))
    }
}

impl Function for UnhexFunction {
    fn name(&self) -> &str {
        "unhex"
    }

    fn return_type(&self, args: &[DataTypeAndNullable]) -> Result<DataTypeAndNullable> {
        if !args[0].is_string() && !args[0].is_null() {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected string or null, but got {}",
                args[0]
            )));
        }

        let dt = DataType::String;
        Ok(DataTypeAndNullable::create(&dt, true))
    }

    fn eval(&self, columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn> {
        const BUFFER_SIZE: usize = 32;

        let array = columns[0]
            .column()
            .cast_with_type(&DataType::String)?
            .to_minimal_array()?;
        let c_array = array.string()?;

        let column: DataColumn = transform(c_array, c_array.inner().values().len(), |x, buffer| {
            if x.len() <= BUFFER_SIZE * 2 {
                let size = x.len() / 2;

                let buffer = &mut buffer[0..size];
                match hex::decode_to_slice(x, buffer) {
                    Ok(()) => Some(size),
                    Err(_) => None,
                }
            } else {
                None
            }
        })
        .into();
        Ok(column)
    }
}

impl fmt::Display for UnhexFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "UNHEX")
    }
}
