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
            .features(FunctionFeatures::default().deterministic())
    }
}

impl Function for UnhexFunction {
    fn name(&self) -> &str {
        "unhex"
    }

    fn num_arguments(&self) -> usize {
        1
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if args[0] != DataType::String && args[0] != DataType::Null {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected string or null, but got {}",
                args[0]
            )));
        }

        Ok(DataType::String)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(true)
    }

    fn eval(&self, columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn> {
        const BUFFER_SIZE: usize = 32;
        let mut buffer = [0; BUFFER_SIZE];
        let mut string_array = StringArrayBuilder::with_capacity(columns[0].column().len());

        for value in columns[0]
            .column()
            .cast_with_type(&DataType::String)?
            .to_minimal_array()?
            .string()?
        {
            match value {
                Some(value) => {
                    if value.len() <= BUFFER_SIZE * 2 {
                        let size = value.len() / 2;
                        match hex::decode_to_slice(value, &mut buffer[..size]) {
                            Ok(()) => string_array.append_value(&buffer[..size]),
                            Err(_) => string_array.append_null(),
                        }
                    } else {
                        string_array.append_option(hex::decode(value).ok())
                    }
                }
                None => string_array.append_null(),
            }
        }

        Ok(string_array.finish().into())
    }
}

impl fmt::Display for UnhexFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "UNHEX")
    }
}
