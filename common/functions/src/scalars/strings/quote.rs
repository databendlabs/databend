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
pub struct QuoteFunction {
    _display_name: String,
}

impl QuoteFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(QuoteFunction {
            _display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic())
    }
}

impl Function for QuoteFunction {
    fn name(&self) -> &str {
        "quote"
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
        let mut string_array = StringArrayBuilder::with_capacity(columns[0].column().len());
        let mut buffer = Vec::new();

        for value in columns[0]
            .column()
            .cast_with_type(&DataType::String)?
            .to_minimal_array()?
            .string()?
        {
            match value {
                Some(value) => string_array.append_value(quote_string(value, &mut buffer)),
                None => string_array.append_null(),
            }
        }

        Ok(string_array.finish().into())
    }
}

impl fmt::Display for QuoteFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "QUOTE")
    }
}

fn quote_string<'a>(value: &[u8], buffer: &'a mut Vec<u8>) -> &'a [u8] {
    buffer.clear();

    for ch in value {
        match *ch {
            0 => buffer.extend_from_slice(&[b'\\', b'0']),
            b'\'' => buffer.extend_from_slice(&[b'\\', b'\'']),
            b'\"' => buffer.extend_from_slice(&[b'\\', b'\"']),
            8 => buffer.extend_from_slice(&[b'\\', b'b']),
            b'\n' => buffer.extend_from_slice(&[b'\\', b'n']),
            b'\r' => buffer.extend_from_slice(&[b'\\', b'r']),
            b'\t' => buffer.extend_from_slice(&[b'\\', b't']),
            b'\\' => buffer.extend_from_slice(&[b'\\', b'\\']),
            _ => buffer.push(*ch),
        }
    }

    &buffer[..buffer.len()]
}
