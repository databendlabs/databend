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

use common_arrow::arrow_format::ipc::flatbuffers::bitflags::_core::cmp::Ordering;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

trait OctString {
    fn oct_string(self) -> String;
}

impl OctString for i64 {
    fn oct_string(self) -> String {
        match self.cmp(&0) {
            Ordering::Less => {
                format!("-0{:o}", self.unsigned_abs())
            }
            Ordering::Equal => "0".to_string(),
            Ordering::Greater => format!("0{:o}", self),
        }
    }
}

impl OctString for u64 {
    fn oct_string(self) -> String {
        if self == 0 {
            "0".to_string()
        } else {
            format!("0{:o}", self)
        }
    }
}

#[derive(Clone)]
pub struct OctFunction {
    _display_name: String,
}

impl OctFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(OctFunction {
            _display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic())
    }
}

impl Function for OctFunction {
    fn name(&self) -> &str {
        "oct"
    }

    fn num_arguments(&self) -> usize {
        1
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if !args[0].is_integer() && args[0] != DataType::String && args[0] != DataType::Null {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected integer or string or null, but got {}",
                args[0]
            )));
        }

        Ok(DataType::String)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(true)
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        match columns[0].data_type() {
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                let mut string_array = StringArrayBuilder::with_capacity(input_rows);
                for value in columns[0]
                    .column()
                    .cast_with_type(&DataType::UInt64)?
                    .to_minimal_array()?
                    .u64()?
                {
                    string_array.append_option(value.map(|n| n.oct_string()));
                }

                let column: DataColumn = string_array.finish().into();
                Ok(column.resize_constant(columns[0].column().len()))
            }
            _ => {
                let mut string_array = StringArrayBuilder::with_capacity(input_rows);
                for value in columns[0]
                    .column()
                    .cast_with_type(&DataType::Int64)?
                    .to_minimal_array()?
                    .i64()?
                {
                    string_array.append_option(value.map(|n| n.oct_string()));
                }

                let column: DataColumn = string_array.finish().into();
                Ok(column.resize_constant(columns[0].column().len()))
            }
        }
    }
}

impl fmt::Display for OctFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "OCT")
    }
}
