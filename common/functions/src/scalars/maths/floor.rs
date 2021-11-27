// Copyright 2020 Datafuse Lfloor.
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
use std::str;

use common_datavalues::prelude::ArrayApply;
use common_datavalues::prelude::DataColumn;
use common_datavalues::prelude::DataColumnsWithField;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;
use crate::scalars::Monotonicity;

#[derive(Clone)]
pub struct FloorFunction {
    display_name: String,
}

impl FloorFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(FloorFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().monotonicity())
    }
}

impl Function for FloorFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn num_arguments(&self) -> usize {
        1
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        if matches!(
            args[0],
            DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64
                | DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::String
                | DataType::Null
        ) {
            Ok(DataType::Float64)
        } else {
            Err(ErrorCode::IllegalDataType(format!(
                "Expected numeric types, but got {}",
                args[0]
            )))
        }
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn> {
        let result = columns[0]
            .column()
            .to_minimal_array()?
            .cast_with_type(&DataType::Float64)?
            .f64()?
            .apply_cast_numeric(|v| v.floor());
        let column: DataColumn = result.into();
        Ok(column)
    }

    fn get_monotonicity(&self, args: &[Monotonicity]) -> Result<Monotonicity> {
        // Floor function should be monotonically positive. For val_1 > val2, we should have floor(val_1) >= floor(val_2), and vise versa.
        // So we return the monotonicity same as the input.
        Ok(Monotonicity::clone_without_range(&args[0]))
    }
}

impl fmt::Display for FloorFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name.to_uppercase())
    }
}
