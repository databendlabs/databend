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

use common_datavalues::columns::DataColumn;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::Function;

#[derive(Clone)]
pub struct ExistsFunction;

impl ExistsFunction {
    pub fn try_create(_display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(ExistsFunction {}))
    }
}

impl Function for ExistsFunction {
    fn name(&self) -> &str {
        "ExistsFunction"
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &[DataColumn], _input_rows: usize) -> Result<DataColumn> {
        match &columns[0] {
            DataColumn::Array(_) => Err(ErrorCode::LogicalError(
                "Logical error: subquery result set must be const.",
            )),
            DataColumn::Constant(values, size) => match values {
                DataValue::List(Some(values), _) => Ok(DataColumn::Constant(
                    DataValue::Boolean(Some(!values.is_empty())),
                    *size,
                )),
                DataValue::Struct(fields) if !fields.is_empty() => match &fields[0] {
                    DataValue::List(Some(values), _) => Ok(DataColumn::Constant(
                        DataValue::Boolean(Some(!values.is_empty())),
                        *size,
                    )),
                    _ => Err(ErrorCode::LogicalError(
                        "Logical error: subquery result set must be Struct(List(Some)).",
                    )),
                },
                _ => Err(ErrorCode::LogicalError(
                    "Logical error: subquery result set must be List(Some) or Struct(List(Some)).",
                )),
            },
        }
    }

    fn num_arguments(&self) -> usize {
        1
    }

    fn is_deterministic(&self) -> bool {
        false
    }
}

impl fmt::Display for ExistsFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "EXISTS")
    }
}
