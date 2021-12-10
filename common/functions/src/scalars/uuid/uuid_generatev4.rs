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

use common_datavalues::columns::DataColumn;
use common_datavalues::prelude::DataColumnsWithField;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::Result;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

use uuid::Uuid;

#[derive(Clone)]
pub struct GenerateUUIDv4Function {
    _display_name: String,
}

impl GenerateUUIDv4Function {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(GenerateUUIDv4Function {
            _display_name: "generateUUIDv4".to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func)).features(
            FunctionFeatures::default().deterministic()
        )
    }
}

impl Function for GenerateUUIDv4Function {
    fn name(&self) -> &str {
        "GenerateUUIDv4Function"
    }

    fn num_arguments(&self) -> usize {
        0
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::String)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, _columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        let uuid = Uuid::new_v4().to_string().as_bytes().to_vec();
        let value = DataValue::String(Some(uuid));
        Ok(DataColumn::Constant(value, input_rows))
    }
}

impl std::fmt::Display for GenerateUUIDv4Function {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "generateUUIDv4")
    }
}
