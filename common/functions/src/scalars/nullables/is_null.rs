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
use common_datavalues::DataType;
use common_datavalues::DataTypeAndNullable;
use common_exception::Result;

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

#[derive(Clone)]
pub struct IsNullFunction {
    _display_name: String,
}

impl IsNullFunction {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(IsNullFunction {
            _display_name: "isNull".to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create_func)).features(
            FunctionFeatures::default()
                .deterministic()
                .negative_function("isnotnull")
                .bool_function()
                .num_arguments(1),
        )
    }
}

impl Function for IsNullFunction {
    fn name(&self) -> &str {
        "IsNullFunction"
    }

    fn return_type(&self, _args: &[DataTypeAndNullable]) -> Result<DataTypeAndNullable> {
        let dt = DataType::Boolean(false);
        Ok(DataTypeAndNullable::create(&dt, false))
    }

    fn eval(&self, columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn> {
        columns[0].column().is_null()
    }
}

impl std::fmt::Display for IsNullFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "isNull")
    }
}
