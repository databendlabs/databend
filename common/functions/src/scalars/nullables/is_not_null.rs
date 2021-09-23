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
use common_exception::Result;

use crate::scalars::Function;

#[derive(Clone)]
pub struct IsNotNullFunction {
    display_name: String,
}

impl IsNotNullFunction {
    pub fn try_create_func(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(IsNotNullFunction {
            display_name: display_name.to_string(),
        }))
    }
}

impl Function for IsNotNullFunction {
    fn name(&self) -> &str {
        "IsNotNullFunction"
    }

    fn num_arguments(&self) -> usize {
        1
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn> {
        columns[0].column().is_not_null()
    }
}

impl std::fmt::Display for IsNotNullFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "isNotNull")
    }
}
