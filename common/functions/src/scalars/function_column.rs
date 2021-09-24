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
use common_datavalues::prelude::DataColumnsWithField;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::Result;

use crate::scalars::Function;

#[derive(Clone, Debug)]
pub struct ColumnFunction {
    value: String,
    _saved: Option<DataValue>,
}

impl ColumnFunction {
    pub fn try_create(value: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(ColumnFunction {
            value: value.to_string(),
            _saved: None,
        }))
    }
}

impl Function for ColumnFunction {
    fn name(&self) -> &str {
        "ColumnFunction"
    }

    fn return_type(&self, args: &[DataType]) -> Result<DataType> {
        Ok(args[0].clone())
    }

    fn nullable(&self, input_schema: &DataSchema) -> Result<bool> {
        let field = input_schema.field_with_name(self.value.as_str())?;
        Ok(field.is_nullable())
    }

    fn eval(&self, columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn> {
        Ok(columns[0].column().clone())
    }

    fn num_arguments(&self) -> usize {
        1
    }
}

impl fmt::Display for ColumnFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:#}", self.value)
    }
}
