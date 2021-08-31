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
use common_exception::Result;

use crate::scalars::Function;

#[derive(Clone)]
pub struct CastFunction {
    display_name: String,
    /// The data type to cast to
    cast_type: DataType,
}

impl CastFunction {
    pub fn create(display_name: String, cast_type: DataType) -> Result<Box<dyn Function>> {
        Ok(Box::new(Self {
            display_name,
            cast_type,
        }))
    }
}

impl Function for CastFunction {
    fn name(&self) -> &str {
        "CastFunction"
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(self.cast_type.clone())
    }

    // TODO
    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        let series = columns[0].column().clone().to_minimal_array()?;
        let column: DataColumn = series.cast_with_type(&self.cast_type)?.into();
        Ok(column.resize_constant(input_rows))
    }

    fn num_arguments(&self) -> usize {
        1
    }
}

impl fmt::Display for CastFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CAST")
    }
}
