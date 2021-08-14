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
use common_exception::Result;

use crate::scalars::Function;

#[derive(Clone, Debug)]
pub struct LiteralFunction {
    value: DataValue,
}

impl LiteralFunction {
    pub fn try_create(value: DataValue) -> Result<Box<dyn Function>> {
        Ok(Box::new(LiteralFunction { value }))
    }
}

impl Function for LiteralFunction {
    fn name(&self) -> &str {
        "LiteralFunction"
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(self.value.data_type())
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(self.value.is_null())
    }

    fn eval(&self, _columns: &[DataColumn], input_rows: usize) -> Result<DataColumn> {
        Ok(DataColumn::Constant(self.value.clone(), input_rows))
    }

    fn num_arguments(&self) -> usize {
        0
    }
}

impl fmt::Display for LiteralFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self.value)
    }
}
