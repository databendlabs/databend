// Copyright 2022 Datafuse Labs.
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

use common_datavalues::BooleanType;
use common_datavalues::ColumnRef;
use common_datavalues::ColumnsWithField;
use common_datavalues::DataTypePtr;
use common_datavalues::DataValue;
use common_exception::Result;

use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;

// ignore(...) is a function that takes any arguments, and always returns 0.
// it can be used in performance tests
// eg: SELECT count() FROM numbers(1000000000) WHERE NOT ignore( toString(number) );
#[derive(Clone)]
pub struct IgnoreFunction {
    display_name: String,
}

impl IgnoreFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(IgnoreFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create)).features(
            FunctionFeatures::default()
                .deterministic()
                .bool_function()
                .variadic_arguments(0, usize::MAX),
        )
    }
}

impl fmt::Display for IgnoreFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name.to_uppercase())
    }
}

impl Function for IgnoreFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self, _args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        Ok(BooleanType::arc())
    }

    fn eval(&self, _columns: &ColumnsWithField, input_rows: usize) -> Result<ColumnRef> {
        let return_type = BooleanType::arc();
        let return_value = DataValue::try_from(false)?;
        return_type.create_constant_column(&return_value, input_rows)
    }

    fn passthrough_null(&self) -> bool {
        false
    }
}
