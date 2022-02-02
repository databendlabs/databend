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

use common_datavalues2::type_primitive;
use common_datavalues2::ColumnRef;
use common_datavalues2::ColumnsWithField;
use common_datavalues2::DataTypePtr;
use common_datavalues2::DataValue;
use common_exception::Result;

use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function2;
use crate::scalars::Function2Description;

// ignore(...) is a function that takes any arguments, and always returns 0.
// it can be used in performance tests
// eg: SELECT count() FROM numbers(1000000000) WHERE NOT ignore( toString(number) );
#[derive(Clone)]
pub struct IgnoreFunction {
    display_name: String,
}

impl IgnoreFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function2>> {
        Ok(Box::new(IgnoreFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> Function2Description {
        Function2Description::creator(Box::new(Self::try_create)).features(
            FunctionFeatures::default()
                .deterministic()
                .variadic_arguments(0, usize::MAX),
        )
    }
}

impl fmt::Display for IgnoreFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name.to_uppercase())
    }
}

impl Function2 for IgnoreFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self, _args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        Ok(type_primitive::UInt8Type::arc())
    }

    fn eval(&self, _columns: &ColumnsWithField, input_rows: usize) -> Result<ColumnRef> {
        let return_type = type_primitive::UInt8Type::arc();
        let return_value = DataValue::try_from(0_u8)?;
        return_type.create_constant_column(&return_value, input_rows)
    }

    fn passthrough_null(&self) -> bool {
        false
    }
}
