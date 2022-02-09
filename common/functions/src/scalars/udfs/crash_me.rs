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

use common_datavalues2::NullType;
use common_exception::Result;

use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function2;
use crate::scalars::Function2Description;

#[derive(Clone)]
pub struct CrashMeFunction {
    _display_name: String,
}

impl CrashMeFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function2>> {
        Ok(Box::new(CrashMeFunction {
            _display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> Function2Description {
        Function2Description::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().num_arguments(1))
    }
}

impl Function2 for CrashMeFunction {
    fn name(&self) -> &str {
        "CrashMeFunction"
    }

    fn return_type(
        &self,
        _args: &[&common_datavalues2::DataTypePtr],
    ) -> Result<common_datavalues2::DataTypePtr> {
        Ok(NullType::arc())
    }

    fn eval(
        &self,
        _columns: &common_datavalues2::ColumnsWithField,
        _input_rows: usize,
    ) -> Result<common_datavalues2::ColumnRef> {
        panic!("crash me function");
    }
}

impl fmt::Display for CrashMeFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "crashme")
    }
}
