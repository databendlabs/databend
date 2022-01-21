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

use common_datavalues2::StringType;
use common_exception::Result;

use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function2;
use crate::scalars::Function2Description;

#[derive(Clone)]
pub struct CurrentUserFunction {}

impl CurrentUserFunction {
    pub fn try_create(_display_name: &str) -> Result<Box<dyn Function2>> {
        Ok(Box::new(CurrentUserFunction {}))
    }

    pub fn desc() -> Function2Description {
        Function2Description::creator(Box::new(Self::try_create)).features(
            FunctionFeatures::default()
                .context_function()
                .num_arguments(1),
        )
    }
}

impl Function2 for CurrentUserFunction {
    fn name(&self) -> &str {
        "CurrentUserFunction"
    }

    fn return_type(
        &self,
        _args: &[&common_datavalues2::DataTypePtr],
    ) -> Result<common_datavalues2::DataTypePtr> {
        Ok(StringType::arc())
    }

    fn eval(
        &self,
        columns: &common_datavalues2::ColumnsWithField,
        _input_rows: usize,
    ) -> Result<common_datavalues2::ColumnRef> {
        Ok(columns[0].column().clone())
    }
}

impl fmt::Display for CurrentUserFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "current_user")
    }
}
