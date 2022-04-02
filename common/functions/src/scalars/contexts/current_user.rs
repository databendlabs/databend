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

use common_datavalues::StringType;
use common_exception::Result;

use crate::scalars::Function;
use crate::scalars::FunctionContext;
use crate::scalars::FunctionFeatures;
use crate::scalars::TypedFunctionDescription;

#[derive(Clone)]
pub struct CurrentUserFunction {}

impl CurrentUserFunction {
    pub fn try_create(
        _display_name: &str,
        _args: &[&common_datavalues::DataTypePtr],
    ) -> Result<Box<dyn Function>> {
        Ok(Box::new(CurrentUserFunction {}))
    }

    pub fn desc() -> TypedFunctionDescription {
        TypedFunctionDescription::creator(Box::new(Self::try_create)).features(
            FunctionFeatures::default()
                .context_function()
                .num_arguments(1),
        )
    }
}

impl Function for CurrentUserFunction {
    fn name(&self) -> &str {
        "CurrentUserFunction"
    }

    fn return_type(
        &self,
        _args: &[&common_datavalues::DataTypePtr],
    ) -> Result<common_datavalues::DataTypePtr> {
        Ok(StringType::arc())
    }

    fn eval(
        &self,
        columns: &common_datavalues::ColumnsWithField,
        _input_rows: usize,
        _func_ctx: FunctionContext,
    ) -> Result<common_datavalues::ColumnRef> {
        Ok(columns[0].column().clone())
    }
}

impl fmt::Display for CurrentUserFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "current_user")
    }
}
