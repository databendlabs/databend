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

use common_datavalues::prelude::*;
use common_exception::Result;

use crate::scalars::Function;
use crate::scalars::FunctionContext;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFeatures;

#[derive(Clone)]
pub struct TypeOfFunction {
    _display_name: String,
}

impl TypeOfFunction {
    pub fn try_create(display_name: &str, _args: &[&DataTypeImpl]) -> Result<Box<dyn Function>> {
        Ok(Box::new(TypeOfFunction {
            _display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create)).features(
            FunctionFeatures::default()
                .deterministic()
                .disable_passthrough_null()
                .num_arguments(1),
        )
    }
}

impl Function for TypeOfFunction {
    fn name(&self) -> &str {
        "TypeOfFunction"
    }

    fn return_type(&self) -> DataTypeImpl {
        StringType::new_impl()
    }

    fn eval(
        &self,
        _func_ctx: FunctionContext,
        columns: &[ColumnRef],
        input_rows: usize,
    ) -> Result<ColumnRef> {
        let non_null_type = remove_nullable(&columns[0].data_type());
        let type_name = format_data_type_sql(&non_null_type);
        let value = DataValue::String(type_name.as_bytes().to_vec());
        let data_type = StringType::new_impl();
        value.as_const_column(&data_type, input_rows)
    }
}

impl fmt::Display for TypeOfFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "typeof")
    }
}
