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

use common_datavalues2::prelude::*;
use common_exception::Result;

use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function2;
use crate::scalars::Function2Description;

#[derive(Clone)]
pub struct IsNotNullFunction {
    _display_name: String,
}

impl IsNotNullFunction {
    pub fn try_create_func(_display_name: &str) -> Result<Box<dyn Function2>> {
        Ok(Box::new(IsNotNullFunction {
            _display_name: "isNotNull".to_string(),
        }))
    }

    pub fn desc() -> Function2Description {
        Function2Description::creator(Box::new(Self::try_create_func)).features(
            FunctionFeatures::default()
                .deterministic()
                .negative_function("isnull")
                .bool_function()
                .num_arguments(1),
        )
    }
}

impl Function2 for IsNotNullFunction {
    fn name(&self) -> &str {
        "IsNotNullFunction"
    }

    fn return_type(
        &self,
        _args: &[&common_datavalues2::DataTypePtr],
    ) -> Result<common_datavalues2::DataTypePtr> {
        Ok(bool::to_data_type())
    }

    fn eval(
        &self,
        columns: &common_datavalues2::ColumnsWithField,
        input_rows: usize,
    ) -> Result<common_datavalues2::ColumnRef> {
        let (all_null, validity) = columns[0].column().validity();
        if all_null {
            return Ok(ConstColumn::new(Series::from_data(vec![false]), input_rows).arc());
        }

        match validity {
            Some(validity) => Ok(BooleanColumn::from_arrow_data(validity.clone()).arc()),
            None => Ok(ConstColumn::new(Series::from_data(vec![true]), input_rows).arc()),
        }
    }

    fn passthrough_null(&self) -> bool {
        false
    }
}

impl std::fmt::Display for IsNotNullFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "isNotNull")
    }
}
