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

use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::compute;
use common_datavalues::prelude::*;
use common_exception::Result;

use crate::scalars::Function;

#[derive(Clone)]
pub struct SubstringFunction {
    display_name: String,
}

impl SubstringFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(SubstringFunction {
            display_name: display_name.to_string(),
        }))
    }
}

impl Function for SubstringFunction {
    fn name(&self) -> &str {
        "substring"
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &[DataColumn], _input_rows: usize) -> Result<DataColumn> {
        // TODO: make this function support column value as arguments rather than literal
        let from_value = columns[1].try_get(0)?;
        let mut from = from_value.as_i64()?;

        if from >= 1 {
            from -= 1;
        }

        let mut end = None;
        if columns.len() >= 3 {
            end = Some(columns[2].try_get(0)?.as_u64()?);
        }

        // todo, move these to datavalues
        let value = columns[0].to_array()?;
        let arrow_array = value.get_array_ref();
        let result = compute::substring::substring(arrow_array.as_ref(), from, &end)?;
        let result: ArrayRef = Arc::from(result);
        Ok(result.into())
    }

    // substring(str, from)
    // substring(str, from, end)
    fn variadic_arguments(&self) -> Option<(usize, usize)> {
        Some((2, 3))
    }
}

impl fmt::Display for SubstringFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SUBSTRING")
    }
}
