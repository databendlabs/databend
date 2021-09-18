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

use common_datavalues::prelude::*;
use common_exception::Result;

use crate::scalars::Function;

#[derive(Clone)]
pub struct RoundFunction {
    display_name: String,
    round: u32,
}

impl RoundFunction {
    pub fn try_create(display_name: &str, round: u32) -> Result<Box<dyn Function>> {
        let s = Self {
            display_name: display_name.to_owned(),
            round,
        };

        Ok(Box::new(s))
    }

    // TODO: (sundy-li)
    // Consider about the timezones/offsets
    // Currently: assuming timezone offset is a multiple of round.
    #[inline]
    fn execute(&self, time: u32) -> u32 {
        time / self.round * self.round
    }
}

impl Function for RoundFunction {
    fn name(&self) -> &str {
        self.display_name.as_str()
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::DateTime32(None))
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn> {
        match columns[0].column() {
            DataColumn::Array(array) => {
                let array = array.u32()?;
                let arr = array.apply(|x| self.execute(x));
                Ok(DataColumn::Array(arr.into_series()))
            }
            DataColumn::Constant(v, rows) => {
                if v.is_null() {
                    return Ok(DataColumn::Constant(DataValue::UInt32(None), *rows));
                }
                let value = v.as_u64()?;
                Ok(DataColumn::Constant(
                    DataValue::UInt32(Some(self.execute(value as u32))),
                    *rows,
                ))
            }
        }
    }

    fn num_arguments(&self) -> usize {
        1
    }
}

impl fmt::Display for RoundFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
