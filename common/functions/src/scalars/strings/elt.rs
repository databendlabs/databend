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

use crate::scalars::function_factory::FunctionDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;

#[derive(Clone)]
pub struct EltFunction {
    display_name: String,
}

impl EltFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(EltFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic())
    }
}

impl Function for EltFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn num_arguments(&self) -> usize {
        0
    }

    fn variadic_arguments(&self) -> Option<(usize, usize)> {
        Some((2, usize::MAX - 1))
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(true)
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::String)
    }

    fn eval(&self, columns: &DataColumnsWithField, input_rows: usize) -> Result<DataColumn> {
        let n_column = columns[0].column().cast_with_type(&DataType::Int64)?;
        let r_column = match n_column {
            DataColumn::Constant(DataValue::Int64(num), _) => {
                if let Some(num) = num {
                    let n = num as usize;
                    if n > 0 && n < columns.len() {
                        columns[n].column().cast_with_type(&DataType::String)?
                    } else {
                        DataColumn::Constant(DataValue::Null, input_rows)
                    }
                } else {
                    DataColumn::Constant(DataValue::Null, input_rows)
                }
            }
            DataColumn::Array(n_series) => {
                let mut r_array = StringArrayBuilder::with_capacity(input_rows);
                for (i, on) in n_series.i64()?.iter().enumerate() {
                    if let Some(on) = on {
                        let n = *on as usize;
                        if n > 0 && n < columns.len() {
                            match columns[n].column().try_get(i) {
                                Ok(v) => r_array.append_value(v.as_string()?),
                                Err(_) => r_array.append_null(),
                            }
                        } else {
                            r_array.append_null();
                        }
                    } else {
                        r_array.append_null();
                    }
                }
                r_array.finish().into()
            }
            _ => DataColumn::Constant(DataValue::Null, input_rows),
        };
        Ok(r_column)
    }
}

impl fmt::Display for EltFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}
