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
pub struct CharFunction {
    _display_name: String,
}

impl CharFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(CharFunction {
            _display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic())
    }
}

impl Function for CharFunction {
    fn name(&self) -> &str {
        "char"
    }

    fn variadic_arguments(&self) -> Option<(usize, usize)> {
        Some((1, 1024))
    }

    fn return_type(&self, _args: &[DataType]) -> Result<DataType> {
        Ok(DataType::String)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn eval(&self, columns: &DataColumnsWithField, _input_rows: usize) -> Result<DataColumn> {
        let len = columns[0].column().len();
        let mut l_array = DFStringArray::full(&[], len);

        for column in columns.iter() {
            let column = column.column();
            if column.data_type().is_null() {
                continue;
            }
            let column = column.cast_with_type(&DataType::UInt32)?;
            match column {
                DataColumn::Array(r_array) => {
                    let r_array = r_array.u32()?;
                    l_array = DFStringArray::new_from_iter_validity(
                        l_array
                            .into_no_null_iter()
                            .zip(r_array.into_no_null_iter())
                            .map(|(l, r)| {
                                [
                                    l,
                                    &r.to_be_bytes()
                                        .iter()
                                        .filter(|&x| *x != 0u8)
                                        .cloned()
                                        .collect::<Vec<u8>>(),
                                ]
                                .concat()
                            }),
                        combine_validities(l_array.inner().validity(), r_array.inner().validity()),
                    );
                }
                DataColumn::Constant(r_array, _) => match r_array {
                    DataValue::UInt32(Some(constant_uint)) => {
                        let mut builder = StringArrayBuilder::with_capacity(len);
                        let iter = l_array.into_no_null_iter();
                        for (_, val) in iter.enumerate() {
                            builder.append_value(
                                [
                                    val,
                                    &constant_uint
                                        .to_be_bytes()
                                        .iter()
                                        .filter(|&x| *x != 0u8)
                                        .cloned()
                                        .collect::<Vec<u8>>(),
                                ]
                                .concat(),
                            );
                        }
                        l_array = builder.finish();
                    }
                    _ => {
                        return Ok(DataColumn::Array(
                            DFStringArray::full_null(len).into_series(),
                        ));
                    }
                },
            }
        }
        Ok(DataColumn::Array(l_array.into_series()))
    }
}

impl fmt::Display for CharFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CHAR")
    }
}
