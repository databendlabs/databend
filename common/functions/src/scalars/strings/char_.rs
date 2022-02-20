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

use crate::scalars::assert_numeric;
use crate::scalars::default_column_cast;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;

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
        FunctionDescription::creator(Box::new(Self::try_create)).features(
            FunctionFeatures::default()
                .deterministic()
                .variadic_arguments(1, 1024),
        )
    }
}

impl Function for CharFunction {
    fn name(&self) -> &str {
        "char"
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        for arg in args {
            assert_numeric(*arg)?;
        }
        Ok(Vu8::to_data_type())
    }

    fn eval(&self, columns: &ColumnsWithField, input_rows: usize) -> Result<ColumnRef> {
        let column_count = columns.len();
        let mut values: Vec<u8> = vec![0; input_rows * column_count];
        let values_ptr = values.as_mut_ptr();

        let mut offsets: Vec<i64> = Vec::with_capacity(input_rows + 1);
        offsets.push(0);

        let u8_type = u8::to_data_type();
        for (i, column) in columns.iter().enumerate() {
            let column = column.column();
            let column = default_column_cast(column, &u8_type)?;

            match column.is_const() {
                false => {
                    let u8_c = Series::check_get_scalar::<u8>(&column)?;
                    for (j, ch) in u8_c.iter().enumerate() {
                        unsafe {
                            *values_ptr.add(column_count * j + i) = *ch;
                        }
                    }
                }
                true => unsafe {
                    let value = column.get_u64(0)? as u8;
                    for j in 0..input_rows {
                        *values_ptr.add(column_count * j + i) = value;
                    }
                },
            }
        }
        for i in 1..input_rows + 1 {
            offsets.push(i as i64 * column_count as i64);
        }
        let mut builder = MutableStringColumn::from_data(values, offsets);
        Ok(builder.to_column())
    }
}

impl fmt::Display for CharFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CHAR")
    }
}
