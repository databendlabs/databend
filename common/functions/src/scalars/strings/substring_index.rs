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
use common_exception::ErrorCode;
use common_exception::Result;
use itertools::izip;

use crate::scalars::cast_column_field;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;

#[derive(Clone)]
pub struct SubstringIndexFunction {
    display_name: String,
}

impl SubstringIndexFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(SubstringIndexFunction {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create))
            .features(FunctionFeatures::default().deterministic().num_arguments(3))
    }
}

impl Function for SubstringIndexFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        if !args[0].data_type_id().is_numeric()
            && !args[0].data_type_id().is_string()
            && !args[0].data_type_id().is_null()
        {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected string or null, but got {}",
                args[0].data_type_id()
            )));
        }
        if !args[1].data_type_id().is_numeric()
            && !args[1].data_type_id().is_string()
            && !args[1].data_type_id().is_null()
        {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected integer or string or null, but got {}",
                args[1].data_type_id()
            )));
        }
        if !args[2].data_type_id().is_integer()
            && !args[2].data_type_id().is_string()
            && !args[2].data_type_id().is_null()
        {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected integer or string or null, but got {}",
                args[2].data_type_id()
            )));
        }
        Ok(StringType::arc())
    }

    fn eval(&self, columns: &ColumnsWithField, input_rows: usize) -> Result<ColumnRef> {
        let s_column = cast_column_field(&columns[0], &StringType::arc())?;
        let s_viewer = Vu8::try_create_viewer(&s_column)?;

        let d_column = cast_column_field(&columns[1], &StringType::arc())?;
        let d_viewer = Vu8::try_create_viewer(&d_column)?;

        let c_column = cast_column_field(&columns[2], &Int64Type::arc())?;
        let c_viewer = i64::try_create_viewer(&c_column)?;

        let iter = izip!(s_viewer, d_viewer, c_viewer);

        let mut builder = ColumnBuilder::<Vu8>::with_capacity(input_rows);

        for (str, delim, count) in iter {
            let val = substring_index(str, delim, &count);
            builder.append(val);
        }

        Ok(builder.build(input_rows))
    }
}

impl fmt::Display for SubstringIndexFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

#[inline]
fn substring_index<'a>(str: &'a [u8], delim: &'a [u8], count: &i64) -> &'a [u8] {
    if *count == 0 {
        return &str[0..0];
    }
    if *count > 0 {
        let count = (*count) as usize;
        let mut c = 0;
        for (p, w) in str.windows(delim.len()).enumerate() {
            if w == delim {
                c += 1;
                if c == count {
                    return &str[0..p];
                }
            }
        }
    } else {
        let count = (*count).abs() as usize;
        let mut c = 0;
        for (p, w) in str.windows(delim.len()).rev().enumerate() {
            if w == delim {
                c += 1;
                if c == count {
                    return &str[str.len() - p..];
                }
            }
        }
    }
    str
}
