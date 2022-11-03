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

use crate::scalars::assert_string;
use crate::scalars::cast_column_field;
use crate::scalars::Function;
use crate::scalars::FunctionContext;
use crate::scalars::FunctionDescription;
use crate::scalars::FunctionFeatures;

#[derive(Clone)]
pub struct SubstringUtf8Function {
    display_name: String,
}

impl SubstringUtf8Function {
    pub fn try_create(display_name: &str, args: &[&DataTypeImpl]) -> Result<Box<dyn Function>> {
        assert_string(args[0])?;

        if !args[1].data_type_id().is_integer() && !args[1].data_type_id().is_string() {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected integer or string or null, but got {}",
                args[1].data_type_id()
            )));
        }

        if args.len() > 2
            && !args[2].data_type_id().is_integer()
            && !args[2].data_type_id().is_string()
        {
            return Err(ErrorCode::IllegalDataType(format!(
                "Expected integer or string or null, but got {}",
                args[2].data_type_id()
            )));
        }

        Ok(Box::new(SubstringUtf8Function {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create)).features(
            FunctionFeatures::default()
                .deterministic()
                .variadic_arguments(2, 3),
        )
    }
}

impl Function for SubstringUtf8Function {
    fn name(&self) -> &str {
        &self.display_name
    }

    fn return_type(&self) -> DataTypeImpl {
        StringType::new_impl()
    }

    fn eval(
        &self,
        func_ctx: FunctionContext,
        columns: &ColumnsWithField,
        input_rows: usize,
    ) -> Result<ColumnRef> {
        let s_column = cast_column_field(
            &columns[0],
            columns[0].data_type(),
            &StringType::new_impl(),
            &func_ctx,
        )?;
        let s_viewer = Vu8::try_create_viewer(&s_column)?;

        let p_column = cast_column_field(
            &columns[1],
            columns[1].data_type(),
            &Int64Type::new_impl(),
            &func_ctx,
        )?;
        let p_viewer = i64::try_create_viewer(&p_column)?;

        let mut builder = MutableStringColumn::with_capacity(input_rows);

        if columns.len() > 2 {
            let p2_column = cast_column_field(
                &columns[2],
                columns[2].data_type(),
                &UInt64Type::new_impl(),
                &func_ctx,
            )?;
            let p2_viewer = u64::try_create_viewer(&p2_column)?;

            let iter = izip!(s_viewer, p_viewer, p2_viewer);

            for (str, pos, len) in iter {
                let str = std::str::from_utf8(str)?;
                substr(&mut builder, str, pos, len);
            }
        } else {
            let iter = s_viewer.iter().zip(p_viewer.iter());

            for (str, pos) in iter {
                let str = std::str::from_utf8(str)?;
                substr(&mut builder, str, pos, u64::MAX);
            }
        }
        Ok(builder.finish().arc())
    }
}

impl fmt::Display for SubstringUtf8Function {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

#[inline]
fn substr(builder: &mut MutableStringColumn, str: &str, pos: i64, len: u64) {
    if pos == 0 || len == 0 {
        builder.commit_row();
        return;
    }
    let char_len = str.chars().count();
    let start = if pos > 0 {
        (pos - 1).min(char_len as i64) as usize
    } else {
        char_len
            .checked_sub(pos.unsigned_abs() as usize)
            .unwrap_or(char_len)
    };

    builder.write_from_char_iter(str.chars().skip(start).take(len as usize));
    builder.commit_row();
}
