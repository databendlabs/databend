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

use bytes::BufMut;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::scalars::assert_numeric;
use crate::scalars::assert_string;
use crate::scalars::cast_with_type;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;
use crate::scalars::DEFAULT_CAST_OPTIONS;

#[derive(Clone)]
pub struct ExportSetFunction {
    display_name: String,
}

impl ExportSetFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(Self {
            display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create)).features(
            FunctionFeatures::default()
                .deterministic()
                .variadic_arguments(3, 5),
        )
    }
}

impl Function for ExportSetFunction {
    fn name(&self) -> &str {
        &*self.display_name
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        assert_numeric(args[0])?;
        assert_string(args[1])?;
        assert_string(args[2])?;

        if args.len() >= 4 {
            assert_string(args[3])?;
        }

        if args.len() >= 5 {
            assert_numeric(args[4])?;
        }

        Ok(Vu8::to_data_type())
    }

    fn eval(&self, columns: &ColumnsWithField, input_rows: usize) -> Result<ColumnRef> {
        let sep_col = if columns.len() >= 4 {
            columns[3].column().clone()
        } else {
            ConstColumn::new(Series::from_data(vec![","]), input_rows).arc()
        };

        let number_bits_column = if columns.len() >= 5 {
            columns[4].column().clone()
        } else {
            ConstColumn::new(Series::from_data(vec![64u64]), input_rows).arc()
        };

        let t = u64::to_data_type();
        let number_bits_column = cast_with_type(
            &number_bits_column,
            &number_bits_column.data_type(),
            &t,
            &DEFAULT_CAST_OPTIONS,
        )?;

        let bits_column = cast_with_type(
            columns[0].column(),
            &columns[0].column().data_type(),
            &t,
            &DEFAULT_CAST_OPTIONS,
        )?;

        if input_rows != 1
            && (!number_bits_column.is_const() || !bits_column.is_const() || !sep_col.is_const())
        {
            return Err(ErrorCode::BadArguments(
                "Expected constant column for bits_column and separator_column and number_bits_column, column indexes: [0, 3, 4]".to_string(),
            ));
        }

        let b = bits_column.get_u64(0)?;
        let n = number_bits_column.get_u64(0)?;
        let n = std::cmp::min(n, 64) as usize;
        let s = sep_col.get_string(0)?;

        let on_viewer = Vu8::try_create_viewer(columns[1].column())?;
        let off_viewer = Vu8::try_create_viewer(columns[2].column())?;
        let sep_viewer = Vu8::try_create_viewer(&sep_col)?;

        let values_capacity =
            (std::cmp::max(on_viewer.len(), off_viewer.len()) + s.len()) * input_rows * n;

        let mut values: Vec<u8> = Vec::with_capacity(values_capacity);
        let mut offsets: Vec<i64> = Vec::with_capacity(input_rows + 1);
        offsets.push(0);
        for row in 0..input_rows {
            export_set(
                b,
                on_viewer.value_at(row),
                off_viewer.value_at(row),
                sep_viewer.value_at(row),
                n,
                &mut values,
            );
            offsets.push(values.len() as i64);
        }
        let mut builder = MutableStringColumn::from_data(values, offsets);
        Ok(builder.to_column())
    }
}

impl fmt::Display for ExportSetFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

#[inline]
fn export_set<'a>(
    bits: u64,
    on: &'a [u8],
    off: &'a [u8],
    sep: &'a [u8],
    n: usize,
    buffer: &mut Vec<u8>,
) {
    for n in 0..n {
        if n != 0 {
            buffer.put_slice(sep);
        }
        if (bits >> n & 1) == 0 {
            buffer.put_slice(off);
        } else {
            buffer.put_slice(on);
        }
    }
}
