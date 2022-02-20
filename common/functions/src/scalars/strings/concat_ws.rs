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

use crate::scalars::assert_string;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::Function;
use crate::scalars::FunctionDescription;

#[derive(Clone)]
pub struct ConcatWsFunction {
    _display_name: String,
}

impl ConcatWsFunction {
    pub fn try_create(display_name: &str) -> Result<Box<dyn Function>> {
        Ok(Box::new(ConcatWsFunction {
            _display_name: display_name.to_string(),
        }))
    }

    pub fn desc() -> FunctionDescription {
        FunctionDescription::creator(Box::new(Self::try_create)).features(
            FunctionFeatures::default()
                .deterministic()
                .variadic_arguments(2, 1024),
        )
    }

    fn concat_column_with_constant_seperator(
        sep: &[u8],
        columns: &[ColumnWithField],
        rows: usize,
    ) -> Result<ColumnRef> {
        let viewers = columns
            .iter()
            .map(|column| Vu8::try_create_viewer(column.column()))
            .collect::<Result<Vec<_>>>()?;

        let mut builder = MutableStringColumn::with_capacity(rows);
        let mut buffer: Vec<u8> = Vec::with_capacity(32);
        (0..rows).for_each(|row| {
            buffer.clear();
            for (idx, viewer) in viewers.iter().enumerate() {
                if !viewer.null_at(row) {
                    if idx > 0 {
                        buffer.extend_from_slice(sep);
                    }

                    buffer.extend_from_slice(viewer.value_at(row));
                }
            }
            builder.append_value(buffer.as_slice());
        });
        Ok(builder.to_column())
    }

    fn concat_column_nonull(
        sep_column: &ColumnWithField,
        columns: &[ColumnWithField],
        rows: usize,
    ) -> Result<ColumnRef> {
        let sep_c = Series::check_get_scalar::<Vu8>(sep_column.column())?;
        let viewers = columns
            .iter()
            .map(|column| Vu8::try_create_viewer(column.column()))
            .collect::<Result<Vec<_>>>()?;

        let mut builder = MutableStringColumn::with_capacity(rows);
        let mut buffer: Vec<u8> = Vec::with_capacity(32);
        (0..rows).for_each(|row| {
            buffer.clear();
            let sep = sep_c.get_data(row);
            for (idx, viewer) in viewers.iter().enumerate() {
                if !viewer.null_at(row) {
                    if idx > 0 {
                        buffer.extend_from_slice(sep);
                    }

                    buffer.extend_from_slice(viewer.value_at(row));
                }
            }
            builder.append_value(buffer.as_slice());
        });
        Ok(builder.to_column())
    }

    fn concat_column_null(
        sep_column: &ColumnWithField,
        columns: &[ColumnWithField],
        rows: usize,
    ) -> Result<ColumnRef> {
        let sep_viewer = Vu8::try_create_viewer(sep_column.column())?;
        let viewers = columns
            .iter()
            .map(|column| Vu8::try_create_viewer(column.column()))
            .collect::<Result<Vec<_>>>()?;

        let mut builder = NullableColumnBuilder::<Vu8>::with_capacity(rows);
        let mut buffer: Vec<u8> = Vec::with_capacity(32);
        (0..rows).for_each(|row| {
            buffer.clear();
            if sep_viewer.null_at(row) {
                builder.append_null();
            }
            let sep = sep_viewer.value_at(row);
            for (idx, viewer) in viewers.iter().enumerate() {
                if !viewer.null_at(row) {
                    if idx > 0 {
                        buffer.extend_from_slice(sep);
                    }

                    buffer.extend_from_slice(viewer.value_at(row));
                }
            }
            builder.append(buffer.as_slice(), true);
        });
        Ok(builder.build(rows))
    }
}

// https://dev.mysql.com/doc/refman/8.0/en/string-functions.html#function_concat-ws
// concat_ws(NULL, "a", "b") -> "NULL"
// concat_ws(",", NULL, NULL) -> ""
// So we recusive call: concat_ws, if will skip nullvalues
impl Function for ConcatWsFunction {
    fn name(&self) -> &str {
        "concat_ws"
    }

    fn return_type(&self, args: &[&DataTypePtr]) -> Result<DataTypePtr> {
        if args[0].is_null() {
            return Ok(NullType::arc());
        }

        for arg in args {
            let arg = remove_nullable(*arg);
            if !arg.is_null() {
                assert_string(&arg)?;
            }
        }

        let dt = Vu8::to_data_type();
        match args[0].is_nullable() {
            true => Ok(wrap_nullable(&dt)),
            false => Ok(dt),
        }
    }

    fn eval(&self, columns: &ColumnsWithField, input_rows: usize) -> Result<ColumnRef> {
        let seperator = &columns[0];
        if seperator.data_type().is_null() {
            return Ok(NullColumn::new(input_rows).arc());
        }

        // remove other null columns
        let cols: Vec<ColumnWithField> = columns[1..]
            .iter()
            .filter(|c| !c.data_type().is_null())
            .cloned()
            .collect();

        let viewer = Vu8::try_create_viewer(columns[0].column())?;
        if seperator.column().is_const() {
            if viewer.null_at(0) {
                return Ok(NullColumn::new(input_rows).arc());
            }
            return Self::concat_column_with_constant_seperator(
                viewer.value_at(0),
                &cols,
                input_rows,
            );
        }

        match columns[0].data_type().is_nullable() {
            false => Self::concat_column_nonull(&columns[0], &cols, input_rows),
            true => Self::concat_column_null(&columns[0], &cols, input_rows),
        }
    }

    fn passthrough_null(&self) -> bool {
        false
    }
}

impl fmt::Display for ConcatWsFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CONCAT_WS")
    }
}
