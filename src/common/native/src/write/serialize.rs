// Copyright 2021 Datafuse Labs
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

use std::io::Write;

use databend_common_column::buffer::Buffer;
use databend_common_column::types::i256;
use databend_common_expression::types::DecimalColumn;
use databend_common_expression::types::GeographyColumn;
use databend_common_expression::types::NumberColumn;
use databend_common_expression::with_decimal_mapped_type;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::Column;

use super::boolean::write_bitmap;
use super::WriteOptions;
use crate::error::Result;
use crate::nested::Nested;
use crate::util::encode_bool;
use crate::write::binary::write_binary;
use crate::write::primitive::write_primitive;
use crate::write::view::write_view;
/// Writes an [`Array`] to the file
pub fn write<W: Write>(
    w: &mut W,
    column: &Column,
    nested: &[Nested],
    write_options: WriteOptions,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    write_nest_info::<W>(w, nested)?;

    let (_, validity) = column.validity();
    let validity = validity.cloned();

    match column.remove_nullable() {
        Column::Null { .. } | Column::EmptyArray { .. } | Column::EmptyMap { .. } => Ok(()),
        Column::Number(column) => {
            with_number_mapped_type!(|NUM_TYPE| match column {
                NumberColumn::NUM_TYPE(column) => {
                    write_primitive::<NUM_TYPE, W>(w, &column, validity, write_options, scratch)
                }
            })
        }
        Column::Decimal(column) => with_decimal_mapped_type!(|DT| match column {
            DecimalColumn::DT(column, _) => {
                let column: Buffer<DT> = unsafe { std::mem::transmute(column) };
                write_primitive::<DT, W>(w, &column, validity, write_options, scratch)
            }
        }),
        Column::Boolean(column) => write_bitmap(w, &column, validity, write_options, scratch),
        Column::String(column) => write_view::<W>(w, &column.to_binview(), write_options, scratch),
        Column::Timestamp(column) => {
            write_primitive::<i64, W>(w, &column, validity, write_options, scratch)
        }
        Column::Date(column) => {
            write_primitive::<i32, W>(w, &column, validity, write_options, scratch)
        }
        Column::Interval(column) => {
            let column: Buffer<i128> = unsafe { std::mem::transmute(column) };
            write_primitive::<i128, W>(w, &column, validity, write_options, scratch)
        }
        Column::Binary(b)
        | Column::Bitmap(b)
        | Column::Variant(b)
        | Column::Geography(GeographyColumn(b))
        | Column::Geometry(b) => write_binary::<W>(w, &b, validity, write_options, scratch),

        Column::Tuple(_) | Column::Map(_) | Column::Array(_) | Column::Nullable(_) => {
            unreachable!()
        }
    }
}

fn write_nest_info<W: Write>(w: &mut W, nesteds: &[Nested]) -> Result<()> {
    let is_simple = nesteds.len() == 1;

    if is_simple {
        let nest = nesteds.last().unwrap();

        if nest.is_nullable() {
            let (_, validity) = nest.inner();
            if let Some(bitmap) = validity {
                w.write_all(&(bitmap.len() as u32).to_le_bytes())?;
                let (s, offset, _) = bitmap.as_slice();
                if offset == 0 {
                    w.write_all(s)?;
                } else {
                    encode_bool(w, bitmap.iter())?;
                }
            } else {
                w.write_all(&0u32.to_le_bytes())?;
            }
        }
    } else {
        for nested in nesteds {
            let (values, validity) = nested.inner();

            if nested.is_nullable() {
                if let Some(bitmap) = validity {
                    w.write_all(&(bitmap.len() as u32).to_le_bytes())?;
                    let (s, offset, _) = bitmap.as_slice();
                    if offset == 0 {
                        w.write_all(s)?;
                    } else {
                        encode_bool(w, bitmap.iter())?;
                    }
                } else {
                    w.write_all(&0u32.to_le_bytes())?;
                }
            }

            if nested.is_list() {
                w.write_all(&(values.len() as u32).to_le_bytes())?;
                let input_buf: &[u8] = bytemuck::cast_slice(&values);
                w.write_all(input_buf)?;
            }
        }
    }

    Ok(())
}
