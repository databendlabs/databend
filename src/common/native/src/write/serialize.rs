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

use databend_common_column::bitmap::Bitmap;
use databend_common_column::buffer::Buffer;
use databend_common_column::types::i256;
use databend_common_expression::Column;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalColumn;
use databend_common_expression::types::GeographyColumn;
use databend_common_expression::types::NullableColumn;
use databend_common_expression::types::NumberColumn;
use databend_common_expression::types::ValueType;
use databend_common_expression::visitor::ValueVisitor;
use databend_common_expression::with_number_mapped_type;

use super::WriteOptions;
use super::boolean::write_bitmap;
use crate::error::Error;
use crate::error::Result;
use crate::nested::Nested;
use crate::util::encode_bool;
use crate::write::binary::write_binary;
use crate::write::primitive::write_primitive;
use crate::write::serialize::Nested::LargeList;
use crate::write::view::write_view;

/// Writes an [`Array`] to the file
pub fn write<W: Write>(
    w: &mut W,
    column: &Column,
    nested: &[Nested],
    write_options: WriteOptions,
    scratch: &mut Vec<u8>,
) -> Result<()> {
    write_nest_info(w, nested)?;

    let mut visitor = WriteVisitor::new(w, write_options, scratch);
    visitor.visit_column(column.clone())?;
    Ok(())
}

struct WriteVisitor<'a, W: Write> {
    w: &'a mut W,
    write_options: WriteOptions,
    scratch: &'a mut Vec<u8>,
    validity: Option<Bitmap>,
}

impl<'a, W: Write> WriteVisitor<'a, W> {
    fn new(w: &'a mut W, write_options: WriteOptions, scratch: &'a mut Vec<u8>) -> Self {
        Self {
            w,
            write_options,
            scratch,
            validity: None,
        }
    }
}

impl<'a, W: Write> ValueVisitor for WriteVisitor<'a, W> {
    type Error = Error;
    fn visit_scalar(&mut self, _scalar: databend_common_expression::Scalar) -> Result<()> {
        unreachable!()
    }

    fn visit_typed_column<T: ValueType>(&mut self, _: T::Column, _: &DataType) -> Result<()> {
        unreachable!()
    }

    fn visit_any_number(&mut self, column: NumberColumn) -> Result<()> {
        with_number_mapped_type!(|NUM_TYPE| match column {
            NumberColumn::NUM_TYPE(column) => {
                write_primitive::<NUM_TYPE, W>(
                    self.w,
                    &column,
                    self.validity.clone(),
                    &self.write_options,
                    self.scratch,
                )
            }
        })
    }

    fn visit_any_decimal(&mut self, column: DecimalColumn) -> Result<()> {
        match column {
            DecimalColumn::Decimal64(buffer, _) => write_primitive::<_, W>(
                self.w,
                &buffer,
                self.validity.clone(),
                &self.write_options,
                self.scratch,
            ),
            DecimalColumn::Decimal128(column, _) => write_primitive::<_, W>(
                self.w,
                &column,
                self.validity.clone(),
                &self.write_options,
                self.scratch,
            ),
            DecimalColumn::Decimal256(column, _) => {
                let column: Buffer<i256> = unsafe { std::mem::transmute(column) };
                write_primitive::<i256, W>(
                    self.w,
                    &column,
                    self.validity.clone(),
                    &self.write_options,
                    self.scratch,
                )
            }
        }
    }

    fn visit_nullable(&mut self, column: Box<NullableColumn<AnyType>>) -> Result<()> {
        let v = self.validity.replace(column.validity);
        assert!(v.is_none());
        self.visit_column(column.column)
    }

    fn visit_column(&mut self, column: Column) -> Result<()> {
        match column {
            Column::Null { .. } | Column::EmptyArray { .. } | Column::EmptyMap { .. } => Ok(()),

            Column::Boolean(column) => write_bitmap(
                self.w,
                &column,
                self.validity.clone(),
                &self.write_options,
                self.scratch,
            ),
            Column::String(column) => write_view::<W>(
                self.w,
                &column.to_binview(),
                self.validity.clone(),
                &self.write_options,
                self.scratch,
            ),
            Column::Timestamp(column) => write_primitive::<i64, W>(
                self.w,
                &column,
                self.validity.clone(),
                &self.write_options,
                self.scratch,
            ),
            Column::Date(column) => write_primitive::<i32, W>(
                self.w,
                &column,
                self.validity.clone(),
                &self.write_options,
                self.scratch,
            ),
            Column::TimestampTz(column) => {
                let column: Buffer<i128> = unsafe { std::mem::transmute(column) };
                write_primitive::<i128, W>(
                    self.w,
                    &column,
                    self.validity.clone(),
                    &self.write_options,
                    self.scratch,
                )
            }
            Column::Interval(column) => {
                let column: Buffer<i128> = unsafe { std::mem::transmute(column) };
                write_primitive::<i128, W>(
                    self.w,
                    &column,
                    self.validity.clone(),
                    &self.write_options,
                    self.scratch,
                )
            }

            Column::Binary(b)
            | Column::Bitmap(b)
            | Column::Variant(b)
            | Column::Geography(GeographyColumn(b))
            | Column::Geometry(b) => write_binary::<W>(
                self.w,
                &b,
                self.validity.clone(),
                self.write_options.clone(),
                self.scratch,
            ),
            _ => Self::default_visit_column(column, self),
        }
    }
}

fn write_nest_info<W: Write>(w: &mut W, nesteds: &[Nested]) -> Result<()> {
    let is_simple = nesteds.len() == 1;

    if is_simple {
        let nest = nesteds.last().unwrap();

        if nest.is_nullable() {
            let validity = nest.validity();
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
            let validity = nested.validity();

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

            match nested {
                LargeList(_) => {
                    let offsets = nested.offsets().unwrap();
                    w.write_all(&(offsets.len() as u32).to_le_bytes())?;
                    let input_buf: &[u8] = bytemuck::cast_slice(&offsets);
                    w.write_all(input_buf)?;
                }
                Nested::FixedList(fixed_list) => {
                    w.write_all(&(fixed_list.length as u32).to_le_bytes())?;
                    w.write_all(&(fixed_list.dimension as u32).to_le_bytes())?;
                }
                _ => {}
            }
        }
    }

    Ok(())
}
