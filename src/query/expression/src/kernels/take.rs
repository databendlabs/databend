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

use std::sync::Arc;

use binary::BinaryColumnBuilder;
use databend_common_arrow::arrow::array::Array;
use databend_common_arrow::arrow::array::Utf8ViewArray;
use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_exception::Result;
use string::StringColumnBuilder;

use crate::types::binary::BinaryColumn;
use crate::types::nullable::NullableColumn;
use crate::types::string::StringColumn;
use crate::types::*;
use crate::visitor::ValueVisitor;
use crate::BlockEntry;
use crate::ColumnBuilder;
use crate::DataBlock;
use crate::Value;
use crate::SELECTIVITY_THRESHOLD;

pub const BIT_MASK: [u8; 8] = [1, 2, 4, 8, 16, 32, 64, 128];

impl DataBlock {
    pub fn take<I>(&self, indices: &[I]) -> Result<Self>
    where I: databend_common_arrow::arrow::types::Index {
        if indices.is_empty() {
            return Ok(self.slice(0..0));
        }

        let mut taker = TakeVisitor::new(indices);

        let after_columns = self
            .columns()
            .iter()
            .map(|entry| {
                taker.visit_value(entry.value.clone())?;
                let result = taker.result.take().unwrap();
                Ok(BlockEntry {
                    value: result,
                    data_type: entry.data_type.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(DataBlock::new_with_meta(
            after_columns,
            indices.len(),
            self.get_meta().cloned(),
        ))
    }
}

struct TakeVisitor<'a, I>
where I: databend_common_arrow::arrow::types::Index
{
    indices: &'a [I],
    result: Option<Value<AnyType>>,
}

impl<'a, I> TakeVisitor<'a, I>
where I: databend_common_arrow::arrow::types::Index
{
    fn new(indices: &'a [I]) -> Self {
        Self {
            indices,
            result: None,
        }
    }
}

impl<'a, I> ValueVisitor for TakeVisitor<'a, I>
where I: databend_common_arrow::arrow::types::Index
{
    fn visit_scalar(&mut self, scalar: crate::Scalar) -> Result<()> {
        self.result = Some(Value::Scalar(scalar));
        Ok(())
    }

    fn visit_nullable(&mut self, column: Box<NullableColumn<AnyType>>) -> Result<()> {
        self.visit_boolean(column.validity.clone())?;
        let validity =
            BooleanType::try_downcast_column(self.result.take().unwrap().as_column().unwrap())
                .unwrap();

        self.visit_column(column.column)?;
        let result = self.result.take().unwrap();
        let result = result.as_column().unwrap();
        self.result = Some(Value::Column(NullableColumn::new_column(
            result.clone(),
            validity,
        )));
        Ok(())
    }

    fn visit_typed_column<T: ValueType>(&mut self, column: <T as ValueType>::Column) -> Result<()> {
        let c = T::upcast_column(column.clone());
        let builder = ColumnBuilder::with_capacity(&c.data_type(), c.len());
        let mut builder = T::try_downcast_owned_builder(builder).unwrap();

        for index in self.indices {
            T::push_item(&mut builder, unsafe {
                T::index_column_unchecked(&column, index.to_usize())
            });
        }
        self.result = Some(Value::Column(T::upcast_column(T::build_column(builder))));
        Ok(())
    }

    fn visit_number<T: Number>(
        &mut self,
        buffer: <NumberType<T> as ValueType>::Column,
    ) -> Result<()> {
        self.result = Some(Value::Column(NumberType::<T>::upcast_column(
            self.take_primitive_types(buffer),
        )));
        Ok(())
    }

    fn visit_timestamp(&mut self, buffer: Buffer<i64>) -> Result<()> {
        self.result = Some(Value::Column(TimestampType::upcast_column(
            self.take_primitive_types(buffer),
        )));
        Ok(())
    }

    fn visit_date(&mut self, buffer: Buffer<i32>) -> Result<()> {
        self.result = Some(Value::Column(DateType::upcast_column(
            self.take_primitive_types(buffer),
        )));
        Ok(())
    }

    fn visit_decimal<T: crate::types::Decimal>(
        &mut self,
        buffer: Buffer<T>,
        size: DecimalSize,
    ) -> Result<()> {
        self.result = Some(Value::Column(T::upcast_column(
            self.take_primitive_types(buffer),
            size,
        )));
        Ok(())
    }

    fn visit_boolean(&mut self, col: Bitmap) -> Result<()> {
        let num_rows = self.indices.len();
        // Fast path: avoid iterating column to generate a new bitmap.
        // If this [`Bitmap`] is all true or all false and `num_rows <= bitmap.len()``,
        // we can just slice it.
        if num_rows <= col.len() && (col.unset_bits() == 0 || col.unset_bits() == col.len()) {
            let mut bitmap = col.clone();
            bitmap.slice(0, num_rows);
            self.result = Some(Value::Column(BooleanType::upcast_column(bitmap)));
            return Ok(());
        }

        let capacity = num_rows.saturating_add(7) / 8;
        let mut builder: Vec<u8> = Vec::with_capacity(capacity);
        let mut unset_bits = 0;
        let mut value = 0;
        let mut i = 0;

        for index in self.indices.iter() {
            if col.get_bit(index.to_usize()) {
                value |= BIT_MASK[i % 8];
            } else {
                unset_bits += 1;
            }
            i += 1;
            if i % 8 == 0 {
                builder.push(value);
                value = 0;
            }
        }
        if i % 8 != 0 {
            builder.push(value);
        }

        let result = unsafe {
            Bitmap::from_inner(Arc::new(builder.into()), 0, num_rows, unset_bits)
                .ok()
                .unwrap()
        };
        self.result = Some(Value::Column(BooleanType::upcast_column(result)));
        Ok(())
    }

    fn visit_binary(&mut self, col: BinaryColumn) -> Result<()> {
        self.result = Some(Value::Column(BinaryType::upcast_column(
            self.take_binary_types(&col),
        )));
        Ok(())
    }

    fn visit_string(&mut self, column: StringColumn) -> Result<()> {
        self.result = Some(Value::Column(StringType::upcast_column(
            self.take_string_types(&column),
        )));
        Ok(())
    }

    fn visit_variant(&mut self, column: BinaryColumn) -> Result<()> {
        self.result = Some(Value::Column(VariantType::upcast_column(
            self.take_binary_types(&column),
        )));
        Ok(())
    }
}

impl<'a, I> TakeVisitor<'a, I>
where I: databend_common_arrow::arrow::types::Index
{
    fn take_primitive_types<T: Copy>(&mut self, buffer: Buffer<T>) -> Buffer<T> {
        let col = buffer.as_slice();
        let result: Vec<T> = self
            .indices
            .iter()
            .map(|index| unsafe { *col.get_unchecked(index.to_usize()) })
            .collect();
        result.into()
    }

    fn take_binary_types(&mut self, col: &BinaryColumn) -> BinaryColumn {
        let num_rows = self.indices.len();
        let mut builder = BinaryColumnBuilder::with_capacity(num_rows, 0);
        for index in self.indices.iter() {
            unsafe {
                builder.put_slice(col.index_unchecked(index.to_usize()));
                builder.commit_row();
            }
        }
        builder.build()
    }

    fn take_string_types(&mut self, col: &StringColumn) -> StringColumn {
        let num_rows = self.indices.len();

        if num_rows as f64 > col.len() as f64 * SELECTIVITY_THRESHOLD {
            // reuse the buffers
            let new_views = self.take_primitive_types(col.data.views().clone());
            let new_col = unsafe {
                Utf8ViewArray::new_unchecked_unknown_md(
                    col.data.data_type().clone(),
                    new_views,
                    col.data.data_buffers().clone(),
                    None,
                    Some(col.data.total_buffer_len()),
                )
            };
            StringColumn::new(new_col)
        } else {
            let mut builder = StringColumnBuilder::with_capacity(num_rows);
            for index in self.indices.iter() {
                unsafe {
                    builder.put_str(col.index_unchecked(index.to_usize()));
                    builder.commit_row();
                }
            }
            builder.build()
        }
    }
}
