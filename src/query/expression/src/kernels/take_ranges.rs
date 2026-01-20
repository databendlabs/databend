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

use core::ops::Range;

use binary::BinaryColumnBuilder;
use databend_common_base::vec_ext::VecExt;
use databend_common_column::bitmap::Bitmap;
use databend_common_column::bitmap::MutableBitmap;
use databend_common_column::buffer::Buffer;
use databend_common_exception::Result;

use crate::BlockEntry;
use crate::Column;
use crate::ColumnBuilder;
use crate::DataBlock;
use crate::TakeIndex;
use crate::Value;
use crate::types::binary::BinaryColumn;
use crate::types::nullable::NullableColumn;
use crate::types::string::StringColumn;
use crate::types::*;
use crate::visitor::ValueVisitor;
use crate::with_opaque_mapped_type;

impl DataBlock {
    // Generate a new `DataBlock` by the specified indices ranges.
    // ranges already cover most data
    pub fn take_ranges(self, ranges: &[Range<u32>], num_rows: usize) -> Result<DataBlock> {
        debug_assert_eq!(
            ranges
                .iter()
                .map(|range| range.end - range.start)
                .sum::<u32>() as usize,
            num_rows
        );

        let mut taker = TakeRangeVisitor::new(ranges, num_rows);
        let after_columns = self
            .columns()
            .iter()
            .map(|entry| {
                taker.visit_value(entry.value())?;
                let result = taker.result.take().unwrap();
                Ok(BlockEntry::new(result, || (entry.data_type(), num_rows)))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(DataBlock::new_with_meta(
            after_columns,
            num_rows,
            self.get_meta().cloned(),
        ))
    }
}

struct TakeRangeVisitor<'a> {
    ranges: &'a [Range<u32>],
    num_rows: usize,
    result: Option<Value<AnyType>>,
}

impl<'a> TakeRangeVisitor<'a> {
    fn new(ranges: &'a [Range<u32>], num_rows: usize) -> Self {
        Self {
            ranges,
            num_rows,
            result: None,
        }
    }
}

impl ValueVisitor for TakeRangeVisitor<'_> {
    fn visit_scalar(&mut self, scalar: crate::Scalar) -> Result<()> {
        self.result = Some(Value::Scalar(scalar));
        Ok(())
    }

    fn visit_column(&mut self, column: Column) -> Result<()> {
        Self::visit_column_use_simple_type(column, self)
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

    fn visit_typed_column<T: ValueType>(
        &mut self,
        column: T::Column,
        data_type: &DataType,
    ) -> Result<()> {
        let mut builder = ColumnBuilder::with_capacity(data_type, T::column_len(&column));
        let mut inner_builder = T::downcast_builder(&mut builder);

        for range in self.ranges {
            for index in range.iter() {
                inner_builder.push_item(unsafe { T::index_column_unchecked(&column, index) });
            }
        }
        drop(inner_builder);
        self.result = Some(Value::Column(builder.build()));
        Ok(())
    }

    fn visit_simple_type<T: simple_type::SimpleType>(
        &mut self,
        buffer: Buffer<T::Scalar>,
        data_type: &DataType,
    ) -> Result<()> {
        self.result = Some(Value::Column(T::upcast_column(
            self.take_primitive_types(buffer),
            data_type,
        )));
        Ok(())
    }

    fn visit_boolean(&mut self, bitmap: Bitmap) -> Result<()> {
        // Fast path: avoid iterating column to generate a new bitmap.
        // If this [`Bitmap`] is all true or all false and `num_rows <= bitmap.len()``,
        // we can just slice it.
        if self.num_rows <= bitmap.len()
            && (bitmap.null_count() == 0 || bitmap.null_count() == bitmap.len())
        {
            self.result = Some(Value::Column(BooleanType::upcast_column(
                bitmap.sliced(0, self.num_rows),
            )));
            return Ok(());
        }

        let mut builder = MutableBitmap::with_capacity(self.num_rows);
        let src = bitmap.values();
        let offset = bitmap.offset();
        self.ranges.iter().for_each(|range| {
            let start = range.start as usize;
            let end = range.end as usize;
            builder.append_packed_range(start + offset..end + offset, src)
        });

        self.result = Some(Value::Column(BooleanType::upcast_column(builder.into())));
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

    fn visit_opaque(&mut self, column: OpaqueColumn) -> Result<()> {
        self.result = Some(Value::Column(
            (with_opaque_mapped_type!(|T| match column {
                OpaqueColumn::T(buffer) => {
                    OpaqueType::<T>::upcast_column(self.take_primitive_types(buffer))
                }
            })),
        ));
        Ok(())
    }
}

impl TakeRangeVisitor<'_> {
    fn take_primitive_types<T: Copy>(&mut self, buffer: Buffer<T>) -> Buffer<T> {
        let mut builder: Vec<T> = Vec::with_capacity(self.num_rows);
        let values = buffer.as_slice();
        for range in self.ranges {
            range.take_primitive_types(values, &mut builder);
        }
        builder.into()
    }

    fn take_binary_types(&mut self, values: &BinaryColumn) -> BinaryColumn {
        let mut builder = BinaryColumnBuilder::with_capacity(self.num_rows, 0);
        for range in self.ranges {
            for index in range.start as usize..range.end as usize {
                let value = unsafe { values.index_unchecked(index) };
                builder.put_slice(value);
                builder.commit_row();
            }
        }
        builder.build()
    }

    fn take_string_types(&mut self, col: &StringColumn) -> StringColumn {
        let new_views = self.take_primitive_types(col.views().clone());
        unsafe {
            StringColumn::new_unchecked_unknown_md(new_views, col.data_buffers().clone(), None)
        }
    }
}
