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
use crate::kernels::has_nested_decimal;
use crate::types::binary::BinaryColumn;
use crate::types::nullable::NullableColumn;
use crate::types::string::StringColumn;
use crate::types::string::StringColumnBuilder;
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

        let mut taker = TakeRangeVisitor::new(ranges, num_rows, false);
        let after_columns = self
            .columns()
            .iter()
            .map(|entry| taker.take_entry(entry))
            .collect::<Result<Vec<_>>>()?;

        Ok(DataBlock::new_with_meta(
            after_columns,
            num_rows,
            self.get_meta().cloned(),
        ))
    }

    pub fn merge_indices_to_ranges(indices: &[u32]) -> Vec<Range<u32>> {
        debug_assert!(indices.is_sorted());
        let mut ranges: Vec<Range<u32>> = Vec::with_capacity(indices.len() / 2);
        for &index in indices {
            if let Some(cur) = ranges.last_mut()
                && cur.end == index
            {
                cur.end += 1;
            } else {
                ranges.push(index..index + 1)
            }
        }
        ranges
    }
}

/// Whether `column` has a decimal child whose width is not the strict width
/// of its precision. Such a child cannot be appended in bulk to a builder,
/// which has the strict width; pushing row by row converts it. An encoded
/// child is reported too, since the width of its decoded values is not known
/// here.
fn has_non_strict_child(column: &Column) -> bool {
    match column {
        Column::Decimal(decimal) => !decimal.is_strict_decimal(),
        Column::Nullable(nullable) => has_non_strict_child(nullable.column()),
        Column::Array(array) | Column::Map(array) => has_non_strict_child(array.values()),
        Column::Tuple(fields) => fields.iter().any(has_non_strict_child),
        Column::Encoded(_) => true,
        _ => false,
    }
}

pub(super) struct TakeRangeVisitor<'a> {
    ranges: &'a [Range<u32>],
    num_rows: usize,
    /// Copies `String` values instead of sharing the data buffers.
    optimize_size: bool,
    result: Option<Value<AnyType>>,
}

impl<'a> TakeRangeVisitor<'a> {
    pub(super) fn new(ranges: &'a [Range<u32>], num_rows: usize, optimize_size: bool) -> Self {
        Self {
            ranges,
            num_rows,
            optimize_size,
            result: None,
        }
    }

    pub(super) fn take_entry(&mut self, entry: &BlockEntry) -> Result<BlockEntry> {
        self.visit_value(entry.value())?;
        let result = self.result.take().unwrap();
        Ok(BlockEntry::new(result, || {
            (entry.data_type(), self.num_rows)
        }))
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
        let (column, validity) = column.destructure();
        self.visit_boolean(validity)?;
        let validity =
            BooleanType::try_downcast_column(self.result.take().unwrap().as_column().unwrap())
                .unwrap();

        self.visit_column(column)?;
        let result = self.result.take().unwrap();
        let result = result.as_column().unwrap();
        self.result = Some(Value::Column(result.clone().wrap_nullable(Some(validity))));
        Ok(())
    }

    fn visit_encoded(&mut self, mut column: Box<EncodedColumn>) -> Result<()> {
        *column = column.map_codes(|codes| {
            crate::map_narrow_buffer!(&codes, |buffer| {
                self.ranges
                    .iter()
                    .flat_map(|range| {
                        buffer.as_slice()[range.start as usize..range.end as usize]
                            .iter()
                            .copied()
                    })
                    .collect::<Vec<_>>()
                    .into()
            })
        });
        self.result = Some(Value::Column(Column::Encoded(column)));
        Ok(())
    }

    fn visit_typed_column<T: ValueType>(
        &mut self,
        column: T::Column,
        data_type: &DataType,
    ) -> Result<()> {
        // Only a nested type can hold a non-strict decimal child: a top-level
        // decimal is taken by `visit_simple_type`.
        let copy_runs = !has_nested_decimal(data_type)
            || !has_non_strict_child(&T::upcast_column_with_type(column.clone(), data_type));

        let mut builder = ColumnBuilder::with_capacity(data_type, self.num_rows);
        let mut inner_builder = T::downcast_builder(&mut builder);

        for range in self.ranges {
            let (start, end) = (range.start as usize, range.end as usize);
            if copy_runs && end - start > 1 {
                inner_builder.append_column(&T::slice_column(&column, start..end));
            } else {
                for index in start..end {
                    inner_builder.push_item(unsafe { T::index_column_unchecked(&column, index) });
                }
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

    /// Copies the bytes and the shifted offsets of each range at once.
    fn take_binary_types(&mut self, values: &BinaryColumn) -> BinaryColumn {
        let offsets = values.offsets().as_slice();
        let bytes = values.data().as_slice();
        let data_len = self
            .ranges
            .iter()
            .map(|range| (offsets[range.end as usize] - offsets[range.start as usize]) as usize)
            .sum();

        let mut data = Vec::with_capacity(data_len);
        let mut new_offsets = Vec::with_capacity(self.num_rows + 1);
        new_offsets.push(0);
        for range in self.ranges {
            let (start, end) = (range.start as usize, range.end as usize);
            let (first, last) = (offsets[start], offsets[end]);
            let base = data.len() as u64;
            data.extend_from_slice(&bytes[first as usize..last as usize]);
            if end - start == 1 {
                new_offsets.push(data.len() as u64);
            } else {
                new_offsets.extend(
                    offsets[start + 1..=end]
                        .iter()
                        .map(|offset| base + (offset - first)),
                );
            }
        }
        BinaryColumn::new(data.into(), new_offsets.into())
    }

    fn take_string_types(&mut self, col: &StringColumn) -> StringColumn {
        if self.optimize_size {
            let mut builder = StringColumnBuilder::with_capacity(self.num_rows);
            for range in self.ranges {
                for index in range.start as usize..range.end as usize {
                    unsafe {
                        builder.put_and_commit(col.index_unchecked(index));
                    }
                }
            }
            return builder.build();
        }

        let new_views = self.take_primitive_types(col.views().clone());
        unsafe {
            StringColumn::new_unchecked_unknown_md(new_views, col.data_buffers().clone(), None)
        }
    }
}
