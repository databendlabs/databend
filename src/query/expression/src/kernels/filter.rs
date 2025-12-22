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

use binary::BinaryColumnBuilder;
use databend_common_column::bitmap::Bitmap;
use databend_common_column::bitmap::MutableBitmap;
use databend_common_column::bitmap::TrueIdxIter;
use databend_common_column::bitmap::utils::SlicesIterator;
use databend_common_column::buffer::Buffer;
use databend_common_exception::Result;
use string::StringColumnBuilder;

use crate::BlockEntry;
use crate::Column;
use crate::ColumnBuilder;
use crate::DataBlock;
use crate::Value;
use crate::types::binary::BinaryColumn;
use crate::types::nullable::NullableColumn;
use crate::types::string::StringColumn;
use crate::types::*;
use crate::visitor::ValueVisitor;

impl DataBlock {
    pub fn filter_with_bitmap(self, bitmap: &Bitmap) -> Result<DataBlock> {
        if self.num_rows() == 0 {
            return Ok(self);
        }

        let count_zeros = bitmap.null_count();
        match count_zeros {
            0 => Ok(self),
            _ => {
                if count_zeros == self.num_rows() {
                    return Ok(self.slice(0..0));
                }

                let mut filter_visitor = FilterVisitor::new(bitmap);
                let after_columns = self
                    .columns()
                    .iter()
                    .map(|entry| {
                        filter_visitor.visit_value(entry.value())?;
                        let result = filter_visitor.result.take().unwrap();
                        Ok(BlockEntry::new(result, || {
                            (entry.data_type(), filter_visitor.filter_rows)
                        }))
                    })
                    .collect::<Result<Vec<_>>>()?;

                Ok(DataBlock::new_with_meta(
                    after_columns,
                    filter_visitor.filter_rows,
                    self.get_meta().cloned(),
                ))
            }
        }
    }

    pub fn filter_boolean_value(self, filter: &Value<BooleanType>) -> Result<DataBlock> {
        if self.num_rows() == 0 {
            return Ok(self);
        }

        match filter {
            Value::Scalar(s) => {
                if *s {
                    Ok(self)
                } else {
                    Ok(self.slice(0..0))
                }
            }
            Value::Column(bitmap) => Self::filter_with_bitmap(self, bitmap),
        }
    }
}

impl Column {
    pub fn filter(&self, bitmap: &Bitmap) -> Column {
        let mut filter_visitor = FilterVisitor::new(bitmap);
        filter_visitor
            .visit_value(Value::Column(self.clone()))
            .unwrap();
        filter_visitor
            .result
            .take()
            .unwrap()
            .as_column()
            .unwrap()
            .clone()
    }
}

/// The iteration strategy used to evaluate [`FilterVisitor`]
#[derive(Debug)]
pub enum IterationStrategy {
    None,
    All,
    /// Range iterator
    SlicesIterator,
    /// True index iterator
    IndexIterator,
}

/// based on <https://dl.acm.org/doi/abs/10.1145/3465998.3466009>
pub const SELECTIVITY_THRESHOLD: f64 = 0.8;

impl IterationStrategy {
    fn default_strategy(length: usize, true_count: usize) -> Self {
        if length == 0 || true_count == 0 {
            return IterationStrategy::None;
        }
        if length == true_count {
            return IterationStrategy::All;
        }
        let selectivity_frac = true_count as f64 / length as f64;
        if selectivity_frac > SELECTIVITY_THRESHOLD {
            return IterationStrategy::SlicesIterator;
        }
        IterationStrategy::IndexIterator
    }
}

pub struct FilterVisitor<'a> {
    filter: &'a Bitmap,
    result: Option<Value<AnyType>>,
    filter_rows: usize,
    original_rows: usize,
    strategy: IterationStrategy,
}

impl<'a> FilterVisitor<'a> {
    pub fn new(filter: &'a Bitmap) -> Self {
        let filter_rows = filter.len() - filter.null_count();
        let strategy = IterationStrategy::default_strategy(filter.len(), filter_rows);
        Self {
            filter,
            result: None,
            filter_rows,
            original_rows: filter.len(),
            strategy,
        }
    }

    pub fn with_strategy(mut self, strategy: IterationStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    pub fn take_result(&mut self) -> Option<Value<AnyType>> {
        self.result.take()
    }
}

impl ValueVisitor for FilterVisitor<'_> {
    fn visit_value(&mut self, value: Value<AnyType>) -> Result<()> {
        match value {
            Value::Scalar(c) => self.visit_scalar(c),
            Value::Column(c) => {
                assert_eq!(c.len(), self.original_rows);
                match self.strategy {
                    IterationStrategy::None => self.result = Some(Value::Column(c.slice(0..0))),
                    IterationStrategy::All => self.result = Some(Value::Column(c)),
                    IterationStrategy::SlicesIterator | IterationStrategy::IndexIterator => {
                        self.visit_column(c)?
                    }
                }
                Ok(())
            }
        }
    }

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

    fn visit_typed_column<T: ValueType>(
        &mut self,
        column: T::Column,
        data_type: &DataType,
    ) -> Result<()> {
        let c = T::upcast_column_with_type(column.clone(), data_type);
        let mut builder = ColumnBuilder::with_capacity(&c.data_type(), c.len());
        let mut inner_builder = T::downcast_builder(&mut builder);
        match self.strategy {
            IterationStrategy::IndexIterator => {
                let iter = TrueIdxIter::new(self.original_rows, Some(self.filter));
                iter.for_each(|index| {
                    inner_builder.push_item(unsafe { T::index_column_unchecked(&column, index) });
                });
            }
            _ => {
                let iter = SlicesIterator::new(self.filter);
                iter.for_each(|(start, len)| {
                    inner_builder.append_column(&T::slice_column(&column, start..start + len));
                });
            }
        }
        drop(inner_builder);
        self.result = Some(Value::Column(builder.build()));
        Ok(())
    }

    fn visit_number<T: Number>(&mut self, buffer: Buffer<T>) -> Result<()> {
        self.result = Some(Value::Column(NumberType::<T>::upcast_column(
            self.filter_primitive_types(buffer),
        )));
        Ok(())
    }

    fn visit_timestamp(&mut self, buffer: Buffer<i64>) -> Result<()> {
        self.result = Some(Value::Column(TimestampType::upcast_column(
            self.filter_primitive_types(buffer),
        )));
        Ok(())
    }

    fn visit_date(&mut self, buffer: Buffer<i32>) -> Result<()> {
        self.result = Some(Value::Column(DateType::upcast_column(
            self.filter_primitive_types(buffer),
        )));
        Ok(())
    }

    fn visit_decimal<T: crate::types::Decimal>(
        &mut self,
        buffer: Buffer<T>,
        size: DecimalSize,
    ) -> Result<()> {
        self.result = Some(Value::Column(T::upcast_column(
            self.filter_primitive_types(buffer),
            size,
        )));
        Ok(())
    }

    fn visit_boolean(&mut self, mut bitmap: Bitmap) -> Result<()> {
        // faster path for all bits set
        if bitmap.null_count() == 0 {
            bitmap.slice(0, self.filter_rows);
            self.result = Some(Value::Column(BooleanType::upcast_column(bitmap)));
            return Ok(());
        }

        let bitmap = match self.strategy {
            IterationStrategy::IndexIterator => {
                let iter = TrueIdxIter::new(self.original_rows, Some(self.filter));
                MutableBitmap::from_trusted_len_iter(iter.map(|index| bitmap.get_bit(index))).into()
            }
            _ => {
                let src = bitmap.values();
                let offset = bitmap.offset();

                let mut builder = MutableBitmap::with_capacity(self.filter_rows);
                let iter = SlicesIterator::new(self.filter);
                iter.for_each(|(start, len)| {
                    builder.append_packed_range(start + offset..start + len + offset, src)
                });
                builder.into()
            }
        };

        self.result = Some(Value::Column(BooleanType::upcast_column(bitmap)));
        Ok(())
    }

    fn visit_binary(&mut self, col: BinaryColumn) -> Result<()> {
        self.result = Some(Value::Column(BinaryType::upcast_column(
            self.filter_binary_types(&col),
        )));
        Ok(())
    }

    fn visit_string(&mut self, column: StringColumn) -> Result<()> {
        self.result = Some(Value::Column(StringType::upcast_column(
            self.filter_string_types(&column),
        )));
        Ok(())
    }

    fn visit_variant(&mut self, column: BinaryColumn) -> Result<()> {
        self.result = Some(Value::Column(VariantType::upcast_column(
            self.filter_binary_types(&column),
        )));
        Ok(())
    }
}

impl FilterVisitor<'_> {
    fn filter_primitive_types<T: Copy>(&mut self, buffer: Buffer<T>) -> Buffer<T> {
        match self.strategy {
            IterationStrategy::IndexIterator => {
                let iter = TrueIdxIter::new(self.original_rows, Some(self.filter));
                Vec::from_iter(iter.map(|index| buffer[index])).into()
            }
            _ => {
                let mut builder = Vec::with_capacity(self.filter_rows);
                let iter = SlicesIterator::new(self.filter);
                iter.for_each(|(start, len)| {
                    builder.extend_from_slice(&buffer[start..start + len]);
                });
                builder.into()
            }
        }
    }

    fn filter_string_types(&mut self, values: &StringColumn) -> StringColumn {
        match self.strategy {
            IterationStrategy::IndexIterator => {
                let mut builder = StringColumnBuilder::with_capacity(self.filter_rows);

                let iter = TrueIdxIter::new(self.original_rows, Some(self.filter));
                for i in iter {
                    unsafe {
                        builder.put_and_commit(values.value_unchecked(i));
                    }
                }
                builder.build()
            }
            _ => {
                // reuse the buffers
                let new_views = self.filter_primitive_types(values.views().clone());
                unsafe {
                    StringColumn::new_unchecked_unknown_md(
                        new_views,
                        values.data_buffers().clone(),
                        None,
                    )
                }
            }
        }
    }

    fn filter_binary_types(&mut self, values: &BinaryColumn) -> BinaryColumn {
        let mut builder = BinaryColumnBuilder::with_capacity(self.filter_rows, 0);
        let iter = TrueIdxIter::new(self.original_rows, Some(self.filter));
        for i in iter {
            unsafe {
                builder.put_slice(values.index_unchecked(i));
                builder.commit_row();
            }
        }
        builder.build()
    }
}
