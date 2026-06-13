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
use databend_common_base::vec_ext::VecExt;
use databend_common_column::buffer::Buffer;
use databend_common_exception::Result;

use super::RepeatIndex;
use crate::BlockEntry;
use crate::Column;
use crate::ColumnBuilder;
use crate::DataBlock;
use crate::TakeIndex;
use crate::Value;
use crate::types::binary::BinaryColumn;
use crate::types::date::CoreDate;
use crate::types::nullable::NullableColumn;
use crate::types::string::StringColumn;
use crate::types::timestamp::CoreTimestamp;
use crate::types::*;
use crate::visitor::ValueVisitor;
use crate::with_number_mapped_type;
use crate::with_opaque_mapped_type;

impl DataBlock {
    pub fn take_compacted_indices(&self, indices: &[RepeatIndex], num_rows: usize) -> Result<Self> {
        if indices.is_empty() {
            return Ok(self.slice(0..0));
        }

        // Each [`RepeatIndex`] holds an `index` and a repeat `count`, whose sum must equal `num_rows`.
        debug_assert_eq!(
            indices.iter().fold(0, |acc, idx| acc + idx.count as usize),
            num_rows
        );

        let mut taker = TakeCompactVisitor::new(indices, num_rows);
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

struct TakeCompactVisitor<'a> {
    indices: &'a [RepeatIndex],
    num_rows: usize,
    result: Option<Value<AnyType>>,
}

impl<'a> TakeCompactVisitor<'a> {
    fn new(indices: &'a [RepeatIndex], num_rows: usize) -> Self {
        Self {
            indices,
            num_rows,
            result: None,
        }
    }
}

impl ValueVisitor for TakeCompactVisitor<'_> {
    fn visit_scalar(&mut self, scalar: crate::Scalar) -> Result<()> {
        self.result = Some(Value::Scalar(scalar));
        Ok(())
    }

    fn visit_column(&mut self, column: Column) -> Result<()> {
        Self::visit_column_use_simple_type(column, self)
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

        for repeat in self.indices {
            for _ in 0..repeat.count as usize {
                inner_builder
                    .push_item(unsafe { T::index_column_unchecked(&column, repeat.row as usize) });
            }
        }
        drop(inner_builder);
        self.result = Some(Value::Column(builder.build()));
        Ok(())
    }

    fn visit_binary(&mut self, col: BinaryColumn) -> Result<()> {
        self.result = Some(Value::Column(BinaryType::upcast_column_with_type(
            self.take_binary_types(&col),
            &DataType::Binary,
        )));
        Ok(())
    }

    fn visit_string(&mut self, col: StringColumn) -> Result<()> {
        self.result = Some(Value::Column(StringType::upcast_column_with_type(
            self.take_string_types(&col),
            &DataType::String,
        )));
        Ok(())
    }

    fn visit_variant(&mut self, column: BinaryColumn) -> Result<()> {
        self.result = Some(Value::Column(VariantType::upcast_column(
            self.take_binary_types(&column),
        )));
        Ok(())
    }

    fn visit_bitmap(&mut self, column: BinaryColumn) -> Result<()> {
        self.result = Some(Value::Column(BitmapType::upcast_column(
            self.take_binary_types(&column),
        )));
        Ok(())
    }

    fn visit_opaque(&mut self, column: OpaqueColumn) -> Result<()> {
        self.result = Some(Value::Column(
            (with_opaque_mapped_type!(|T| match column {
                OpaqueColumn::T(column) => {
                    OpaqueType::<T>::upcast_column(self.take_primitive_types(column))
                }
            })),
        ));
        Ok(())
    }
}

impl TakeCompactVisitor<'_> {
    fn take_primitive_types<T: Copy>(&mut self, buffer: Buffer<T>) -> Buffer<T> {
        let buffer = buffer.as_slice();
        let mut builder: Vec<T> = Vec::with_capacity(self.num_rows);

        for repeat in self.indices.iter() {
            repeat.take_primitive_types(buffer, &mut builder);
        }

        builder.into()
    }

    fn take_binary_types(&mut self, col: &BinaryColumn) -> BinaryColumn {
        let num_rows = self.num_rows;
        let mut builder = BinaryColumnBuilder::with_capacity(num_rows, 0);
        for repeat in self.indices.iter() {
            for _ in 0..repeat.count as usize {
                unsafe {
                    builder.put_slice(col.index_unchecked(repeat.row as usize));
                    builder.commit_row();
                }
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
