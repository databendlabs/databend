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

use common_arrow::arrow::buffer::Buffer;
use common_exception::Result;

use crate::types::array::ArrayColumn;
use crate::types::array::ArrayColumnBuilder;
use crate::types::bitmap::BitmapType;
use crate::types::decimal::DecimalColumn;
use crate::types::map::KvColumnBuilder;
use crate::types::nullable::NullableColumn;
use crate::types::number::NumberColumn;
use crate::types::string::StringColumn;
use crate::types::AnyType;
use crate::types::ArgType;
use crate::types::ArrayType;
use crate::types::BooleanType;
use crate::types::MapType;
use crate::types::NumberType;
use crate::types::StringType;
use crate::types::ValueType;
use crate::types::VariantType;
use crate::with_decimal_type;
use crate::with_number_mapped_type;
use crate::BlockEntry;
use crate::Column;
use crate::ColumnBuilder;
use crate::DataBlock;
use crate::Value;

impl DataBlock {
    pub fn take<I>(&self, indices: &[I]) -> Result<Self>
    where I: common_arrow::arrow::types::Index {
        if indices.is_empty() {
            return Ok(self.slice(0..0));
        }

        let after_columns = self
            .columns()
            .iter()
            .map(|entry| match &entry.value {
                Value::Scalar(s) => {
                    BlockEntry::new(entry.data_type.clone(), Value::Scalar(s.clone()))
                }
                Value::Column(c) => BlockEntry::new(
                    entry.data_type.clone(),
                    Value::Column(Column::take(c, indices)),
                ),
            })
            .collect();

        Ok(DataBlock::new_with_meta(
            after_columns,
            indices.len(),
            self.get_meta().cloned(),
        ))
    }
}

impl Column {
    pub fn take<I>(&self, indices: &[I]) -> Self
    where I: common_arrow::arrow::types::Index {
        match self {
            Column::Null { .. } | Column::EmptyArray { .. } | Column::EmptyMap { .. } => {
                self.slice(0..indices.len())
            }
            Column::Number(column) => with_number_mapped_type!(|NUM_TYPE| match column {
                NumberColumn::NUM_TYPE(values) => {
                    let builder = Self::take_primitive_types(values, indices);
                    <NumberType<NUM_TYPE>>::upcast_column(<NumberType<NUM_TYPE>>::column_from_vec(
                        builder,
                        &[],
                    ))
                }
            }),
            Column::Decimal(column) => with_decimal_type!(|DECIMAL_TYPE| match column {
                DecimalColumn::DECIMAL_TYPE(values, size) => {
                    let builder = Self::take_primitive_types(values, indices);
                    Column::Decimal(DecimalColumn::DECIMAL_TYPE(builder.into(), *size))
                }
            }),
            Column::Boolean(bm) => Self::take_arg_types::<BooleanType, _>(bm, indices),
            Column::String(column) => {
                StringType::upcast_column(Self::take_string_types(column, indices))
            }
            Column::Timestamp(column) => {
                let builder = Self::take_primitive_types(column, indices);
                let ts = <NumberType<i64>>::upcast_column(<NumberType<i64>>::column_from_vec(
                    builder,
                    &[],
                ))
                .into_number()
                .unwrap()
                .into_int64()
                .unwrap();
                Column::Timestamp(ts)
            }
            Column::Date(column) => {
                let builder = Self::take_primitive_types(column, indices);
                let d = <NumberType<i32>>::upcast_column(<NumberType<i32>>::column_from_vec(
                    builder,
                    &[],
                ))
                .into_number()
                .unwrap()
                .into_int32()
                .unwrap();
                Column::Date(d)
            }
            Column::Array(column) => {
                let mut offsets = Vec::with_capacity(indices.len() + 1);
                offsets.push(0);
                let builder = ColumnBuilder::with_capacity(&column.values.data_type(), self.len());
                let builder = ArrayColumnBuilder { builder, offsets };
                Self::take_value_types::<ArrayType<AnyType>, _>(column, builder, indices)
            }
            Column::Map(column) => {
                let mut offsets = Vec::with_capacity(indices.len() + 1);
                offsets.push(0);
                let builder = ColumnBuilder::from_column(
                    ColumnBuilder::with_capacity(&column.values.data_type(), self.len()).build(),
                );
                let (key_builder, val_builder) = match builder {
                    ColumnBuilder::Tuple(fields) => (fields[0].clone(), fields[1].clone()),
                    _ => unreachable!(),
                };
                let builder = KvColumnBuilder {
                    keys: key_builder,
                    values: val_builder,
                };
                let builder = ArrayColumnBuilder { builder, offsets };
                let column = ArrayColumn::try_downcast(column).unwrap();
                Self::take_value_types::<MapType<AnyType, AnyType>, _>(&column, builder, indices)
            }
            Column::Bitmap(column) => {
                BitmapType::upcast_column(Self::take_string_types(column, indices))
            }
            Column::Nullable(c) => {
                let column = c.column.take(indices);
                let validity = Self::take_arg_types::<BooleanType, _>(&c.validity, indices);
                Column::Nullable(Box::new(NullableColumn {
                    column,
                    validity: BooleanType::try_downcast_column(&validity).unwrap(),
                }))
            }
            Column::Tuple(fields) => {
                let fields = fields.iter().map(|c| c.take(indices)).collect();
                Column::Tuple(fields)
            }
            Column::Variant(column) => {
                VariantType::upcast_column(Self::take_string_types(column, indices))
            }
        }
    }

    pub fn take_primitive_types<T, I>(col: &Buffer<T>, indices: &[I]) -> Vec<T>
    where
        T: Copy,
        I: common_arrow::arrow::types::Index,
    {
        let num_rows = indices.len();
        let mut builder: Vec<T> = Vec::with_capacity(num_rows);
        let ptr = builder.as_mut_ptr();
        // # Safety
        // `i` must be less than `num_rows` and the capacity of builder is `num_rows`.
        unsafe {
            for (i, index) in indices.iter().enumerate() {
                std::ptr::write(ptr.add(i), col[index.to_usize()]);
            }
            builder.set_len(num_rows);
        }
        builder
    }

    pub fn take_string_types<'a, I>(col: &'a StringColumn, indices: &[I]) -> StringColumn
    where I: common_arrow::arrow::types::Index {
        let num_rows = indices.len();
        let mut items: Vec<&[u8]> = Vec::with_capacity(num_rows);
        let mut offsets: Vec<u64> = Vec::with_capacity(num_rows + 1);
        offsets.push(0);
        let items_ptr = items.as_mut_ptr();
        let offsets_ptr = unsafe { offsets.as_mut_ptr().add(1) };

        let mut data_size = 0;
        for (i, index) in indices.iter().enumerate() {
            let item = unsafe { col.index_unchecked(index.to_usize()) };
            data_size += item.len() as u64;
            // # Safety
            // `i` must be less than the capacity of Vec.
            unsafe {
                std::ptr::write(items_ptr.add(i), item);
                std::ptr::write(offsets_ptr.add(i), data_size);
            }
        }
        unsafe {
            items.set_len(num_rows);
            offsets.set_len(num_rows + 1);
        }

        let mut data: Vec<u8> = Vec::with_capacity(data_size as usize);
        let data_ptr = data.as_mut_ptr();
        let mut offset = 0;
        for item in items {
            let len = item.len();
            // # Safety
            // `offset` + `len` < `data_size`.
            unsafe {
                std::ptr::copy_nonoverlapping(item.as_ptr(), data_ptr.add(offset), len);
            }
            offset += len;
        }
        unsafe { data.set_len(offset) };
        StringColumn::new(data.into(), offsets.into())
    }

    fn take_arg_types<T: ArgType, I>(col: &T::Column, indices: &[I]) -> Column
    where I: common_arrow::arrow::types::Index {
        let mut builder = T::create_builder(indices.len(), &[]);
        for index in indices {
            T::push_item(&mut builder, unsafe {
                T::index_column_unchecked(col, index.to_usize())
            });
        }
        let column = T::build_column(builder);
        T::upcast_column(column)
    }

    fn take_value_types<T: ValueType, I>(
        col: &T::Column,
        mut builder: T::ColumnBuilder,
        indices: &[I],
    ) -> Column
    where
        I: common_arrow::arrow::types::Index,
    {
        for index in indices {
            T::push_item(&mut builder, unsafe {
                T::index_column_unchecked(col, index.to_usize())
            });
        }
        T::upcast_column(T::build_column(builder))
    }
}
