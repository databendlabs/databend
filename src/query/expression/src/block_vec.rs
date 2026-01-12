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

use std::any::Any;

use binary::BinaryColumnBuilder;
use binary::take_binary_from_views;
use binary::take_nullable_binary_from_views;
use boolean::take_boolean_from_views;
use boolean::take_nullable_boolean_from_views;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use string::StringColumnBuilder;

use crate::BlockEntry;
use crate::BlockIndex;
use crate::Chunk;
use crate::ChunkIndex;
use crate::Column;
use crate::ColumnBuilder;
use crate::ColumnView;
use crate::DataBlock;
use crate::TakeIndex;
use crate::types::AccessType;
use crate::types::date::CoreDate;
use crate::types::interval::CoreInterval;
use crate::types::nullable::NullableColumnBuilder;
use crate::types::simple_type::SimpleType;
use crate::types::simple_type::SimpleValueType;
use crate::types::timestamp::CoreTimestamp;
use crate::types::timestamp_tz::CoreTimestampTz;
use crate::types::*;
use crate::with_number_mapped_type;
use crate::with_opaque_size;

#[derive(Debug, Default)]
pub struct DataBlockVec {
    columns: Vec<ColumnStorage>,
    block_rows: Vec<usize>,
}

#[derive(Debug)]
struct ColumnStorage {
    data_type: DataType,
    data: Box<dyn Any>, // ColumnView<T>
}

#[derive(Clone, Copy)]
struct TypeHandler {
    init: fn(BlockEntry) -> Result<ColumnStorage>,
    push: fn(&mut ColumnStorage, BlockEntry) -> Result<()>,
    replace: fn(&mut ColumnStorage, usize, BlockEntry),
    take: fn(&ColumnStorage, &ChunkIndex) -> BlockEntry,
    require_same_type: bool,
}

impl TypeHandler {
    fn typed<T: ValueType>(require_same_type: bool) -> Self {
        Self {
            init: DataBlockVec::init_typed::<T>,
            push: DataBlockVec::push_typed::<T>,
            replace: DataBlockVec::replace_typed::<T>,
            take: DataBlockVec::take_typed::<T>,
            require_same_type,
        }
    }

    fn nullable<T: ValueType>() -> Self {
        Self::typed::<NullableType<T>>(true)
    }

    fn primitive<TColumn, TSimple>() -> Self
    where
        TColumn: ValueType,
        TSimple: SimpleType,
    {
        Self {
            init: DataBlockVec::init_typed::<TColumn>,
            push: DataBlockVec::push_typed::<TColumn>,
            replace: DataBlockVec::replace_typed::<TColumn>,
            take: DataBlockVec::take_primitive::<TSimple>,
            require_same_type: false,
        }
    }

    fn nullable_primitive<TColumn, TSimple>() -> Self
    where
        TColumn: ValueType,
        TSimple: SimpleType,
    {
        Self {
            init: DataBlockVec::init_typed::<NullableType<TColumn>>,
            push: DataBlockVec::push_typed::<NullableType<TColumn>>,
            replace: DataBlockVec::replace_typed::<NullableType<TColumn>>,
            take: DataBlockVec::take_nullable_primitive::<TSimple>,
            require_same_type: true,
        }
    }

    fn binary<T>() -> Self
    where
        T: ArgType<ColumnBuilder = BinaryColumnBuilder>,
        for<'a> T::ScalarRef<'a>: AsRef<[u8]>,
    {
        Self {
            init: DataBlockVec::init_typed::<T>,
            push: DataBlockVec::push_typed::<T>,
            replace: DataBlockVec::replace_typed::<T>,
            take: DataBlockVec::take_binary::<T>,
            require_same_type: false,
        }
    }

    fn nullable_binary<T>() -> Self
    where
        T: ArgType<ColumnBuilder = BinaryColumnBuilder>,
        for<'a> T::ScalarRef<'a>: AsRef<[u8]>,
    {
        Self {
            init: DataBlockVec::init_typed::<NullableType<T>>,
            push: DataBlockVec::push_typed::<NullableType<T>>,
            replace: DataBlockVec::replace_typed::<NullableType<T>>,
            take: DataBlockVec::take_nullable_binary::<T>,
            require_same_type: true,
        }
    }

    fn string() -> Self {
        Self {
            init: DataBlockVec::init_typed::<StringType>,
            push: DataBlockVec::push_typed::<StringType>,
            replace: DataBlockVec::replace_typed::<StringType>,
            take: DataBlockVec::take_string,
            require_same_type: false,
        }
    }

    fn nullable_string() -> Self {
        Self {
            init: DataBlockVec::init_typed::<NullableType<StringType>>,
            push: DataBlockVec::push_typed::<NullableType<StringType>>,
            replace: DataBlockVec::replace_typed::<NullableType<StringType>>,
            take: DataBlockVec::take_nullable_string,
            require_same_type: true,
        }
    }

    fn boolean() -> Self {
        Self {
            init: DataBlockVec::init_typed::<BooleanType>,
            push: DataBlockVec::push_typed::<BooleanType>,
            replace: DataBlockVec::replace_typed::<BooleanType>,
            take: DataBlockVec::take_boolean,
            require_same_type: false,
        }
    }

    fn nullable_boolean() -> Self {
        Self {
            init: DataBlockVec::init_typed::<NullableType<BooleanType>>,
            push: DataBlockVec::push_typed::<NullableType<BooleanType>>,
            replace: DataBlockVec::replace_typed::<NullableType<BooleanType>>,
            take: DataBlockVec::take_nullable_boolean,
            require_same_type: true,
        }
    }
}

impl DataBlockVec {
    pub fn push(&mut self, data_block: DataBlock) -> Result<()> {
        if !self.block_rows.is_empty() && self.columns.len() != data_block.num_columns() {
            return Err(ErrorCode::Internal(format!(
                "DataBlockVec push columns mismatch, expected {}, got {}",
                self.columns.len(),
                data_block.num_columns()
            )));
        }

        let num_rows = data_block.num_rows();
        if self.columns.is_empty() {
            self.columns.reserve(data_block.num_columns());
            for entry in data_block.take_columns() {
                let data_type = entry.data_type();
                let handler = Self::handler_for(&data_type);
                let storage = (handler.init)(entry)?;
                self.columns.push(storage);
            }
        } else {
            for (idx, entry) in data_block.take_columns().into_iter().enumerate() {
                let storage = &mut self.columns[idx];
                let entry_type = entry.data_type();
                {
                    let handler = Self::handler_for(&storage.data_type);
                    if handler.require_same_type && storage.data_type != entry_type {
                        return Err(ErrorCode::Internal(format!(
                            "DataBlockVec columns mismatch, expected {}, got {}",
                            storage.data_type, entry_type
                        )));
                    }
                    (handler.push)(storage, entry)
                }?;
            }
        }
        self.block_rows.push(num_rows);

        Ok(())
    }

    pub fn replace(&mut self, i: usize, data_block: DataBlock) {
        assert!(
            i < self.block_rows.len(),
            "DataBlockVec replace index out of range"
        );
        assert_eq!(
            self.columns.len(),
            data_block.num_columns(),
            "DataBlockVec replace columns mismatch"
        );
        self.block_rows[i] = data_block.num_rows();

        if self.columns.is_empty() {
            return;
        }

        for (idx, entry) in data_block.take_columns().into_iter().enumerate() {
            let storage = &mut self.columns[idx];
            let handler = Self::handler_for(&storage.data_type);
            if handler.require_same_type {
                debug_assert_eq!(&storage.data_type, &entry.data_type());
            }
            (handler.replace)(storage, i, entry);
        }
    }

    pub fn take(&self, indices: &ChunkIndex) -> DataBlock {
        let num_rows = indices.num_rows();
        if self.columns.is_empty() {
            return DataBlock::new(vec![], num_rows);
        }

        let columns = self
            .columns
            .iter()
            .map(|column| {
                let handler = Self::handler_for(&column.data_type);
                (handler.take)(column, indices)
            })
            .collect::<Vec<_>>();

        DataBlock::new(columns, num_rows)
    }

    fn init_typed<T: AccessType>(entry: BlockEntry) -> Result<ColumnStorage> {
        let data_type = entry.data_type();
        let mut data = Box::new(Vec::<ColumnView<T>>::new());
        data.push(entry.downcast::<T>()?);
        Ok(ColumnStorage {
            data_type,
            data: data as _,
        })
    }

    fn push_typed<T: AccessType>(storage: &mut ColumnStorage, entry: BlockEntry) -> Result<()> {
        let views = storage
            .data
            .downcast_mut::<Vec<ColumnView<T>>>()
            .expect("column view storage type mismatch");
        views.push(entry.downcast::<T>()?);
        Ok(())
    }

    fn replace_typed<T: AccessType>(storage: &mut ColumnStorage, index: usize, entry: BlockEntry) {
        let views = storage
            .data
            .downcast_mut::<Vec<ColumnView<T>>>()
            .expect("column view storage type mismatch");
        let view = entry.downcast::<T>().expect("column view type mismatch");
        views[index] = view;
    }

    fn handler_for(data_type: &DataType) -> TypeHandler {
        match data_type {
            DataType::Nullable(inner) => Self::handler_for_nullable(inner),
            _ => Self::handler_for_non_nullable(data_type),
        }
    }

    fn handler_for_non_nullable(data_type: &DataType) -> TypeHandler {
        match data_type {
            DataType::Null => TypeHandler::typed::<NullType>(false),
            DataType::EmptyArray => TypeHandler::typed::<EmptyArrayType>(false),
            DataType::EmptyMap => TypeHandler::typed::<EmptyMapType>(false),
            DataType::Boolean => TypeHandler::boolean(),
            DataType::Binary => TypeHandler::binary::<BinaryType>(),
            DataType::String => TypeHandler::string(),
            DataType::Bitmap => TypeHandler::binary::<BitmapType>(),
            DataType::Variant => TypeHandler::binary::<VariantType>(),
            DataType::Geometry => TypeHandler::binary::<GeometryType>(),
            DataType::Geography => TypeHandler::binary::<GeographyType>(),
            DataType::Timestamp => TypeHandler::primitive::<TimestampType, CoreTimestamp>(),
            DataType::TimestampTz => TypeHandler::primitive::<TimestampTzType, CoreTimestampTz>(),
            DataType::Date => TypeHandler::primitive::<DateType, CoreDate>(),
            DataType::Interval => TypeHandler::primitive::<IntervalType, CoreInterval>(),
            DataType::Number(number) => {
                with_number_mapped_type!(|NUM_TYPE| match number {
                    NumberDataType::NUM_TYPE =>
                        TypeHandler::primitive::<NumberType<NUM_TYPE>, CoreNumber<NUM_TYPE>>(),
                })
            }
            DataType::Decimal(size) => match size.data_kind() {
                DecimalDataKind::Decimal64 => {
                    TypeHandler::primitive::<Decimal64Type, CoreDecimal<i64>>()
                }
                DecimalDataKind::Decimal128 => {
                    TypeHandler::primitive::<Decimal128Type, CoreDecimal<i128>>()
                }
                DecimalDataKind::Decimal256 => {
                    TypeHandler::primitive::<Decimal256Type, CoreDecimal<i256>>()
                }
            },
            DataType::Array(_) => TypeHandler::typed::<ArrayType<AnyType>>(true),
            DataType::Map(_) => TypeHandler::typed::<MapType<AnyType, AnyType>>(true),
            DataType::Tuple(_) => TypeHandler::typed::<AnyType>(true),
            DataType::Vector(_) => TypeHandler::typed::<VectorType>(false),
            DataType::Opaque(size) => with_opaque_size!(|N| match *size {
                N => TypeHandler::typed::<OpaqueType<N>>(false),
                _ => unreachable!("Unsupported opaque size: {}", size),
            }),
            DataType::Generic(_) | DataType::StageLocation => unreachable!(),
            DataType::Nullable(_) => unreachable!(),
        }
    }

    fn handler_for_nullable(data_type: &DataType) -> TypeHandler {
        match data_type {
            DataType::EmptyArray => TypeHandler::nullable::<EmptyArrayType>(),
            DataType::EmptyMap => TypeHandler::nullable::<EmptyMapType>(),
            DataType::Boolean => TypeHandler::nullable_boolean(),
            DataType::Binary => TypeHandler::nullable_binary::<BinaryType>(),
            DataType::String => TypeHandler::nullable_string(),
            DataType::Bitmap => TypeHandler::nullable_binary::<BitmapType>(),
            DataType::Variant => TypeHandler::nullable_binary::<VariantType>(),
            DataType::Geometry => TypeHandler::nullable_binary::<GeometryType>(),
            DataType::Geography => TypeHandler::nullable_binary::<GeographyType>(),
            DataType::Timestamp => {
                TypeHandler::nullable_primitive::<TimestampType, CoreTimestamp>()
            }
            DataType::TimestampTz => {
                TypeHandler::nullable_primitive::<TimestampTzType, CoreTimestampTz>()
            }
            DataType::Date => TypeHandler::nullable_primitive::<DateType, CoreDate>(),
            DataType::Interval => TypeHandler::nullable_primitive::<IntervalType, CoreInterval>(),
            DataType::Number(number) => {
                with_number_mapped_type!(|NUM_TYPE| match number {
                    NumberDataType::NUM_TYPE =>
                        TypeHandler::nullable_primitive::<NumberType<NUM_TYPE>, CoreNumber<NUM_TYPE>>(
                        ),
                })
            }
            DataType::Decimal(size) => match size.data_kind() {
                DecimalDataKind::Decimal64 => {
                    TypeHandler::nullable_primitive::<Decimal64Type, CoreDecimal<i64>>()
                }
                DecimalDataKind::Decimal128 => {
                    TypeHandler::nullable_primitive::<Decimal128Type, CoreDecimal<i128>>()
                }
                DecimalDataKind::Decimal256 => {
                    TypeHandler::nullable_primitive::<Decimal256Type, CoreDecimal<i256>>()
                }
            },
            DataType::Array(_) => TypeHandler::nullable::<ArrayType<AnyType>>(),
            DataType::Map(_) => TypeHandler::nullable::<MapType<AnyType, AnyType>>(),
            DataType::Tuple(_) => TypeHandler::nullable::<AnyType>(),
            DataType::Vector(_) => TypeHandler::nullable::<VectorType>(),
            DataType::Opaque(size) => with_opaque_size!(|N| match *size {
                N => TypeHandler::nullable::<OpaqueType<N>>(),
                _ => unreachable!("Unsupported opaque size: {}", size),
            }),
            DataType::Null
            | DataType::Nullable(_)
            | DataType::Generic(_)
            | DataType::StageLocation => unreachable!(),
        }
    }

    fn column_views<T: ValueType>(storage: &ColumnStorage) -> &Vec<ColumnView<T>> {
        storage
            .data
            .downcast_ref::<Vec<ColumnView<T>>>()
            .expect("column view storage type mismatch")
    }

    fn take_typed<T: ValueType>(storage: &ColumnStorage, indices: &ChunkIndex) -> BlockEntry {
        let views = Self::column_views::<T>(storage);
        let mut builder = ColumnBuilder::with_capacity(&storage.data_type, indices.num_rows());
        let mut typed_builder = T::downcast_builder(&mut builder);
        for item in indices.iter_chunk() {
            match item {
                Chunk::Single { block, rows } => {
                    let view = &views[block as usize];
                    for row in TakeIndex::iter(rows) {
                        let scalar = unsafe { view.index_unchecked(row) };
                        typed_builder.push_item(scalar);
                    }
                }
                Chunk::Repeat { block, rows } => {
                    let view = &views[block as usize];
                    for row in rows.iter() {
                        let scalar = unsafe { view.index_unchecked(row) };
                        typed_builder.push_item(scalar);
                    }
                }
                Chunk::Range { block, row, len } => {
                    let view = &views[block as usize];
                    for row in row..row + len {
                        let scalar = unsafe { view.index_unchecked(row as usize) };
                        typed_builder.push_item(scalar);
                    }
                }
            }
        }
        drop(typed_builder);
        builder.build().into()
    }

    fn take_primitive<T>(column: &ColumnStorage, indices: &ChunkIndex) -> BlockEntry
    where T: SimpleType {
        let views = Self::column_views::<SimpleValueType<T>>(column);
        SimpleValueType::<T>::take_from_views(views, indices, &column.data_type)
    }

    fn take_nullable_primitive<T>(column: &ColumnStorage, indices: &ChunkIndex) -> BlockEntry
    where T: SimpleType {
        let views = Self::column_views::<NullableType<SimpleValueType<T>>>(column);
        NullableColumnBuilder::<SimpleValueType<T>>::take_from_views(
            views,
            indices,
            &column.data_type,
        )
    }

    fn take_binary<T>(column: &ColumnStorage, indices: &ChunkIndex) -> BlockEntry
    where
        T: ArgType<ColumnBuilder = BinaryColumnBuilder>,
        for<'a> T::ScalarRef<'a>: AsRef<[u8]>,
    {
        let views = Self::column_views::<T>(column);
        take_binary_from_views::<T>(views, indices)
    }

    fn take_nullable_binary<T>(column: &ColumnStorage, indices: &ChunkIndex) -> BlockEntry
    where
        T: ArgType<ColumnBuilder = BinaryColumnBuilder>,
        for<'a> T::ScalarRef<'a>: AsRef<[u8]>,
    {
        let views = Self::column_views::<NullableType<T>>(column);
        take_nullable_binary_from_views::<T>(views, indices, &column.data_type)
    }

    fn take_string(storage: &ColumnStorage, indices: &ChunkIndex) -> BlockEntry {
        let views = Self::column_views::<StringType>(storage);
        StringColumnBuilder::take_from_views(views, indices)
    }

    fn take_nullable_string(storage: &ColumnStorage, indices: &ChunkIndex) -> BlockEntry {
        let views = Self::column_views::<NullableType<StringType>>(storage);
        NullableColumnBuilder::<StringType>::take_from_views(views, indices)
    }

    fn take_boolean(storage: &ColumnStorage, indices: &ChunkIndex) -> BlockEntry {
        let views = Self::column_views::<BooleanType>(storage);
        take_boolean_from_views(views, indices)
    }

    fn take_nullable_boolean(storage: &ColumnStorage, indices: &ChunkIndex) -> BlockEntry {
        let views = Self::column_views::<NullableType<BooleanType>>(storage);
        take_nullable_boolean_from_views(views, indices)
    }
}
