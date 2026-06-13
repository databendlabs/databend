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

use std::cmp::Ordering;
use std::ops::Range;

use databend_common_exception::Result;

use super::AccessType;
use super::ArgType;
use super::BuilderMut;
use super::DataType;
use super::GenericMap;
use super::NullableType;
use super::ReturnType;
use super::ScalarRef;
use super::ValueType;
use super::column_type_error;
use super::domain_type_error;
use super::nullable::NullableColumnBuilder;
use super::scalar_type_error;
use crate::BlockEntry;
use crate::Chunk;
use crate::ChunkIndex;
use crate::ColumnBuilder;
use crate::ColumnView;
use crate::TakeIndex;
use crate::property::Domain;
use crate::values::Column;
use crate::values::Scalar;

pub type BinaryColumn = databend_common_column::binary::BinaryColumn;
pub type BinaryColumnBuilder = databend_common_column::binary::BinaryColumnBuilder;
pub type BinaryColumnIter<'a> = databend_common_column::binary::BinaryColumnIter<'a>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinaryType;

impl AccessType for BinaryType {
    type Scalar = Vec<u8>;
    type ScalarRef<'a> = &'a [u8];
    type Column = BinaryColumn;
    type Domain = ();
    type ColumnIterator<'a> = BinaryColumnIter<'a>;

    fn to_owned_scalar(scalar: Self::ScalarRef<'_>) -> Self::Scalar {
        scalar.to_vec()
    }

    fn to_scalar_ref(scalar: &Self::Scalar) -> Self::ScalarRef<'_> {
        scalar
    }

    fn try_downcast_scalar<'a>(scalar: &ScalarRef<'a>) -> Result<Self::ScalarRef<'a>> {
        scalar
            .as_binary()
            .cloned()
            .ok_or_else(|| scalar_type_error::<Self>(scalar))
    }

    fn try_downcast_column(col: &Column) -> Result<Self::Column> {
        col.as_binary()
            .cloned()
            .ok_or_else(|| column_type_error::<Self>(col))
    }

    fn try_downcast_domain(domain: &Domain) -> Result<Self::Domain> {
        if domain.is_undefined() {
            Ok(())
        } else {
            Err(domain_type_error::<Self>(domain))
        }
    }

    fn column_len(col: &Self::Column) -> usize {
        col.len()
    }

    fn index_column(col: &Self::Column, index: usize) -> Option<Self::ScalarRef<'_>> {
        col.index(index)
    }

    unsafe fn index_column_unchecked(col: &Self::Column, index: usize) -> Self::ScalarRef<'_> {
        unsafe { col.index_unchecked(index) }
    }

    fn slice_column(col: &Self::Column, range: Range<usize>) -> Self::Column {
        col.slice(range)
    }

    fn iter_column(col: &Self::Column) -> Self::ColumnIterator<'_> {
        col.iter()
    }

    fn scalar_memory_size(scalar: &Self::ScalarRef<'_>) -> usize {
        scalar.len()
    }

    fn column_memory_size(col: &Self::Column, _gc: bool) -> usize {
        col.data().len() + col.offsets().len() * 8
    }

    #[inline(always)]
    fn compare(lhs: Self::ScalarRef<'_>, rhs: Self::ScalarRef<'_>) -> Ordering {
        lhs.cmp(rhs)
    }
}

impl ValueType for BinaryType {
    type ColumnBuilder = BinaryColumnBuilder;
    type ColumnBuilderMut<'a> = BuilderMut<'a, Self>;

    fn upcast_scalar_with_type(scalar: Self::Scalar, data_type: &DataType) -> Scalar {
        debug_assert!(data_type.is_binary());
        Scalar::Binary(scalar)
    }

    fn upcast_domain_with_type(_domain: Self::Domain, data_type: &DataType) -> Domain {
        debug_assert!(data_type.is_binary());
        Domain::Undefined
    }

    fn upcast_column_with_type(col: Self::Column, data_type: &DataType) -> Column {
        debug_assert!(data_type.is_binary());
        Column::Binary(col)
    }

    fn downcast_builder(builder: &mut ColumnBuilder) -> Self::ColumnBuilderMut<'_> {
        builder.as_binary_mut().unwrap().into()
    }

    fn try_upcast_column_builder(
        builder: Self::ColumnBuilder,
        data_type: &DataType,
    ) -> Option<ColumnBuilder> {
        debug_assert!(data_type.is_binary());
        Some(ColumnBuilder::Binary(builder))
    }

    fn column_to_builder(col: Self::Column) -> Self::ColumnBuilder {
        BinaryColumnBuilder::from_column(col)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        builder.len()
    }

    fn builder_len_mut(builder: &Self::ColumnBuilderMut<'_>) -> usize {
        builder.len()
    }

    fn push_item_mut(builder: &mut Self::ColumnBuilderMut<'_>, item: Self::ScalarRef<'_>) {
        builder.put_slice(item);
        builder.commit_row();
    }

    fn push_item_repeat_mut(
        builder: &mut Self::ColumnBuilderMut<'_>,
        item: Self::ScalarRef<'_>,
        n: usize,
    ) {
        builder.push_repeat(item, n);
    }

    fn push_default_mut(builder: &mut Self::ColumnBuilderMut<'_>) {
        builder.commit_row();
    }

    fn append_column_mut(builder: &mut Self::ColumnBuilderMut<'_>, other: &Self::Column) {
        builder.append_column(other);
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        builder.build()
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        builder.build_scalar()
    }
}

impl ArgType for BinaryType {
    fn data_type() -> DataType {
        DataType::Binary
    }

    fn full_domain() -> Self::Domain {}
}

impl ReturnType for BinaryType {
    fn create_builder(capacity: usize, _: &GenericMap) -> Self::ColumnBuilder {
        BinaryColumnBuilder::with_capacity(capacity, 0)
    }
}

pub fn take_binary_from_views<T>(views: &[ColumnView<T>], indices: &ChunkIndex) -> BlockEntry
where
    T: ArgType<ColumnBuilder = BinaryColumnBuilder>,
    for<'a> T::ScalarRef<'a>: AsRef<[u8]>,
{
    let mut builder = BinaryColumnBuilder::with_capacity(indices.num_rows(), 0);
    for chunk in indices.iter_chunk() {
        match chunk {
            Chunk::Single { block, rows } => {
                let view = &views[block as usize];
                for row in TakeIndex::iter(rows) {
                    let scalar = unsafe { view.index_unchecked(row) };
                    builder.put_slice(scalar.as_ref());
                    builder.commit_row();
                }
            }
            Chunk::Repeat { block, rows } => {
                let view = &views[block as usize];
                for row in rows.iter() {
                    let scalar = unsafe { view.index_unchecked(row) };
                    builder.put_slice(scalar.as_ref());
                    builder.commit_row();
                }
            }
            Chunk::Range { block, row, len } => {
                let view = &views[block as usize];
                for r in row..row + len {
                    let scalar = unsafe { view.index_unchecked(r as usize) };
                    builder.put_slice(scalar.as_ref());
                    builder.commit_row();
                }
            }
        }
    }
    let column = T::build_column(builder);
    T::upcast_column(column).into()
}

pub fn take_nullable_binary_from_views<T>(
    views: &[ColumnView<NullableType<T>>],
    indices: &ChunkIndex,
    data_type: &DataType,
) -> BlockEntry
where
    T: ArgType<ColumnBuilder = BinaryColumnBuilder>,
    for<'a> T::ScalarRef<'a>: AsRef<[u8]>,
{
    let mut builder = NullableColumnBuilder::<T>::with_capacity(indices.num_rows(), &[]);
    for chunk in indices.iter_chunk() {
        match chunk {
            Chunk::Single { block, rows } => {
                let view = &views[block as usize];
                for row in TakeIndex::iter(rows) {
                    match unsafe { view.index_unchecked(row) } {
                        Some(value) => builder.push(value),
                        None => builder.push_null(),
                    }
                }
            }
            Chunk::Repeat { block, rows } => {
                let view = &views[block as usize];
                match unsafe { view.index_unchecked(rows.row as usize) } {
                    Some(value) => builder.push_repeat(value, rows.count as usize),
                    None => builder.push_repeat_null(rows.count as usize),
                }
            }
            Chunk::Range { block, row, len } => {
                let view = &views[block as usize];
                for r in row..row + len {
                    match unsafe { view.index_unchecked(r as usize) } {
                        Some(value) => builder.push(value),
                        None => builder.push_null(),
                    }
                }
            }
        }
    }
    let column = builder.build();
    NullableType::<T>::upcast_column_with_type(column, data_type).into()
}
