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

use super::binary::BinaryColumn;
use super::binary::BinaryColumnBuilder;
use super::binary::BinaryColumnIter;
use crate::property::Domain;
use crate::types::AccessType;
use crate::types::ArgType;
use crate::types::BuilderMut;
use crate::types::DataType;
use crate::types::GenericMap;
use crate::types::ReturnType;
use crate::types::ValueType;
use crate::values::Column;
use crate::values::Scalar;
use crate::ColumnBuilder;
use crate::ScalarRef;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitmapType;

impl AccessType for BitmapType {
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

    fn try_downcast_scalar<'a>(scalar: &ScalarRef<'a>) -> Option<Self::ScalarRef<'a>> {
        scalar.as_bitmap().cloned()
    }

    fn try_downcast_column(col: &Column) -> Option<Self::Column> {
        col.as_bitmap().cloned()
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        if domain.is_undefined() {
            Some(())
        } else {
            None
        }
    }

    fn column_len(col: &Self::Column) -> usize {
        col.len()
    }

    fn index_column(col: &Self::Column, index: usize) -> Option<Self::ScalarRef<'_>> {
        col.index(index)
    }

    #[inline(always)]
    unsafe fn index_column_unchecked(col: &Self::Column, index: usize) -> Self::ScalarRef<'_> {
        col.index_unchecked(index)
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

    fn column_memory_size(col: &Self::Column) -> usize {
        col.memory_size()
    }

    #[inline(always)]
    fn compare(lhs: Self::ScalarRef<'_>, rhs: Self::ScalarRef<'_>) -> Ordering {
        lhs.cmp(rhs)
    }
}

impl ValueType for BitmapType {
    type ColumnBuilder = BinaryColumnBuilder;
    type ColumnBuilderMut<'a> = BuilderMut<'a, Self>;

    fn upcast_scalar_with_type(scalar: Self::Scalar, data_type: &DataType) -> Scalar {
        debug_assert!(data_type.is_bitmap());
        Scalar::Bitmap(scalar)
    }

    fn upcast_domain_with_type(_domain: Self::Domain, data_type: &DataType) -> Domain {
        debug_assert!(data_type.is_bitmap());
        Domain::Undefined
    }

    fn upcast_column_with_type(col: Self::Column, data_type: &DataType) -> Column {
        debug_assert!(data_type.is_bitmap());
        Column::Bitmap(col)
    }

    fn downcast_builder(builder: &mut ColumnBuilder) -> Self::ColumnBuilderMut<'_> {
        builder.as_bitmap_mut().unwrap().into()
    }

    fn try_upcast_column_builder(
        builder: Self::ColumnBuilder,
        data_type: &DataType,
    ) -> Option<ColumnBuilder> {
        debug_assert!(data_type.is_bitmap());
        Some(ColumnBuilder::Bitmap(builder))
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

impl ArgType for BitmapType {
    fn data_type() -> DataType {
        DataType::Bitmap
    }

    fn full_domain() -> Self::Domain {}
}

impl ReturnType for BitmapType {
    fn create_builder(capacity: usize, _: &GenericMap) -> Self::ColumnBuilder {
        BinaryColumnBuilder::with_capacity(capacity, 0)
    }
}
