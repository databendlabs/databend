// Copyright 2023 Datafuse Labs.
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

use std::ops::Range;

use common_arrow::arrow::buffer::Buffer;
use roaring::RoaringBitmap;

use crate::arrow::buffer_into_mut;
use crate::property::Domain;
use crate::types::ArgType;
use crate::types::DataType;
use crate::types::GenericMap;
use crate::types::ValueType;
use crate::values::Column;
use crate::values::Scalar;
use crate::ColumnBuilder;
use crate::ScalarRef;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BitmapType;

impl ValueType for BitmapType {
    type Scalar = Vec<u32>;
    type ScalarRef<'a> = &'a [u32];
    type Column = Buffer<RoaringBitmap>;
    type Domain = BitmapDomain;
    type ColumnIterator<'a> = std::iter::Cloned<std::slice::Iter<'a, &'a [u32]>>;
    type ColumnBuilder = Vec<RoaringBitmap>;

    #[inline]
    fn upcast_gat<'short, 'long: 'short>(long: Self::ScalarRef<'long>) -> Self::ScalarRef<'short> {
        long
    }

    fn to_owned_scalar<'a>(scalar: Self::ScalarRef<'a>) -> Self::Scalar {
        scalar.to_vec()
    }

    fn to_scalar_ref<'a>(scalar: &'a Self::Scalar) -> Self::ScalarRef<'a> {
        scalar
    }

    fn try_downcast_scalar<'a>(scalar: &'a ScalarRef) -> Option<Self::ScalarRef<'a>> {
        match scalar {
            ScalarRef::Bitmap(scalar) => Some(scalar.clone()),
            _ => None,
        }
    }

    fn try_downcast_column<'a>(col: &'a Column) -> Option<Self::Column> {
        match col {
            Column::Bitmap(column) => Some(column.clone()),
            _ => None,
        }
    }

    fn try_downcast_builder<'a>(
        builder: &'a mut ColumnBuilder,
    ) -> Option<&'a mut Self::ColumnBuilder> {
        match builder {
            crate::ColumnBuilder::Bitmap(builder) => Some(builder),
            _ => None,
        }
    }

    fn try_downcast_domain(_domain: &Domain) -> Option<Self::Domain> {
        None
    }

    fn upcast_scalar(scalar: Self::Scalar) -> Scalar {
        Scalar::Bitmap(scalar)
    }

    fn upcast_column(col: Self::Column) -> Column {
        Column::Bitmap(col)
    }

    fn upcast_domain(domain: Self::Domain) -> Domain {
        Domain::Bitmap(domain)
    }

    fn column_len<'a>(col: &'a Self::Column) -> usize {
        col.len()
    }

    fn index_column<'a>(col: &'a Self::Column, index: usize) -> Option<Self::ScalarRef<'a>> {
        col.get(index)
            .map(|c| c.into_iter().collect::<Vec<_>>().as_slice())
    }

    unsafe fn index_column_unchecked<'a>(
        col: &'a Self::Column,
        index: usize,
    ) -> Self::ScalarRef<'a> {
        col.get_unchecked(index)
            .into_iter()
            .collect::<Vec<_>>()
            .as_slice()
    }

    fn slice_column<'a>(col: &'a Self::Column, range: Range<usize>) -> Self::Column {
        col.clone().sliced(range.start, range.end - range.start)
    }

    fn iter_column<'a>(col: &'a Self::Column) -> Self::ColumnIterator<'a> {
        col.iter()
            .map(|c| c.into_iter().collect::<Vec<_>>().as_slice())
            .collect::<Vec<_>>()
            .iter()
            .cloned()
    }

    fn column_to_builder(col: Self::Column) -> Self::ColumnBuilder {
        buffer_into_mut(col)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        builder.len()
    }

    fn push_item(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>) {
        builder.push(RoaringBitmap::from_iter(item));
    }

    fn push_default(builder: &mut Self::ColumnBuilder) {
        builder.push(RoaringBitmap::new());
    }

    fn append_column(builder: &mut Self::ColumnBuilder, bitmap: &Self::Column) {
        builder.extend_from_slice(bitmap)
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        builder.into()
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        assert_eq!(builder.len(), 1);
        builder
            .get(0)
            .map(|c| c.into_iter().collect())
            .unwrap_or(vec![])
    }
}

impl ArgType for BitmapType {
    fn data_type() -> DataType {
        DataType::Bitmap
    }

    fn full_domain() -> Self::Domain {
        BitmapDomain {}
    }

    fn create_builder(capacity: usize, _: &GenericMap) -> Self::ColumnBuilder {
        Vec::with_capacity(capacity)
    }

    fn column_from_vec(vec: Vec<Self::Scalar>, _generics: &GenericMap) -> Self::Column {
        vec.iter()
            .map(|s| RoaringBitmap::from_iter(s.iter()))
            .collect()
    }

    fn column_from_iter(iter: impl Iterator<Item = Self::Scalar>, _: &GenericMap) -> Self::Column {
        iter.map(|s| RoaringBitmap::from_iter(s.iter())).collect()
    }

    fn column_from_ref_iter<'a>(
        iter: impl Iterator<Item = Self::ScalarRef<'a>>,
        _generics: &GenericMap,
    ) -> Self::Column {
        iter.map(|s| RoaringBitmap::from_iter(s.iter())).collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BitmapDomain {}
