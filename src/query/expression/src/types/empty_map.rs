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

use std::ops::Range;

use crate::property::Domain;
use crate::types::ArgType;
use crate::types::DataType;
use crate::types::DecimalSize;
use crate::types::GenericMap;
use crate::types::ValueType;
use crate::values::Column;
use crate::values::Scalar;
use crate::ColumnBuilder;
use crate::ScalarRef;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmptyMapType;

impl ValueType for EmptyMapType {
    type Scalar = ();
    type ScalarRef<'a> = ();
    type Column = usize;
    type Domain = ();
    type ColumnIterator<'a> = std::iter::Take<std::iter::Repeat<()>>;
    type ColumnBuilder = usize;

    #[inline]
    fn upcast_gat<'short, 'long: 'short>(_: Self::ScalarRef<'long>) -> Self::ScalarRef<'short> {}

    fn to_owned_scalar(scalar: Self::ScalarRef<'_>) -> Self::Scalar {
        scalar
    }

    fn to_scalar_ref(scalar: &Self::Scalar) -> Self::ScalarRef<'_> {
        *scalar
    }

    fn try_downcast_scalar<'a>(scalar: &'a ScalarRef) -> Option<Self::ScalarRef<'a>> {
        match scalar {
            ScalarRef::EmptyMap => Some(()),
            _ => None,
        }
    }

    fn try_downcast_column(col: &Column) -> Option<Self::Column> {
        match col {
            Column::EmptyMap { len } => Some(*len),
            _ => None,
        }
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        match domain {
            Domain::Map(None) => Some(()),
            _ => None,
        }
    }

    fn try_downcast_builder(builder: &mut ColumnBuilder) -> Option<&mut Self::ColumnBuilder> {
        match builder {
            ColumnBuilder::EmptyMap { len } => Some(len),
            _ => None,
        }
    }

    fn try_downcast_owned_builder(builder: ColumnBuilder) -> Option<Self::ColumnBuilder> {
        match builder {
            ColumnBuilder::EmptyMap { len } => Some(len),
            _ => None,
        }
    }

    fn try_upcast_column_builder(
        len: Self::ColumnBuilder,
        _decimal_size: Option<DecimalSize>,
    ) -> Option<ColumnBuilder> {
        Some(ColumnBuilder::EmptyMap { len })
    }

    fn upcast_scalar(_: Self::Scalar) -> Scalar {
        Scalar::EmptyMap
    }

    fn upcast_column(len: Self::Column) -> Column {
        Column::EmptyMap { len }
    }

    fn upcast_domain(_: Self::Domain) -> Domain {
        Domain::Map(None)
    }

    fn column_len(len: &Self::Column) -> usize {
        *len
    }

    fn index_column(len: &Self::Column, index: usize) -> Option<Self::ScalarRef<'_>> {
        if index < *len { Some(()) } else { None }
    }

    #[inline(always)]
    unsafe fn index_column_unchecked(_len: &Self::Column, _index: usize) -> Self::ScalarRef<'_> {}

    fn slice_column(len: &Self::Column, range: Range<usize>) -> Self::Column {
        assert!(range.end <= *len, "range {range:?} out of 0..{len}");
        range.end - range.start
    }

    fn iter_column(len: &Self::Column) -> Self::ColumnIterator<'_> {
        std::iter::repeat(()).take(*len)
    }

    fn column_to_builder(len: Self::Column) -> Self::ColumnBuilder {
        len
    }

    fn builder_len(len: &Self::ColumnBuilder) -> usize {
        *len
    }

    fn push_item(len: &mut Self::ColumnBuilder, _: Self::Scalar) {
        *len += 1
    }

    fn push_item_repeat(len: &mut Self::ColumnBuilder, _: Self::ScalarRef<'_>, n: usize) {
        *len += n
    }

    fn push_default(len: &mut Self::ColumnBuilder) {
        *len += 1
    }

    fn append_column(len: &mut Self::ColumnBuilder, other_len: &Self::Column) {
        *len += other_len
    }

    fn build_column(len: Self::ColumnBuilder) -> Self::Column {
        len
    }

    fn build_scalar(len: Self::ColumnBuilder) -> Self::Scalar {
        assert_eq!(len, 1);
    }

    fn scalar_memory_size(_: &Self::ScalarRef<'_>) -> usize {
        0
    }

    fn column_memory_size(_: &Self::Column) -> usize {
        std::mem::size_of::<usize>()
    }
}

impl ArgType for EmptyMapType {
    fn data_type() -> DataType {
        DataType::EmptyMap
    }

    fn full_domain() -> Self::Domain {}

    fn create_builder(_capacity: usize, _generics: &GenericMap) -> Self::ColumnBuilder {
        0
    }

    fn column_from_iter(iter: impl Iterator<Item = Self::Scalar>, _: &GenericMap) -> Self::Column {
        iter.count()
    }

    fn column_from_ref_iter<'a>(
        iter: impl Iterator<Item = Self::ScalarRef<'a>>,
        _: &GenericMap,
    ) -> Self::Column {
        iter.count()
    }
}
