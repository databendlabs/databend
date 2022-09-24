// Copyright 2022 Datafuse Labs.
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

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;

use crate::property::Domain;
use crate::types::ArgType;
use crate::types::DataType;
use crate::types::GenericMap;
use crate::types::ValueType;
use crate::util::bitmap_into_mut;
use crate::values::Column;
use crate::values::Scalar;
use crate::ColumnBuilder;
use crate::ScalarRef;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BooleanType;

impl ValueType for BooleanType {
    type Scalar = bool;
    type ScalarRef<'a> = bool;
    type Column = Bitmap;
    type Domain = BooleanDomain;
    type ColumnIterator<'a> = common_arrow::arrow::bitmap::utils::BitmapIter<'a>;
    type ColumnBuilder = MutableBitmap;

    #[inline]
    fn upcast_gat<'short, 'long: 'short>(long: bool) -> bool {
        long
    }

    fn to_owned_scalar<'a>(scalar: Self::ScalarRef<'a>) -> Self::Scalar {
        scalar
    }

    fn to_scalar_ref<'a>(scalar: &'a Self::Scalar) -> Self::ScalarRef<'a> {
        *scalar
    }

    fn try_downcast_scalar<'a>(scalar: &'a ScalarRef) -> Option<Self::ScalarRef<'a>> {
        match scalar {
            ScalarRef::Boolean(scalar) => Some(*scalar),
            _ => None,
        }
    }

    fn try_downcast_column<'a>(col: &'a Column) -> Option<Self::Column> {
        match col {
            Column::Boolean(column) => Some(column.clone()),
            _ => None,
        }
    }

    fn try_downcast_builder<'a>(
        builder: &'a mut ColumnBuilder,
    ) -> Option<&'a mut Self::ColumnBuilder> {
        match builder {
            crate::ColumnBuilder::Boolean(builder) => Some(builder),
            _ => None,
        }
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        domain.as_boolean().map(BooleanDomain::clone)
    }

    fn upcast_scalar(scalar: Self::Scalar) -> Scalar {
        Scalar::Boolean(scalar)
    }

    fn upcast_column(col: Self::Column) -> Column {
        Column::Boolean(col)
    }

    fn upcast_domain(domain: Self::Domain) -> Domain {
        Domain::Boolean(domain)
    }

    fn column_len<'a>(col: &'a Self::Column) -> usize {
        col.len()
    }

    fn index_column<'a>(col: &'a Self::Column, index: usize) -> Option<Self::ScalarRef<'a>> {
        col.get(index)
    }

    unsafe fn index_column_unchecked<'a>(
        col: &'a Self::Column,
        index: usize,
    ) -> Self::ScalarRef<'a> {
        col.get_bit_unchecked(index)
    }

    fn slice_column<'a>(col: &'a Self::Column, range: Range<usize>) -> Self::Column {
        col.clone().slice(range.start, range.end - range.start)
    }

    fn iter_column<'a>(col: &'a Self::Column) -> Self::ColumnIterator<'a> {
        col.iter()
    }

    fn column_to_builder(col: Self::Column) -> Self::ColumnBuilder {
        bitmap_into_mut(col)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        builder.len()
    }

    fn push_item(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>) {
        builder.push(item);
    }

    fn push_default(builder: &mut Self::ColumnBuilder) {
        builder.push(false);
    }

    fn append_builder(builder: &mut Self::ColumnBuilder, other_builder: &Self::ColumnBuilder) {
        builder.extend_from_slice(other_builder.as_slice(), 0, other_builder.len());
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        builder.into()
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        assert_eq!(builder.len(), 1);
        builder.get(0)
    }
}

impl ArgType for BooleanType {
    fn data_type() -> DataType {
        DataType::Boolean
    }

    fn create_builder(capacity: usize, _: &GenericMap) -> Self::ColumnBuilder {
        MutableBitmap::with_capacity(capacity)
    }

    fn column_from_iter(iter: impl Iterator<Item = Self::Scalar>, _: &GenericMap) -> Self::Column {
        iter.collect()
    }

    fn column_from_ref_iter<'a>(
        iter: impl Iterator<Item = Self::ScalarRef<'a>>,
        _: &GenericMap,
    ) -> Self::Column {
        iter.collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BooleanDomain {
    pub has_false: bool,
    pub has_true: bool,
}
