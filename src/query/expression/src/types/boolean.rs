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

use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::bitmap::MutableBitmap;

use crate::property::Domain;
use crate::types::ArgType;
use crate::types::DataType;
use crate::types::DecimalSize;
use crate::types::GenericMap;
use crate::types::ValueType;
use crate::utils::arrow::bitmap_into_mut;
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
    type ColumnIterator<'a> = databend_common_arrow::arrow::bitmap::utils::BitmapIter<'a>;
    type ColumnBuilder = MutableBitmap;

    #[inline]
    fn upcast_gat<'short, 'long: 'short>(long: bool) -> bool {
        long
    }

    fn to_owned_scalar(scalar: Self::ScalarRef<'_>) -> Self::Scalar {
        scalar
    }

    fn to_scalar_ref(scalar: &Self::Scalar) -> Self::ScalarRef<'_> {
        *scalar
    }

    fn try_downcast_scalar<'a>(scalar: &'a ScalarRef) -> Option<Self::ScalarRef<'a>> {
        match scalar {
            ScalarRef::Boolean(scalar) => Some(*scalar),
            _ => None,
        }
    }

    fn try_downcast_column(col: &Column) -> Option<Self::Column> {
        match col {
            Column::Boolean(column) => Some(column.clone()),
            _ => None,
        }
    }

    fn try_downcast_builder(builder: &mut ColumnBuilder) -> Option<&mut Self::ColumnBuilder> {
        match builder {
            ColumnBuilder::Boolean(builder) => Some(builder),
            _ => None,
        }
    }

    fn try_downcast_owned_builder(builder: ColumnBuilder) -> Option<Self::ColumnBuilder> {
        match builder {
            ColumnBuilder::Boolean(builder) => Some(builder),
            _ => None,
        }
    }

    fn try_upcast_column_builder(
        builder: Self::ColumnBuilder,
        _decimal_size: Option<DecimalSize>,
    ) -> Option<ColumnBuilder> {
        Some(ColumnBuilder::Boolean(builder))
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        domain.as_boolean().cloned()
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

    fn column_len(col: &Self::Column) -> usize {
        col.len()
    }

    fn index_column(col: &Self::Column, index: usize) -> Option<Self::ScalarRef<'_>> {
        col.get(index)
    }

    #[inline(always)]
    unsafe fn index_column_unchecked(col: &Self::Column, index: usize) -> Self::ScalarRef<'_> {
        debug_assert!(index < col.len());

        col.get_bit_unchecked(index)
    }

    fn slice_column(col: &Self::Column, range: Range<usize>) -> Self::Column {
        col.clone().sliced(range.start, range.end - range.start)
    }

    fn iter_column(col: &Self::Column) -> Self::ColumnIterator<'_> {
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

    fn push_item_repeat(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>, n: usize) {
        if n == 1 {
            builder.push(item)
        } else {
            builder.extend_constant(n, item)
        }
    }

    fn push_default(builder: &mut Self::ColumnBuilder) {
        builder.push(false);
    }

    fn append_column(builder: &mut Self::ColumnBuilder, bitmap: &Self::Column) {
        builder.extend_from_bitmap(bitmap)
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        builder.into()
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        assert_eq!(builder.len(), 1);
        builder.get(0)
    }

    #[inline(always)]
    fn equal(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        left == right
    }

    #[inline(always)]
    fn not_equal(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        left != right
    }

    #[inline(always)]
    fn greater_than(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        left & !right
    }

    #[inline(always)]
    fn greater_than_equal(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        left | !right
    }

    #[inline(always)]
    fn less_than(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        !left & right
    }

    #[inline(always)]
    fn less_than_equal(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        !left | right
    }
}

impl ArgType for BooleanType {
    fn data_type() -> DataType {
        DataType::Boolean
    }

    fn full_domain() -> Self::Domain {
        BooleanDomain {
            has_false: true,
            has_true: true,
        }
    }

    fn create_builder(capacity: usize, _: &GenericMap) -> Self::ColumnBuilder {
        MutableBitmap::with_capacity(capacity)
    }

    fn column_from_iter(iter: impl Iterator<Item = Self::Scalar>, _: &GenericMap) -> Self::Column {
        match iter.size_hint() {
            (_, Some(_)) => unsafe { MutableBitmap::from_trusted_len_iter_unchecked(iter).into() },
            (_, None) => MutableBitmap::from_iter(iter).into(),
        }
    }

    fn column_from_ref_iter<'a>(
        iter: impl Iterator<Item = Self::ScalarRef<'a>>,
        generics: &GenericMap,
    ) -> Self::Column {
        Self::column_from_iter(iter, generics)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BooleanDomain {
    pub has_false: bool,
    pub has_true: bool,
}
