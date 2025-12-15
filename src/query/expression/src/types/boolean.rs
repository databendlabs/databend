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
use std::fmt::Debug;
use std::ops::Range;

pub use databend_common_column::bitmap::*;
use databend_common_exception::Result;

use super::column_type_error;
use super::domain_type_error;
use super::scalar_type_error;
use super::AccessType;
use super::ArgType;
use super::BuilderMut;
use super::DataType;
use super::GenericMap;
use super::ReturnType;
use super::ValueType;
use crate::property::Domain;
use crate::utils::arrow::bitmap_into_mut;
use crate::values::Column;
use crate::values::Scalar;
use crate::ColumnBuilder;
use crate::ColumnView;
use crate::ScalarRef;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BooleanType;

impl AccessType for BooleanType {
    type Scalar = bool;
    type ScalarRef<'a> = bool;
    type Column = Bitmap;
    type Domain = BooleanDomain;
    type ColumnIterator<'a> = databend_common_column::bitmap::utils::BitmapIter<'a>;

    fn to_owned_scalar(scalar: Self::ScalarRef<'_>) -> Self::Scalar {
        scalar
    }

    fn to_scalar_ref(scalar: &Self::Scalar) -> Self::ScalarRef<'_> {
        *scalar
    }

    fn try_downcast_scalar<'a>(scalar: &ScalarRef<'a>) -> Result<Self::ScalarRef<'a>> {
        match scalar {
            ScalarRef::Boolean(scalar) => Ok(*scalar),
            _ => Err(scalar_type_error::<Self>(scalar)),
        }
    }

    fn try_downcast_column(col: &Column) -> Result<Self::Column> {
        match col {
            Column::Boolean(column) => Ok(column.clone()),
            _ => Err(column_type_error::<Self>(col)),
        }
    }

    fn try_downcast_domain(domain: &Domain) -> Result<Self::Domain> {
        domain
            .as_boolean()
            .cloned()
            .ok_or_else(|| domain_type_error::<Self>(domain))
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

    #[inline(always)]
    fn compare(lhs: Self::ScalarRef<'_>, rhs: Self::ScalarRef<'_>) -> Ordering {
        lhs.cmp(&rhs)
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

impl ValueType for BooleanType {
    type ColumnBuilder = MutableBitmap;
    type ColumnBuilderMut<'a> = BuilderMut<'a, Self>;

    fn upcast_scalar_with_type(scalar: Self::Scalar, data_type: &DataType) -> Scalar {
        debug_assert!(data_type.is_boolean());
        Scalar::Boolean(scalar)
    }

    fn upcast_domain_with_type(domain: Self::Domain, data_type: &DataType) -> Domain {
        debug_assert!(data_type.is_boolean());
        Domain::Boolean(domain)
    }

    fn upcast_column_with_type(col: Self::Column, data_type: &DataType) -> Column {
        debug_assert!(data_type.is_boolean());
        Column::Boolean(col)
    }

    fn downcast_builder(builder: &mut ColumnBuilder) -> Self::ColumnBuilderMut<'_> {
        builder.as_boolean_mut().unwrap().into()
    }

    fn try_upcast_column_builder(
        builder: Self::ColumnBuilder,
        data_type: &DataType,
    ) -> Option<ColumnBuilder> {
        debug_assert!(data_type.is_boolean());
        Some(ColumnBuilder::Boolean(builder))
    }

    fn column_to_builder(col: Self::Column) -> Self::ColumnBuilder {
        bitmap_into_mut(col)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        builder.len()
    }

    fn builder_len_mut(builder: &Self::ColumnBuilderMut<'_>) -> usize {
        builder.len()
    }

    fn push_item_mut(builder: &mut Self::ColumnBuilderMut<'_>, item: Self::ScalarRef<'_>) {
        builder.push(item);
    }

    fn push_item_repeat_mut(
        builder: &mut Self::ColumnBuilderMut<'_>,
        item: Self::ScalarRef<'_>,
        n: usize,
    ) {
        if n == 1 {
            builder.push(item)
        } else {
            builder.extend_constant(n, item)
        }
    }

    fn push_default_mut(builder: &mut Self::ColumnBuilderMut<'_>) {
        builder.push(false);
    }

    fn append_column_mut(builder: &mut Self::ColumnBuilderMut<'_>, other: &Self::Column) {
        builder.extend_from_bitmap(other);
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

    fn full_domain() -> Self::Domain {
        BooleanDomain {
            has_false: true,
            has_true: true,
        }
    }
}

impl ReturnType for BooleanType {
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

impl ColumnView<BooleanType> {
    pub fn and_bitmap(&self, rhs: Option<&Bitmap>) -> Self {
        debug_assert!(rhs.map(|rhs| rhs.len() == self.len()).unwrap_or(true));
        use ColumnView::*;
        match (self, rhs) {
            (Const(false, _), _) | (Const(true, _), None) => self.clone(),
            (Const(true, n), Some(rhs)) => {
                if rhs.null_count() == 0 {
                    Const(true, *n)
                } else if rhs.true_count() == 0 {
                    Const(false, *n)
                } else {
                    Column(rhs.clone())
                }
            }
            (Column(b), None) => {
                if b.null_count() == 0 {
                    Const(true, b.len())
                } else if b.true_count() == 0 {
                    Const(false, b.len())
                } else {
                    Column(b.clone())
                }
            }
            (Column(b), Some(rhs)) => {
                let merge = b & rhs;
                if merge.null_count() == 0 {
                    Const(true, merge.len())
                } else if merge.true_count() == 0 {
                    Const(false, merge.len())
                } else {
                    Column(merge)
                }
            }
        }
    }
}
