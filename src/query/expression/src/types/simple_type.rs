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

use databend_common_column::buffer::Buffer;

use super::AccessType;
use super::DecimalSize;
use super::Scalar;
use super::ValueType;
use crate::arrow::buffer_into_mut;
use crate::Column;
use crate::ColumnBuilder;
use crate::Domain;
use crate::ScalarRef;

pub trait SimpleType: Debug + Clone + PartialEq + Sized + 'static {
    type Scalar: Debug + Clone + Copy + PartialEq + Eq + Default + Send + 'static;
    type Domain: Debug + Clone + Copy + PartialEq;

    fn downcast_scalar<'a>(scalar: &ScalarRef<'a>) -> Option<Self::Scalar>;
    fn upcast_scalar(scalar: Self::Scalar) -> Scalar;

    fn downcast_column(col: &Column) -> Option<Buffer<Self::Scalar>>;
    fn upcast_column(col: Buffer<Self::Scalar>) -> Column;

    fn downcast_domain(domain: &Domain) -> Option<Self::Domain>;
    fn upcast_domain(domain: Self::Domain) -> Domain;

    fn downcast_builder(builder: &mut ColumnBuilder) -> Option<&mut Vec<Self::Scalar>>;
    fn downcast_owned_builder(builder: ColumnBuilder) -> Option<Vec<Self::Scalar>>;
    fn upcast_column_builder(
        builder: Vec<Self::Scalar>,
        decimal_size: Option<DecimalSize>,
    ) -> Option<ColumnBuilder>;

    fn compare(lhs: &Self::Scalar, rhs: &Self::Scalar) -> Ordering;

    #[inline(always)]
    fn greater_than(left: &Self::Scalar, right: &Self::Scalar) -> bool {
        matches!(Self::compare(left, right), Ordering::Greater)
    }

    #[inline(always)]
    fn less_than(left: &Self::Scalar, right: &Self::Scalar) -> bool {
        matches!(Self::compare(left, right), Ordering::Less)
    }

    #[inline(always)]
    fn greater_than_equal(left: &Self::Scalar, right: &Self::Scalar) -> bool {
        matches!(
            Self::compare(left, right),
            Ordering::Greater | Ordering::Equal
        )
    }

    #[inline(always)]
    fn less_than_equal(left: &Self::Scalar, right: &Self::Scalar) -> bool {
        matches!(Self::compare(left, right), Ordering::Less | Ordering::Equal)
    }
}

#[doc(hidden)]
pub trait SimpleValueType: SimpleType {}

#[doc(hidden)]
impl<T: SimpleValueType> AccessType for T {
    type Scalar = T::Scalar;
    type ScalarRef<'a> = T::Scalar;
    type Column = Buffer<T::Scalar>;
    type Domain = T::Domain;
    type ColumnIterator<'a> = std::iter::Copied<std::slice::Iter<'a, T::Scalar>>;

    fn to_owned_scalar(scalar: Self::ScalarRef<'_>) -> Self::Scalar {
        scalar
    }

    fn to_scalar_ref(scalar: &Self::Scalar) -> Self::ScalarRef<'_> {
        *scalar
    }

    fn try_downcast_scalar<'a>(scalar: &ScalarRef<'a>) -> Option<Self::ScalarRef<'a>> {
        T::downcast_scalar(scalar)
    }

    fn try_downcast_column(col: &Column) -> Option<Buffer<Self::Scalar>> {
        T::downcast_column(col)
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        T::downcast_domain(domain)
    }

    fn upcast_scalar(scalar: Self::Scalar) -> Scalar {
        T::upcast_scalar(scalar)
    }

    fn upcast_column(col: Buffer<Self::Scalar>) -> Column {
        T::upcast_column(col)
    }

    fn upcast_domain(domain: Self::Domain) -> Domain {
        T::upcast_domain(domain)
    }

    fn column_len(buffer: &Buffer<Self::Scalar>) -> usize {
        buffer.len()
    }

    fn index_column(buffer: &Buffer<Self::Scalar>, index: usize) -> Option<Self::ScalarRef<'_>> {
        buffer.get(index).copied()
    }

    unsafe fn index_column_unchecked(
        buffer: &Buffer<Self::Scalar>,
        index: usize,
    ) -> Self::ScalarRef<'_> {
        debug_assert!(index < buffer.len());
        *buffer.get_unchecked(index)
    }

    fn slice_column(buffer: &Buffer<Self::Scalar>, range: Range<usize>) -> Buffer<Self::Scalar> {
        buffer.clone().sliced(range.start, range.end - range.start)
    }

    fn iter_column(buffer: &Buffer<Self::Scalar>) -> Self::ColumnIterator<'_> {
        buffer.iter().copied()
    }

    unsafe fn index_column_unchecked_scalar(
        col: &Buffer<Self::Scalar>,
        index: usize,
    ) -> Self::Scalar {
        Self::to_owned_scalar(Self::index_column_unchecked(col, index))
    }

    fn scalar_memory_size(_: &Self::ScalarRef<'_>) -> usize {
        std::mem::size_of::<Self::Scalar>()
    }

    fn column_memory_size(buffer: &Buffer<Self::Scalar>) -> usize {
        buffer.len() * std::mem::size_of::<Self::Scalar>()
    }

    fn compare(lhs: Self::ScalarRef<'_>, rhs: Self::ScalarRef<'_>) -> Ordering {
        T::compare(&lhs, &rhs)
    }

    fn equal(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        left == right
    }

    fn not_equal(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        left != right
    }

    fn greater_than(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        T::greater_than(&left, &right)
    }

    fn less_than(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        T::less_than(&left, &right)
    }

    fn greater_than_equal(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        T::greater_than_equal(&left, &right)
    }

    fn less_than_equal(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        T::less_than_equal(&left, &right)
    }
}

impl<T: SimpleValueType> ValueType for T {
    type ColumnBuilder = Vec<Self::Scalar>;

    fn try_downcast_builder(builder: &mut ColumnBuilder) -> Option<&mut Vec<Self::Scalar>> {
        T::downcast_builder(builder)
    }

    fn try_downcast_owned_builder(builder: ColumnBuilder) -> Option<Vec<Self::Scalar>> {
        T::downcast_owned_builder(builder)
    }

    fn try_upcast_column_builder(
        builder: Vec<Self::Scalar>,
        decimal_size: Option<DecimalSize>,
    ) -> Option<ColumnBuilder> {
        T::upcast_column_builder(builder, decimal_size)
    }

    fn column_to_builder(buffer: Buffer<Self::Scalar>) -> Vec<Self::Scalar> {
        buffer_into_mut(buffer)
    }

    fn builder_len(builder: &Vec<Self::Scalar>) -> usize {
        builder.len()
    }

    fn push_item(builder: &mut Vec<Self::Scalar>, item: Self::Scalar) {
        builder.push(item);
    }

    fn push_item_repeat(builder: &mut Vec<Self::Scalar>, item: Self::Scalar, n: usize) {
        if n == 1 {
            builder.push(item)
        } else {
            builder.resize(builder.len() + n, item)
        }
    }

    fn push_default(builder: &mut Vec<Self::Scalar>) {
        builder.push(Self::Scalar::default());
    }

    fn append_column(builder: &mut Vec<Self::Scalar>, other: &Buffer<Self::Scalar>) {
        builder.extend_from_slice(other);
    }

    fn build_column(builder: Vec<Self::Scalar>) -> Buffer<Self::Scalar> {
        builder.into()
    }

    fn build_scalar(builder: Vec<Self::Scalar>) -> Self::Scalar {
        assert_eq!(builder.len(), 1);
        builder[0]
    }
}
