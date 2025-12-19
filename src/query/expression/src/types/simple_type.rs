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
use std::marker::PhantomData;
use std::ops::Range;

use databend_common_column::buffer::Buffer;
use databend_common_exception::Result;

use super::AccessType;
use super::BuilderMut;
use super::DataType;
use super::GenericMap;
use super::ReturnType;
use super::Scalar;
use super::ValueType;
use super::column_type_error;
use super::domain_type_error;
use super::scalar_type_error;
use crate::Column;
use crate::ColumnBuilder;
use crate::Domain;
use crate::ScalarRef;
use crate::arrow::buffer_into_mut;

pub trait SimpleType: Debug + Clone + PartialEq + Sized + 'static {
    type Scalar: Debug + Clone + Copy + PartialEq + Eq + Default + Send + 'static;
    type Domain: Debug + Clone + Copy + PartialEq;

    fn downcast_scalar<'a>(scalar: &ScalarRef<'a>) -> Option<Self::Scalar>;
    fn upcast_scalar(scalar: Self::Scalar, data_type: &DataType) -> Scalar;

    fn downcast_column(col: &Column) -> Option<Buffer<Self::Scalar>>;
    fn upcast_column(col: Buffer<Self::Scalar>, data_type: &DataType) -> Column;

    fn downcast_domain(domain: &Domain) -> Option<Self::Domain>;
    fn upcast_domain(domain: Self::Domain, data_type: &DataType) -> Domain;

    fn downcast_builder(builder: &mut ColumnBuilder) -> Option<&mut Vec<Self::Scalar>>;
    fn downcast_owned_builder(builder: ColumnBuilder) -> Option<Vec<Self::Scalar>>;
    fn upcast_column_builder(
        builder: Vec<Self::Scalar>,
        data_type: &DataType,
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

#[derive(Debug, Clone, PartialEq)]
pub struct SimpleValueType<T: SimpleType>(PhantomData<T>);

impl<T: SimpleType> AccessType for SimpleValueType<T> {
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

    fn try_downcast_scalar<'a>(scalar: &ScalarRef<'a>) -> Result<Self::ScalarRef<'a>> {
        T::downcast_scalar(scalar).ok_or_else(|| scalar_type_error::<Self>(scalar))
    }

    fn try_downcast_column(col: &Column) -> Result<Buffer<Self::Scalar>> {
        T::downcast_column(col).ok_or_else(|| column_type_error::<Self>(col))
    }

    fn try_downcast_domain(domain: &Domain) -> Result<Self::Domain> {
        T::downcast_domain(domain).ok_or_else(|| domain_type_error::<Self>(domain))
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
        unsafe {
            debug_assert!(index < buffer.len());
            *buffer.get_unchecked(index)
        }
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
        unsafe { Self::to_owned_scalar(Self::index_column_unchecked(col, index)) }
    }

    fn scalar_memory_size(_: &Self::ScalarRef<'_>) -> usize {
        std::mem::size_of::<Self::Scalar>()
    }

    fn column_memory_size(buffer: &Buffer<Self::Scalar>, _gc: bool) -> usize {
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

impl<T: SimpleType> ValueType for SimpleValueType<T> {
    type ColumnBuilder = Vec<Self::Scalar>;
    type ColumnBuilderMut<'a> = BuilderMut<'a, Self>;

    fn upcast_scalar_with_type(scalar: Self::Scalar, data_type: &DataType) -> Scalar {
        T::upcast_scalar(scalar, data_type)
    }

    fn upcast_domain_with_type(domain: Self::Domain, data_type: &DataType) -> Domain {
        T::upcast_domain(domain, data_type)
    }

    fn upcast_column_with_type(col: Buffer<Self::Scalar>, data_type: &DataType) -> Column {
        T::upcast_column(col, data_type)
    }

    fn downcast_builder(builder: &mut ColumnBuilder) -> Self::ColumnBuilderMut<'_> {
        T::downcast_builder(builder).unwrap().into()
    }

    fn try_upcast_column_builder(
        builder: Vec<Self::Scalar>,
        data_type: &DataType,
    ) -> Option<ColumnBuilder> {
        T::upcast_column_builder(builder, data_type)
    }

    fn column_to_builder(buffer: Buffer<Self::Scalar>) -> Vec<Self::Scalar> {
        buffer_into_mut(buffer)
    }

    fn builder_len(builder: &Vec<Self::Scalar>) -> usize {
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
            let new_len = builder.len() + n;
            builder.resize(new_len, item)
        }
    }

    fn push_default_mut(builder: &mut Self::ColumnBuilderMut<'_>) {
        builder.push(Self::Scalar::default());
    }

    fn append_column_mut(builder: &mut Self::ColumnBuilderMut<'_>, other: &Self::Column) {
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

impl<T: SimpleType> ReturnType for SimpleValueType<T> {
    fn create_builder(capacity: usize, _: &GenericMap) -> Self::ColumnBuilder {
        Vec::with_capacity(capacity)
    }
}
