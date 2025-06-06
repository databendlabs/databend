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

use super::AccessType;
use super::GenericMap;
use super::ReturnType;
use super::Scalar;
use super::ValueType;
use crate::types::DataType;
use crate::Column;
use crate::ColumnBuilder;
use crate::Domain;
use crate::ScalarRef;

pub trait ZeroSizeType: Debug + Clone + PartialEq + Sized + 'static {
    fn downcast_scalar<'a>(scalar: &ScalarRef<'a>) -> Option<()>;
    fn upcast_scalar() -> Scalar;

    fn downcast_column(col: &Column) -> Option<usize>;
    fn upcast_column(len: usize) -> Column;

    fn downcast_domain(domain: &Domain) -> Option<()>;
    fn upcast_domain() -> Domain;

    fn downcast_builder(builder: &mut ColumnBuilder) -> Option<&mut usize>;
    fn downcast_owned_builder(builder: ColumnBuilder) -> Option<usize>;
    fn upcast_column_builder(len: usize) -> Option<ColumnBuilder>;
}

#[derive(Debug, Clone, PartialEq)]
pub struct ZeroSizeValueType<T: ZeroSizeType>(PhantomData<T>);

impl<T: ZeroSizeType> AccessType for ZeroSizeValueType<T> {
    type Scalar = ();
    type ScalarRef<'a> = ();
    type Column = usize;
    type Domain = ();
    type ColumnIterator<'a> = std::iter::RepeatN<()>;

    fn to_owned_scalar(_: Self::ScalarRef<'_>) -> Self::Scalar {}
    fn to_scalar_ref(_: &Self::Scalar) -> Self::ScalarRef<'_> {}

    fn try_downcast_scalar<'a>(scalar: &ScalarRef<'a>) -> Option<Self::ScalarRef<'a>> {
        T::downcast_scalar(scalar)
    }

    fn try_downcast_column(col: &Column) -> Option<Self::Column> {
        T::downcast_column(col)
    }

    fn try_downcast_domain(domain: &Domain) -> Option<()> {
        T::downcast_domain(domain)
    }

    fn column_len(len: &usize) -> usize {
        *len
    }

    fn index_column(len: &usize, index: usize) -> Option<()> {
        if index < *len {
            Some(())
        } else {
            None
        }
    }

    unsafe fn index_column_unchecked(_: &usize, _: usize) {}

    fn slice_column(len: &usize, range: Range<usize>) -> usize {
        assert!(range.start < *len, "range {range:?} out of 0..{len}");
        range.end - range.start
    }

    fn iter_column(len: &usize) -> Self::ColumnIterator<'_> {
        std::iter::repeat_n((), *len)
    }

    unsafe fn index_column_unchecked_scalar(_: &usize, _: usize) {}

    fn scalar_memory_size(_: &()) -> usize {
        0
    }

    fn column_memory_size(_: &usize) -> usize {
        std::mem::size_of::<usize>()
    }

    fn compare(_: (), _: ()) -> Ordering {
        Ordering::Equal
    }

    fn equal(_left: Self::ScalarRef<'_>, _right: Self::ScalarRef<'_>) -> bool {
        true
    }

    fn not_equal(_left: Self::ScalarRef<'_>, _right: Self::ScalarRef<'_>) -> bool {
        false
    }

    fn greater_than(_left: Self::ScalarRef<'_>, _right: Self::ScalarRef<'_>) -> bool {
        false
    }

    fn less_than(_left: Self::ScalarRef<'_>, _right: Self::ScalarRef<'_>) -> bool {
        false
    }

    fn greater_than_equal(_left: Self::ScalarRef<'_>, _right: Self::ScalarRef<'_>) -> bool {
        true
    }

    fn less_than_equal(_left: Self::ScalarRef<'_>, _right: Self::ScalarRef<'_>) -> bool {
        true
    }
}

impl<T: ZeroSizeType> ValueType for ZeroSizeValueType<T> {
    type ColumnBuilder = usize;

    fn upcast_scalar_with_type(_: (), _: &DataType) -> Scalar {
        T::upcast_scalar()
    }

    fn upcast_domain_with_type(_domain: Self::Domain, _: &DataType) -> Domain {
        T::upcast_domain()
    }

    fn upcast_column_with_type(col: Self::Column, _: &DataType) -> Column {
        T::upcast_column(col)
    }

    fn try_downcast_builder(builder: &mut ColumnBuilder) -> Option<&mut usize> {
        T::downcast_builder(builder)
    }

    fn try_downcast_owned_builder(builder: ColumnBuilder) -> Option<usize> {
        T::downcast_owned_builder(builder)
    }

    fn try_upcast_column_builder(builder: usize, _: &DataType) -> Option<ColumnBuilder> {
        T::upcast_column_builder(builder)
    }

    fn column_to_builder(len: usize) -> usize {
        len
    }

    fn builder_len(builder: &usize) -> usize {
        *builder
    }

    fn push_item(builder: &mut usize, _: ()) {
        *builder += 1
    }

    fn push_item_repeat(builder: &mut usize, _: (), n: usize) {
        *builder += n
    }

    fn push_default(builder: &mut usize) {
        *builder += 1
    }

    fn append_column(builder: &mut usize, other: &usize) {
        *builder += *other
    }

    fn build_column(builder: usize) -> usize {
        builder
    }

    fn build_scalar(builder: usize) {
        assert_eq!(builder, 1);
    }
}

impl<T: ZeroSizeType> ReturnType for ZeroSizeValueType<T> {
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
