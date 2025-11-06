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
use std::iter::TrustedLen;
use std::marker::PhantomData;
use std::ops::Range;

use databend_common_column::bitmap::Bitmap;
use databend_common_column::bitmap::MutableBitmap;
use databend_common_exception::Result;

use super::column_type_error;
use super::domain_type_error;
use super::AccessType;
use super::AnyType;
use super::ArgType;
use super::BuilderExt;
use super::DataType;
use super::GenericMap;
use super::ReturnType;
use super::ValueType;
use crate::property::Domain;
use crate::utils::arrow::bitmap_into_mut;
use crate::values::Column;
use crate::values::Scalar;
use crate::ColumnBuilder;
use crate::ColumnVec;
use crate::ScalarRef;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NullableType<T>(PhantomData<T>);

impl<T: AccessType> AccessType for NullableType<T> {
    type Scalar = Option<T::Scalar>;
    type ScalarRef<'a> = Option<T::ScalarRef<'a>>;
    type Column = NullableColumn<T>;
    type Domain = NullableDomain<T>;
    type ColumnIterator<'a> = NullableIterator<'a, T>;

    fn to_owned_scalar(scalar: Self::ScalarRef<'_>) -> Self::Scalar {
        scalar.map(T::to_owned_scalar)
    }

    fn to_scalar_ref(scalar: &Self::Scalar) -> Self::ScalarRef<'_> {
        scalar.as_ref().map(T::to_scalar_ref)
    }

    fn try_downcast_scalar<'a>(scalar: &ScalarRef<'a>) -> Result<Self::ScalarRef<'a>> {
        match scalar {
            ScalarRef::Null => Ok(None),
            scalar => Ok(Some(T::try_downcast_scalar(scalar)?)),
        }
    }

    fn try_downcast_column(col: &Column) -> Result<Self::Column> {
        let nullable = col
            .as_nullable()
            .ok_or_else(|| column_type_error::<Self>(col))?;
        NullableColumn::try_downcast(nullable)
    }

    fn try_downcast_domain(domain: &Domain) -> Result<Self::Domain> {
        match domain {
            Domain::Nullable(NullableDomain {
                has_null,
                value: Some(value),
            }) => Ok(NullableDomain {
                has_null: *has_null,
                value: Some(Box::new(T::try_downcast_domain(value)?)),
            }),
            Domain::Nullable(NullableDomain {
                has_null,
                value: None,
            }) => Ok(NullableDomain {
                has_null: *has_null,
                value: None,
            }),
            _ => Err(domain_type_error::<Self>(domain)),
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
        match scalar {
            Some(scalar) => T::scalar_memory_size(scalar),
            None => 0,
        }
    }

    fn column_memory_size(col: &Self::Column) -> usize {
        col.memory_size()
    }

    // Null default lastly
    #[inline(always)]
    fn compare(lhs: Self::ScalarRef<'_>, rhs: Self::ScalarRef<'_>) -> Ordering {
        match (lhs, rhs) {
            (Some(lhs), Some(rhs)) => T::compare(lhs, rhs),
            (Some(_), None) => Ordering::Greater,
            (None, Some(_)) => Ordering::Less,
            (None, None) => Ordering::Equal,
        }
    }

    unsafe fn index_column_unchecked_scalar(col: &Self::Column, index: usize) -> Self::Scalar {
        Self::to_owned_scalar(Self::index_column_unchecked(col, index))
    }

    fn equal(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        std::matches!(Self::compare(left, right), Ordering::Equal)
    }

    fn not_equal(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        !std::matches!(Self::compare(left, right), Ordering::Equal)
    }

    fn greater_than(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        std::matches!(Self::compare(left, right), Ordering::Greater)
    }

    fn less_than(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        std::matches!(Self::compare(left, right), Ordering::Less)
    }

    fn greater_than_equal(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        !std::matches!(Self::compare(left, right), Ordering::Less)
    }

    fn less_than_equal(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        !std::matches!(Self::compare(left, right), Ordering::Greater)
    }
}

impl<T: ValueType> ValueType for NullableType<T> {
    type ColumnBuilder = NullableColumnBuilder<T>;
    type ColumnBuilderMut<'a> = NullableColumnBuilderMut<'a, T>;

    fn upcast_scalar_with_type(scalar: Self::Scalar, data_type: &DataType) -> Scalar {
        let value_type = data_type.as_nullable().unwrap();
        match scalar {
            Some(scalar) => T::upcast_scalar_with_type(scalar, value_type),
            None => Scalar::Null,
        }
    }

    fn upcast_domain_with_type(domain: Self::Domain, data_type: &DataType) -> Domain {
        let value_type = data_type.as_nullable().unwrap();
        Domain::Nullable(NullableDomain {
            has_null: domain.has_null,
            value: domain
                .value
                .map(|value| Box::new(T::upcast_domain_with_type(*value, value_type))),
        })
    }

    fn upcast_column_with_type(col: Self::Column, data_type: &DataType) -> Column {
        let value_type = data_type.as_nullable().unwrap();
        Column::Nullable(Box::new(NullableColumn::new(
            T::upcast_column_with_type(col.column, value_type),
            col.validity,
        )))
    }

    fn downcast_builder(builder: &mut ColumnBuilder) -> Self::ColumnBuilderMut<'_> {
        let any_nullable = builder.as_nullable_mut().unwrap();
        NullableColumnBuilderMut {
            builder: T::downcast_builder(&mut any_nullable.builder),
            validity: &mut any_nullable.validity,
        }
    }

    fn try_upcast_column_builder(
        builder: Self::ColumnBuilder,
        data_type: &DataType,
    ) -> Option<ColumnBuilder> {
        Some(ColumnBuilder::Nullable(Box::new(builder.upcast(data_type))))
    }

    fn column_to_builder(col: Self::Column) -> Self::ColumnBuilder {
        NullableColumnBuilder::from_column(col)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        builder.len()
    }

    fn builder_len_mut(builder: &Self::ColumnBuilderMut<'_>) -> usize {
        builder.validity.len()
    }

    fn push_item_mut(builder: &mut Self::ColumnBuilderMut<'_>, item: Self::ScalarRef<'_>) {
        match item {
            Some(item) => builder.push(item),
            None => builder.push_null(),
        }
    }

    fn push_item_repeat_mut(
        builder: &mut Self::ColumnBuilderMut<'_>,
        item: Self::ScalarRef<'_>,
        n: usize,
    ) {
        match item {
            Some(item) => {
                T::push_item_repeat_mut(&mut builder.builder, item, n);
                builder.validity.extend_constant(n, true);
            }
            None => {
                for _ in 0..n {
                    T::push_default_mut(&mut builder.builder);
                }
                builder.validity.extend_constant(n, false);
            }
        }
    }

    fn push_default_mut(builder: &mut Self::ColumnBuilderMut<'_>) {
        T::push_default_mut(&mut builder.builder);
        builder.validity.push(false);
    }

    fn append_column_mut(builder: &mut Self::ColumnBuilderMut<'_>, other: &Self::Column) {
        T::append_column_mut(&mut builder.builder, &other.column);
        builder.validity.extend_from_bitmap(&other.validity);
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        builder.build()
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        builder.build_scalar()
    }
}

impl<T: ArgType> ArgType for NullableType<T> {
    fn data_type() -> DataType {
        DataType::Nullable(Box::new(T::data_type()))
    }

    fn full_domain() -> Self::Domain {
        NullableDomain {
            has_null: true,
            value: Some(Box::new(T::full_domain())),
        }
    }
}

impl<T: ReturnType> ReturnType for NullableType<T> {
    fn create_builder(capacity: usize, generics: &GenericMap) -> Self::ColumnBuilder {
        NullableColumnBuilder::with_capacity(capacity, generics)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NullableColumn<T: AccessType> {
    pub column: T::Column,
    pub validity: Bitmap,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NullableColumnVec {
    pub column: ColumnVec,
    pub validity: ColumnVec,
}

impl<T: AccessType> NullableColumn<T> {
    pub fn len(&self) -> usize {
        self.validity.len()
    }

    pub fn column(&self) -> &T::Column {
        &self.column
    }

    pub fn validity(&self) -> &Bitmap {
        &self.validity
    }

    pub fn index(&self, index: usize) -> Option<Option<T::ScalarRef<'_>>> {
        match self.validity.get(index) {
            Some(true) => Some(Some(T::index_column(&self.column, index).unwrap())),
            Some(false) => Some(None),
            None => None,
        }
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    pub unsafe fn index_unchecked(&self, index: usize) -> Option<T::ScalarRef<'_>> {
        // we need to check the validity firstly
        // cause `self.validity.get_bit_unchecked` may check the index from buffer address with `true` result
        if index < self.validity.len() {
            match self.validity.get_bit_unchecked(index) {
                true => Some(T::index_column_unchecked(&self.column, index)),
                false => None,
            }
        } else {
            None
        }
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        NullableColumn {
            validity: self
                .validity
                .clone()
                .sliced(range.start, range.end - range.start),
            column: T::slice_column(&self.column, range),
        }
    }

    pub fn iter(&self) -> NullableIterator<T> {
        NullableIterator {
            iter: T::iter_column(&self.column),
            validity: self.validity.iter(),
        }
    }

    pub fn memory_size(&self) -> usize {
        T::column_memory_size(&self.column) + self.validity.as_slice().0.len()
    }

    pub fn destructure(self) -> (T::Column, Bitmap) {
        (self.column, self.validity)
    }
}

impl<T: ReturnType> NullableColumn<T> {
    // though column and validity are public
    // we should better use new to create a new instance to ensure the validity and column are consistent
    // todo: make column and validity private
    pub fn new_unchecked(column: T::Column, validity: Bitmap) -> Self {
        debug_assert_eq!(T::column_len(&column), validity.len());
        NullableColumn { column, validity }
    }
}

impl NullableColumn<AnyType> {
    pub fn new(column: Column, validity: Bitmap) -> Self {
        debug_assert_eq!(column.len(), validity.len());
        debug_assert!(!column.is_nullable() && !column.is_null());
        NullableColumn { column, validity }
    }
}

impl<T: ArgType> NullableColumn<T> {
    pub fn upcast(self) -> NullableColumn<AnyType> {
        NullableColumn::new(
            T::upcast_column_with_type(self.column, &T::data_type()),
            self.validity,
        )
    }
}

impl NullableColumn<AnyType> {
    pub fn try_downcast<T: AccessType>(&self) -> Result<NullableColumn<T>> {
        let inner = T::try_downcast_column(&self.column)?;
        debug_assert_eq!(T::column_len(&inner), self.validity.len());
        Ok(NullableColumn {
            column: inner,
            validity: self.validity.clone(),
        })
    }

    pub fn new_column(column: Column, validity: Bitmap) -> Column {
        debug_assert_eq!(column.len(), validity.len());
        debug_assert!(!matches!(column, Column::Nullable(_)));
        Column::Nullable(Box::new(NullableColumn { column, validity }))
    }
}

pub struct NullableIterator<'a, T: AccessType> {
    iter: T::ColumnIterator<'a>,
    validity: databend_common_column::bitmap::utils::BitmapIter<'a>,
}

impl<'a, T: AccessType> Iterator for NullableIterator<'a, T> {
    type Item = Option<T::ScalarRef<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .zip(self.validity.next())
            .map(
                |(scalar, is_not_null)| {
                    if is_not_null {
                        Some(scalar)
                    } else {
                        None
                    }
                },
            )
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        assert_eq!(self.iter.size_hint(), self.validity.size_hint());
        self.validity.size_hint()
    }
}

unsafe impl<T: AccessType> TrustedLen for NullableIterator<'_, T> {}

#[derive(Debug, Clone, PartialEq)]
pub struct NullableColumnBuilder<T: ValueType> {
    pub builder: T::ColumnBuilder,
    pub validity: MutableBitmap,
}

#[derive(Debug)]
pub struct NullableColumnBuilderMut<'a, T: ValueType> {
    builder: T::ColumnBuilderMut<'a>,
    validity: &'a mut MutableBitmap,
}

impl<'a, T: ValueType> From<&'a mut NullableColumnBuilder<T>> for NullableColumnBuilderMut<'a, T> {
    fn from(builder: &'a mut NullableColumnBuilder<T>) -> Self {
        NullableColumnBuilderMut {
            builder: (&mut builder.builder).into(),
            validity: &mut builder.validity,
        }
    }
}

impl<T: ValueType> BuilderExt<NullableType<T>> for NullableColumnBuilderMut<'_, T> {
    fn len(&self) -> usize {
        NullableType::<T>::builder_len_mut(self)
    }

    fn push_item(&mut self, item: <NullableType<T> as AccessType>::ScalarRef<'_>) {
        NullableType::<T>::push_item_mut(self, item);
    }

    fn push_repeat(&mut self, item: <NullableType<T> as AccessType>::ScalarRef<'_>, n: usize) {
        NullableType::<T>::push_item_repeat_mut(self, item, n);
    }

    fn push_default(&mut self) {
        NullableType::<T>::push_default_mut(self);
    }

    fn append_column(&mut self, other: &<NullableType<T> as AccessType>::Column) {
        NullableType::<T>::append_column_mut(self, other);
    }
}

impl<T: ValueType> NullableColumnBuilder<T> {
    pub fn as_mut(&mut self) -> NullableColumnBuilderMut<'_, T> {
        self.into()
    }

    pub fn from_column(col: NullableColumn<T>) -> Self {
        NullableColumnBuilder {
            builder: T::column_to_builder(col.column),
            validity: bitmap_into_mut(col.validity),
        }
    }

    pub fn len(&self) -> usize {
        self.validity.len()
    }

    pub fn push(&mut self, item: T::ScalarRef<'_>) {
        self.as_mut().push(item);
    }

    pub fn push_repeat(&mut self, item: T::ScalarRef<'_>, n: usize) {
        T::push_item_repeat(&mut self.builder, item, n);
        self.validity.extend_constant(n, true)
    }

    pub fn push_null(&mut self) {
        self.as_mut().push_null();
    }

    pub fn push_repeat_null(&mut self, repeat: usize) {
        for _ in 0..repeat {
            T::push_default(&mut self.builder);
        }
        self.validity.extend_constant(repeat, false);
    }

    pub fn append_column(&mut self, other: &NullableColumn<T>) {
        T::append_column(&mut self.builder, &other.column);
        self.validity.extend_from_bitmap(&other.validity)
    }

    pub fn build(self) -> NullableColumn<T> {
        assert_eq!(self.validity.len(), T::builder_len(&self.builder));
        NullableColumn {
            column: T::build_column(self.builder),
            validity: self.validity.into(),
        }
    }

    pub fn build_scalar(self) -> Option<T::Scalar> {
        assert_eq!(T::builder_len(&self.builder), 1);
        assert_eq!(self.validity.len(), 1);
        if self.validity.get(0) {
            Some(T::build_scalar(self.builder))
        } else {
            None
        }
    }

    pub fn upcast(self, data_type: &DataType) -> NullableColumnBuilder<AnyType> {
        let value_type = data_type.as_nullable().unwrap();
        NullableColumnBuilder {
            builder: T::try_upcast_column_builder(self.builder, value_type).unwrap(),
            validity: self.validity,
        }
    }
}

impl<T: ValueType> NullableColumnBuilderMut<'_, T> {
    pub fn push(&mut self, item: T::ScalarRef<'_>) {
        T::push_item_mut(&mut self.builder, item);
        self.validity.push(true);
    }

    pub fn push_null(&mut self) {
        T::push_default_mut(&mut self.builder);
        self.validity.push(false);
    }
}

impl<T: ReturnType> NullableColumnBuilder<T> {
    pub fn with_capacity(capacity: usize, generics: &GenericMap) -> Self {
        NullableColumnBuilder {
            builder: T::create_builder(capacity, generics),
            validity: MutableBitmap::with_capacity(capacity),
        }
    }
}

impl NullableColumnBuilder<AnyType> {
    pub fn pop(&mut self) -> Option<Option<Scalar>> {
        if self.validity.pop()? {
            Some(Some(self.builder.pop().unwrap()))
        } else {
            self.builder.pop().unwrap();
            Some(None)
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NullableDomain<T: AccessType> {
    pub has_null: bool,
    // `None` means all rows are `NULL`s.
    //
    // Invariant: `has_null` must be `true` when `value` is `None`.
    pub value: Option<Box<T::Domain>>,
}
