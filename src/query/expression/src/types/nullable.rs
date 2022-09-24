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

use std::marker::PhantomData;
use std::ops::Range;

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_arrow::arrow::trusted_len::TrustedLen;

use super::AnyType;
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
pub struct NullableType<T: ValueType>(PhantomData<T>);

impl<T: ValueType> ValueType for NullableType<T> {
    type Scalar = Option<T::Scalar>;
    type ScalarRef<'a> = Option<T::ScalarRef<'a>>;
    type Column = NullableColumn<T>;
    type Domain = NullableDomain<T>;
    type ColumnIterator<'a> = NullableIterator<'a, T>;
    type ColumnBuilder = NullableColumnBuilder<T>;

    #[inline]
    fn upcast_gat<'short, 'long: 'short>(
        long: Option<T::ScalarRef<'long>>,
    ) -> Option<T::ScalarRef<'short>> {
        long.map(|long| T::upcast_gat(long))
    }

    fn to_owned_scalar<'a>(scalar: Self::ScalarRef<'a>) -> Self::Scalar {
        scalar.map(T::to_owned_scalar)
    }

    fn to_scalar_ref<'a>(scalar: &'a Self::Scalar) -> Self::ScalarRef<'a> {
        scalar.as_ref().map(T::to_scalar_ref)
    }

    fn try_downcast_scalar<'a>(scalar: &'a ScalarRef) -> Option<Self::ScalarRef<'a>> {
        match scalar {
            ScalarRef::Null => Some(None),
            scalar => Some(Some(T::try_downcast_scalar(scalar)?)),
        }
    }

    fn try_downcast_column<'a>(col: &'a Column) -> Option<Self::Column> {
        NullableColumn::try_downcast(col.as_nullable()?)
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        match domain {
            Domain::Nullable(NullableDomain {
                has_null,
                value: Some(value),
            }) => Some(NullableDomain {
                has_null: *has_null,
                value: Some(Box::new(T::try_downcast_domain(value)?)),
            }),
            Domain::Nullable(NullableDomain {
                has_null,
                value: None,
            }) => Some(NullableDomain {
                has_null: *has_null,
                value: None,
            }),
            _ => None,
        }
    }

    fn try_downcast_builder<'a>(
        _builder: &'a mut ColumnBuilder,
    ) -> Option<&'a mut Self::ColumnBuilder> {
        None
    }

    fn upcast_scalar(scalar: Self::Scalar) -> Scalar {
        match scalar {
            Some(scalar) => T::upcast_scalar(scalar),
            None => Scalar::Null,
        }
    }

    fn upcast_column(col: Self::Column) -> Column {
        Column::Nullable(Box::new(col.upcast()))
    }

    fn upcast_domain(domain: Self::Domain) -> Domain {
        Domain::Nullable(NullableDomain {
            has_null: domain.has_null,
            value: domain.value.map(|value| Box::new(T::upcast_domain(*value))),
        })
    }

    fn column_len<'a>(col: &'a Self::Column) -> usize {
        col.len()
    }

    fn index_column<'a>(col: &'a Self::Column, index: usize) -> Option<Self::ScalarRef<'a>> {
        col.index(index)
    }

    unsafe fn index_column_unchecked<'a>(
        col: &'a Self::Column,
        index: usize,
    ) -> Self::ScalarRef<'a> {
        col.index_unchecked(index)
    }

    fn slice_column<'a>(col: &'a Self::Column, range: Range<usize>) -> Self::Column {
        col.slice(range)
    }

    fn iter_column<'a>(col: &'a Self::Column) -> Self::ColumnIterator<'a> {
        col.iter()
    }

    fn column_to_builder(col: Self::Column) -> Self::ColumnBuilder {
        NullableColumnBuilder::from_column(col)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        builder.len()
    }

    fn push_item(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>) {
        builder.push(item);
    }

    fn push_default(builder: &mut Self::ColumnBuilder) {
        builder.push_null();
    }

    fn append_builder(builder: &mut Self::ColumnBuilder, other_builder: &Self::ColumnBuilder) {
        builder.append(other_builder);
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

    fn create_builder(capacity: usize, generics: &GenericMap) -> Self::ColumnBuilder {
        NullableColumnBuilder::with_capacity(capacity, generics)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NullableColumn<T: ValueType> {
    pub column: T::Column,
    pub validity: Bitmap,
}

impl<T: ValueType> NullableColumn<T> {
    pub fn len(&self) -> usize {
        self.validity.len()
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
        match self.validity.get_bit_unchecked(index) {
            true => Some(T::index_column(&self.column, index).unwrap()),
            false => None,
        }
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        NullableColumn {
            validity: self
                .validity
                .clone()
                .slice(range.start, range.end - range.start),
            column: T::slice_column(&self.column, range),
        }
    }

    pub fn iter(&self) -> NullableIterator<T> {
        NullableIterator {
            iter: T::iter_column(&self.column),
            validity: self.validity.iter(),
        }
    }

    pub fn upcast(self) -> NullableColumn<AnyType> {
        NullableColumn {
            column: T::upcast_column(self.column),
            validity: self.validity,
        }
    }
}

impl NullableColumn<AnyType> {
    pub fn try_downcast<T: ValueType>(&self) -> Option<NullableColumn<T>> {
        Some(NullableColumn {
            column: T::try_downcast_column(&self.column)?,
            validity: self.validity.clone(),
        })
    }
}

pub struct NullableIterator<'a, T: ValueType> {
    iter: T::ColumnIterator<'a>,
    validity: common_arrow::arrow::bitmap::utils::BitmapIter<'a>,
}

impl<'a, T: ValueType> Iterator for NullableIterator<'a, T> {
    type Item = Option<T::ScalarRef<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .zip(self.validity.next())
            .map(
                |(scalar, is_not_null)| {
                    if is_not_null { Some(scalar) } else { None }
                },
            )
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        assert_eq!(self.iter.size_hint(), self.validity.size_hint());
        self.validity.size_hint()
    }
}

unsafe impl<'a, T: ValueType> TrustedLen for NullableIterator<'a, T> {}

#[derive(Debug, Clone, PartialEq)]
pub struct NullableColumnBuilder<T: ValueType> {
    pub builder: T::ColumnBuilder,
    pub validity: MutableBitmap,
}

impl<T: ValueType> NullableColumnBuilder<T> {
    pub fn from_column(col: NullableColumn<T>) -> Self {
        NullableColumnBuilder {
            builder: T::column_to_builder(col.column),
            validity: bitmap_into_mut(col.validity),
        }
    }

    pub fn len(&self) -> usize {
        self.validity.len()
    }

    pub fn push(&mut self, item: Option<T::ScalarRef<'_>>) {
        match item {
            Some(scalar) => {
                T::push_item(&mut self.builder, scalar);
                self.validity.push(true);
            }
            None => self.push_null(),
        }
    }

    pub fn push_null(&mut self) {
        T::push_default(&mut self.builder);
        self.validity.push(false);
    }

    pub fn append(&mut self, other: &Self) {
        T::append_builder(&mut self.builder, &other.builder);
        self.validity
            .extend_from_slice(other.validity.as_slice(), 0, other.validity.len());
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
}

impl<T: ArgType> NullableColumnBuilder<T> {
    pub fn with_capacity(capacity: usize, generics: &GenericMap) -> Self {
        NullableColumnBuilder {
            builder: T::create_builder(capacity, generics),
            validity: MutableBitmap::with_capacity(capacity),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NullableDomain<T: ValueType> {
    pub has_null: bool,
    // `None` means all rows are `NULL`s.
    //
    // Invariant: `has_null` must be `true` when `value` is `None`.
    pub value: Option<Box<T::Domain>>,
}
