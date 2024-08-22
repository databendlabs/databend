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

use std::marker::PhantomData;
use std::ops::Range;

use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::bitmap::MutableBitmap;
use databend_common_arrow::arrow::trusted_len::TrustedLen;

use super::AnyType;
use super::DecimalSize;
use crate::property::Domain;
use crate::types::ArgType;
use crate::types::DataType;
use crate::types::GenericMap;
use crate::types::ValueType;
use crate::utils::arrow::bitmap_into_mut;
use crate::values::Column;
use crate::values::Scalar;
use crate::ColumnBuilder;
use crate::ColumnVec;
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

    fn to_owned_scalar(scalar: Self::ScalarRef<'_>) -> Self::Scalar {
        scalar.map(T::to_owned_scalar)
    }

    fn to_scalar_ref(scalar: &Self::Scalar) -> Self::ScalarRef<'_> {
        scalar.as_ref().map(T::to_scalar_ref)
    }

    fn try_downcast_scalar<'a>(scalar: &'a ScalarRef) -> Option<Self::ScalarRef<'a>> {
        match scalar {
            ScalarRef::Null => Some(None),
            scalar => Some(Some(T::try_downcast_scalar(scalar)?)),
        }
    }

    fn try_downcast_column(col: &Column) -> Option<Self::Column> {
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

    fn try_downcast_builder(_builder: &mut ColumnBuilder) -> Option<&mut Self::ColumnBuilder> {
        None
    }

    #[allow(clippy::manual_map)]
    fn try_downcast_owned_builder(builder: ColumnBuilder) -> Option<Self::ColumnBuilder> {
        match builder {
            ColumnBuilder::Nullable(inner) => {
                let builder = T::try_downcast_owned_builder(inner.builder);
                // ```
                // builder.map(|builder| NullableColumnBuilder {
                //     builder,
                //     validity: inner.validity,
                // })
                // ```
                // If we using the clippy recommend way like above, the compiler will complain:
                // use of partially moved value: `inner`.
                // That's rust borrow checker error, if we using the new borrow checker named polonius,
                // everything goes fine, but polonius is very slow, so we allow manual map here.
                if let Some(builder) = builder {
                    Some(NullableColumnBuilder {
                        builder,
                        validity: inner.validity,
                    })
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    fn try_upcast_column_builder(
        builder: Self::ColumnBuilder,
        decimal_size: Option<DecimalSize>,
    ) -> Option<ColumnBuilder> {
        Some(ColumnBuilder::Nullable(Box::new(
            builder.upcast(decimal_size),
        )))
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

    fn column_to_builder(col: Self::Column) -> Self::ColumnBuilder {
        NullableColumnBuilder::from_column(col)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        builder.len()
    }

    fn push_item(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>) {
        match item {
            Some(item) => builder.push(item),
            None => builder.push_null(),
        }
    }

    fn push_item_repeat(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>, n: usize) {
        match item {
            Some(item) => builder.push_repeat(item, n),
            None => {
                for _ in 0..n {
                    builder.push_null()
                }
            }
        }
    }

    fn push_default(builder: &mut Self::ColumnBuilder) {
        builder.push_null();
    }

    fn append_column(builder: &mut Self::ColumnBuilder, other: &Self::Column) {
        builder.append_column(other);
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        builder.build()
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        builder.build_scalar()
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

    fn create_builder(capacity: usize, generics: &GenericMap) -> Self::ColumnBuilder {
        NullableColumnBuilder::with_capacity(capacity, generics)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct NullableColumn<T: ValueType> {
    pub column: T::Column,
    pub validity: Bitmap,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NullableColumnVec {
    pub column: ColumnVec,
    pub validity: ColumnVec,
}

impl<T: ValueType> NullableColumn<T> {
    // though column and validity are public
    // we should better use new to create a new instance to ensure the validity and column are consistent
    // todo: make column and validity private
    pub fn new(column: T::Column, validity: Bitmap) -> Self {
        debug_assert_eq!(T::column_len(&column), validity.len());
        debug_assert!(!matches!(
            T::upcast_column(column.clone()),
            Column::Nullable(_)
        ));
        NullableColumn { column, validity }
    }

    pub fn len(&self) -> usize {
        self.validity.len()
    }

    pub fn column_ref(&self) -> &T::Column {
        &self.column
    }

    pub fn validity_ref(&self) -> &Bitmap {
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

    pub fn upcast(self) -> NullableColumn<AnyType> {
        NullableColumn::new(T::upcast_column(self.column), self.validity)
    }

    pub fn memory_size(&self) -> usize {
        T::column_memory_size(&self.column) + self.validity.as_slice().0.len()
    }
}

impl NullableColumn<AnyType> {
    pub fn try_downcast<T: ValueType>(&self) -> Option<NullableColumn<T>> {
        Some(NullableColumn::new(
            T::try_downcast_column(&self.column)?,
            self.validity.clone(),
        ))
    }

    pub fn new_column(column: Column, validity: Bitmap) -> Column {
        debug_assert_eq!(column.len(), validity.len());
        debug_assert!(!matches!(column, Column::Nullable(_)));
        Column::Nullable(Box::new(NullableColumn { column, validity }))
    }
}

pub struct NullableIterator<'a, T: ValueType> {
    iter: T::ColumnIterator<'a>,
    validity: databend_common_arrow::arrow::bitmap::utils::BitmapIter<'a>,
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

    pub fn push(&mut self, item: T::ScalarRef<'_>) {
        T::push_item(&mut self.builder, item);
        self.validity.push(true);
    }

    pub fn push_repeat(&mut self, item: T::ScalarRef<'_>, n: usize) {
        T::push_item_repeat(&mut self.builder, item, n);
        self.validity.extend_constant(n, true)
    }

    pub fn push_null(&mut self) {
        T::push_default(&mut self.builder);
        self.validity.push(false);
    }

    pub fn append_column(&mut self, other: &NullableColumn<T>) {
        T::append_column(&mut self.builder, &other.column);
        self.validity.extend_from_bitmap(&other.validity)
    }

    pub fn build(self) -> NullableColumn<T> {
        assert_eq!(self.validity.len(), T::builder_len(&self.builder));
        NullableColumn::new(T::build_column(self.builder), self.validity.into())
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

    pub fn upcast(self, decimal_size: Option<DecimalSize>) -> NullableColumnBuilder<AnyType> {
        NullableColumnBuilder {
            builder: T::try_upcast_column_builder(self.builder, decimal_size).unwrap(),
            validity: self.validity,
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
pub struct NullableDomain<T: ValueType> {
    pub has_null: bool,
    // `None` means all rows are `NULL`s.
    //
    // Invariant: `has_null` must be `true` when `value` is `None`.
    pub value: Option<Box<T::Domain>>,
}
