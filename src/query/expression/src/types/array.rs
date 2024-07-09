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

use std::iter::once;
use std::marker::PhantomData;
use std::ops::Range;

use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_arrow::arrow::trusted_len::TrustedLen;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use super::AnyType;
use super::DecimalSize;
use crate::property::Domain;
use crate::types::ArgType;
use crate::types::DataType;
use crate::types::GenericMap;
use crate::types::ValueType;
use crate::values::Column;
use crate::values::Scalar;
use crate::ColumnBuilder;
use crate::ScalarRef;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArrayType<T: ValueType>(PhantomData<T>);

impl<T: ValueType> ValueType for ArrayType<T> {
    type Scalar = T::Column;
    type ScalarRef<'a> = T::Column;
    type Column = ArrayColumn<T>;
    type Domain = Option<T::Domain>;
    type ColumnIterator<'a> = ArrayIterator<'a, T>;
    type ColumnBuilder = ArrayColumnBuilder<T>;

    #[inline]
    fn upcast_gat<'short, 'long: 'short>(long: T::Column) -> T::Column {
        long
    }

    fn to_owned_scalar(scalar: Self::ScalarRef<'_>) -> Self::Scalar {
        scalar
    }

    fn to_scalar_ref(scalar: &Self::Scalar) -> Self::ScalarRef<'_> {
        scalar.clone()
    }

    fn try_downcast_scalar<'a>(scalar: &'a ScalarRef) -> Option<Self::ScalarRef<'a>> {
        match scalar {
            ScalarRef::Array(array) => T::try_downcast_column(array),
            _ => None,
        }
    }

    fn try_downcast_column(col: &Column) -> Option<Self::Column> {
        ArrayColumn::try_downcast(col.as_array()?)
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        match domain {
            Domain::Array(Some(domain)) => Some(Some(T::try_downcast_domain(domain)?)),
            Domain::Array(None) => Some(None),
            _ => None,
        }
    }

    fn try_downcast_builder(_builder: &mut ColumnBuilder) -> Option<&mut Self::ColumnBuilder> {
        None
    }

    #[allow(clippy::manual_map)]
    fn try_downcast_owned_builder(builder: ColumnBuilder) -> Option<Self::ColumnBuilder> {
        match builder {
            ColumnBuilder::Array(inner) => {
                let builder = T::try_downcast_owned_builder(inner.builder);
                // ```
                // builder.map(|builder| ArrayColumnBuilder {
                //     builder,
                //     offsets: inner.offsets,
                // })
                // ```
                // If we using the clippy recommend way like above, the compiler will complain:
                // use of partially moved value: `inner`.
                // That's rust borrow checker error, if we using the new borrow checker named polonius,
                // everything goes fine, but polonius is very slow, so we allow manual map here.
                if let Some(builder) = builder {
                    Some(ArrayColumnBuilder {
                        builder,
                        offsets: inner.offsets,
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
        Some(ColumnBuilder::Array(Box::new(builder.upcast(decimal_size))))
    }

    fn upcast_scalar(scalar: Self::Scalar) -> Scalar {
        Scalar::Array(T::upcast_column(scalar))
    }

    fn upcast_column(col: Self::Column) -> Column {
        Column::Array(Box::new(col.upcast()))
    }

    fn upcast_domain(domain: Self::Domain) -> Domain {
        Domain::Array(domain.map(|domain| Box::new(T::upcast_domain(domain))))
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
        ArrayColumnBuilder::from_column(col)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        builder.len()
    }

    fn push_item(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>) {
        builder.push(item);
    }

    fn push_item_repeat(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>, n: usize) {
        builder.push_repeat(&item, n);
    }

    fn push_default(builder: &mut Self::ColumnBuilder) {
        builder.push_default();
    }

    fn append_column(builder: &mut Self::ColumnBuilder, other_builder: &Self::Column) {
        builder.append_column(other_builder);
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        builder.build()
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        builder.build_scalar()
    }

    fn scalar_memory_size(scalar: &Self::ScalarRef<'_>) -> usize {
        T::column_memory_size(scalar)
    }

    fn column_memory_size(col: &Self::Column) -> usize {
        col.memory_size()
    }
}

impl<T: ArgType> ArgType for ArrayType<T> {
    fn data_type() -> DataType {
        DataType::Array(Box::new(T::data_type()))
    }

    fn full_domain() -> Self::Domain {
        Some(T::full_domain())
    }

    fn create_builder(capacity: usize, generics: &GenericMap) -> Self::ColumnBuilder {
        ArrayColumnBuilder::with_capacity(capacity, 0, generics)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ArrayColumn<T: ValueType> {
    pub values: T::Column,
    pub offsets: Buffer<u64>,
}

impl<T: ValueType> ArrayColumn<T> {
    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    pub fn index(&self, index: usize) -> Option<T::Column> {
        Some(T::slice_column(
            &self.values,
            (self.offsets[index] as usize)..(self.offsets[index + 1] as usize),
        ))
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    pub unsafe fn index_unchecked(&self, index: usize) -> T::Column {
        T::slice_column(
            &self.values,
            (self.offsets[index] as usize)..(self.offsets[index + 1] as usize),
        )
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        let offsets = self
            .offsets
            .clone()
            .sliced(range.start, range.end - range.start + 1);
        ArrayColumn {
            values: self.values.clone(),
            offsets,
        }
    }

    pub fn iter(&self) -> ArrayIterator<T> {
        ArrayIterator {
            values: &self.values,
            offsets: self.offsets.windows(2),
        }
    }

    pub fn upcast(self) -> ArrayColumn<AnyType> {
        ArrayColumn {
            values: T::upcast_column(self.values),
            offsets: self.offsets,
        }
    }

    pub fn memory_size(&self) -> usize {
        T::column_memory_size(&self.values) + self.offsets.len() * 8
    }

    pub fn underlying_column(&self) -> T::Column {
        debug_assert!(!self.offsets.is_empty());
        let range = *self.offsets.first().unwrap() as usize..*self.offsets.last().unwrap() as usize;
        T::slice_column(&self.values, range)
    }

    pub fn check_valid(&self) -> Result<()> {
        let offsets = self.offsets.as_slice();
        let len = offsets.len();
        if len < 1 {
            return Err(ErrorCode::Internal(format!(
                "ArrayColumn offsets length must be equal or greater than 1, but got {}",
                len
            )));
        }

        for i in 1..len {
            if offsets[i] < offsets[i - 1] {
                return Err(ErrorCode::Internal(format!(
                    "ArrayColumn offsets value must be equal or greater than previous value, but got {}",
                    offsets[i]
                )));
            }
        }
        Ok(())
    }
}

impl ArrayColumn<AnyType> {
    pub fn try_downcast<T: ValueType>(&self) -> Option<ArrayColumn<T>> {
        Some(ArrayColumn {
            values: T::try_downcast_column(&self.values)?,
            offsets: self.offsets.clone(),
        })
    }
}

pub struct ArrayIterator<'a, T: ValueType> {
    values: &'a T::Column,
    offsets: std::slice::Windows<'a, u64>,
}

impl<'a, T: ValueType> Iterator for ArrayIterator<'a, T> {
    type Item = T::Column;

    fn next(&mut self) -> Option<Self::Item> {
        self.offsets
            .next()
            .map(|range| T::slice_column(self.values, (range[0] as usize)..(range[1] as usize)))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.offsets.size_hint()
    }
}

unsafe impl<'a, T: ValueType> TrustedLen for ArrayIterator<'a, T> {}

#[derive(Debug, Clone, PartialEq)]
pub struct ArrayColumnBuilder<T: ValueType> {
    pub builder: T::ColumnBuilder,
    pub offsets: Vec<u64>,
}

impl<T: ValueType> ArrayColumnBuilder<T> {
    pub fn from_column(col: ArrayColumn<T>) -> Self {
        ArrayColumnBuilder {
            builder: T::column_to_builder(col.values),
            offsets: col.offsets.to_vec(),
        }
    }

    pub fn repeat(array: &T::Column, n: usize) -> Self {
        let mut builder = T::column_to_builder(array.clone());
        let len = T::builder_len(&builder);
        for _ in 1..n {
            T::append_column(&mut builder, array)
        }
        let offsets = once(0)
            .chain((0..n).map(|i| (len * (i + 1)) as u64))
            .collect();
        ArrayColumnBuilder { builder, offsets }
    }

    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    pub fn reserve(&mut self, additional: usize) {
        self.offsets.reserve(additional);
    }

    pub fn put_item(&mut self, item: T::ScalarRef<'_>) {
        T::push_item(&mut self.builder, item);
    }

    pub fn commit_row(&mut self) {
        self.offsets.push(T::builder_len(&self.builder) as u64);
    }

    pub fn push(&mut self, item: T::Column) {
        T::append_column(&mut self.builder, &item);
        let len = T::builder_len(&self.builder);
        self.offsets.push(len as u64);
    }

    pub fn push_repeat(&mut self, item: &T::Column, n: usize) {
        if n == 0 {
            return;
        }
        let before = T::builder_len(&self.builder);
        T::append_column(&mut self.builder, item);
        let len = T::builder_len(&self.builder) - before;
        for _ in 1..n {
            T::append_column(&mut self.builder, item);
        }
        self.offsets.reserve(n);
        self.offsets
            .extend((1..=n).map(|i| (before + len * i) as u64));
    }

    pub fn push_default(&mut self) {
        let len = T::builder_len(&self.builder);
        self.offsets.push(len as u64);
    }

    pub fn append_column(&mut self, other: &ArrayColumn<T>) {
        // the first offset of other column may not be zero
        let other_start = *other.offsets.first().unwrap() as usize;
        let other_end = *other.offsets.last().unwrap() as usize;
        let other_values = T::slice_column(&other.values, other_start..other_end);
        T::append_column(&mut self.builder, &other_values);

        let end = self.offsets.last().cloned().unwrap();
        self.offsets.extend(
            other
                .offsets
                .iter()
                .skip(1)
                .map(|offset| offset + end - (other_start as u64)),
        );
    }

    pub fn build(self) -> ArrayColumn<T> {
        ArrayColumn {
            values: T::build_column(self.builder),
            offsets: self.offsets.into(),
        }
    }

    pub fn build_scalar(self) -> T::Column {
        assert_eq!(self.offsets.len(), 2);
        T::slice_column(
            &T::build_column(self.builder),
            (self.offsets[0] as usize)..(self.offsets[1] as usize),
        )
    }

    pub fn upcast(self, decimal_size: Option<DecimalSize>) -> ArrayColumnBuilder<AnyType> {
        ArrayColumnBuilder {
            builder: T::try_upcast_column_builder(self.builder, decimal_size).unwrap(),
            offsets: self.offsets,
        }
    }
}

impl<T: ArgType> ArrayColumnBuilder<T> {
    pub fn with_capacity(len: usize, values_capacity: usize, generics: &GenericMap) -> Self {
        let mut offsets = Vec::with_capacity(len + 1);
        offsets.push(0);
        ArrayColumnBuilder {
            builder: T::create_builder(values_capacity, generics),
            offsets,
        }
    }
}

impl ArrayColumnBuilder<AnyType> {
    pub fn pop(&mut self) -> Option<Column> {
        if self.len() > 0 {
            let pop_count = self.offsets[self.offsets.len() - 1] as usize
                - self.offsets[self.offsets.len() - 2] as usize;
            self.offsets.pop();
            let mut builder = ColumnBuilder::with_capacity(&self.builder.data_type(), pop_count);
            for _ in 0..pop_count {
                builder.push(self.builder.pop().unwrap().as_ref());
            }
            Some(builder.build())
        } else {
            None
        }
    }
}
