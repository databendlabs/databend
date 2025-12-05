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
use std::iter::once;
use std::iter::TrustedLen;
use std::marker::PhantomData;
use std::ops::Range;

use databend_common_column::buffer::Buffer;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use super::column_type_error;
use super::domain_type_error;
use super::scalar_type_error;
use super::AccessType;
use super::AnyType;
use super::ArgType;
use super::BuilderExt;
use super::Column;
use super::DataType;
use super::GenericMap;
use super::ReturnType;
use super::Scalar;
use super::ScalarRef;
use super::ValueType;
use crate::property::Domain;
use crate::ColumnBuilder;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArrayType<T>(PhantomData<T>);

impl<T: AccessType> AccessType for ArrayType<T> {
    type Scalar = T::Column;
    type ScalarRef<'a> = T::Column;
    type Column = ArrayColumn<T>;
    type Domain = Option<T::Domain>;
    type ColumnIterator<'a> = ArrayIterator<'a, T>;

    fn to_owned_scalar(scalar: Self::ScalarRef<'_>) -> Self::Scalar {
        scalar
    }

    fn to_scalar_ref(scalar: &Self::Scalar) -> Self::ScalarRef<'_> {
        scalar.clone()
    }

    fn try_downcast_scalar<'a>(scalar: &ScalarRef<'a>) -> Result<Self::ScalarRef<'a>> {
        match scalar {
            ScalarRef::Array(array) => T::try_downcast_column(array),
            _ => Err(scalar_type_error::<Self>(scalar)),
        }
    }

    fn try_downcast_column(col: &Column) -> Result<Self::Column> {
        let array = col
            .as_array()
            .ok_or_else(|| column_type_error::<Self>(col))?;
        ArrayColumn::try_downcast(array)
    }

    fn try_downcast_domain(domain: &Domain) -> Result<Self::Domain> {
        match domain {
            Domain::Array(Some(domain)) => Ok(Some(T::try_downcast_domain(domain)?)),
            Domain::Array(None) => Ok(None),
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
        <T as AccessType>::column_memory_size(scalar, false)
    }

    fn column_memory_size(col: &Self::Column, gc: bool) -> usize {
        col.memory_size(gc)
    }

    fn compare(a: T::Column, b: T::Column) -> Ordering {
        let ord = T::column_len(&a).cmp(&T::column_len(&b));
        if ord != Ordering::Equal {
            return ord;
        }

        for (l, r) in T::iter_column(&a).zip(T::iter_column(&b)) {
            let ord = T::compare(l, r);
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    }
}

impl<T: ValueType> ValueType for ArrayType<T> {
    type ColumnBuilder = ArrayColumnBuilder<T>;
    type ColumnBuilderMut<'a> = ArrayColumnBuilderMut<'a, T>;

    fn upcast_scalar_with_type(scalar: Self::Scalar, data_type: &DataType) -> Scalar {
        let values_type = data_type.as_array().unwrap();
        Scalar::Array(T::upcast_column_with_type(scalar, values_type))
    }

    fn upcast_domain_with_type(domain: Self::Domain, data_type: &DataType) -> Domain {
        Domain::Array(domain.map(|domain| {
            let values_type = data_type.as_array().unwrap();
            Box::new(T::upcast_domain_with_type(domain, values_type))
        }))
    }

    fn upcast_column_with_type(col: Self::Column, data_type: &DataType) -> Column {
        Column::Array(Box::new(col.upcast(data_type)))
    }

    fn downcast_builder(builder: &mut ColumnBuilder) -> Self::ColumnBuilderMut<'_> {
        let any_array = builder.as_array_mut().unwrap();
        ArrayColumnBuilderMut {
            builder: T::downcast_builder(&mut any_array.builder),
            offsets: &mut any_array.offsets,
        }
    }

    fn try_upcast_column_builder(
        builder: Self::ColumnBuilder,
        data_type: &DataType,
    ) -> Option<ColumnBuilder> {
        let values_type = data_type.as_array()?;
        Some(ColumnBuilder::Array(Box::new(builder.upcast(values_type))))
    }

    fn column_to_builder(col: Self::Column) -> Self::ColumnBuilder {
        ArrayColumnBuilder::from_column(col)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        builder.len()
    }

    fn builder_len_mut(builder: &Self::ColumnBuilderMut<'_>) -> usize {
        builder.offsets.len() - 1
    }

    fn push_item_mut(builder: &mut Self::ColumnBuilderMut<'_>, item: Self::ScalarRef<'_>) {
        builder.push(item);
    }

    fn push_item_repeat_mut(
        builder: &mut Self::ColumnBuilderMut<'_>,
        item: Self::ScalarRef<'_>,
        n: usize,
    ) {
        builder.push_repeat(&item, n);
    }

    fn push_default_mut(builder: &mut Self::ColumnBuilderMut<'_>) {
        let len = T::builder_len_mut(&builder.builder);
        builder.offsets.push(len as u64);
    }

    fn append_column_mut(builder: &mut Self::ColumnBuilderMut<'_>, other: &Self::Column) {
        builder.append_column(other);
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        builder.build()
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        builder.build_scalar()
    }
}

impl<T: ArgType> ArgType for ArrayType<T> {
    fn data_type() -> DataType {
        DataType::Array(Box::new(T::data_type()))
    }

    fn full_domain() -> Self::Domain {
        Some(T::full_domain())
    }
}

impl<T: ReturnType> ReturnType for ArrayType<T> {
    fn create_builder(capacity: usize, generics: &GenericMap) -> Self::ColumnBuilder {
        ArrayColumnBuilder::with_capacity(capacity, 0, generics)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ArrayColumn<T: AccessType> {
    values: T::Column,
    offsets: Buffer<u64>,
}

impl<T: AccessType> ArrayColumn<T> {
    pub fn new(values: T::Column, offsets: Buffer<u64>) -> ArrayColumn<T> {
        ArrayColumn { values, offsets }
    }

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
        // We need keep the last offsets in slice
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

    pub fn memory_size(&self, gc: bool) -> usize {
        T::column_memory_size(&self.underlying_column(), gc) + self.offsets.len() * 8
    }

    // Note: if the array column has been sliced, the number of values may not match the offsets.
    // Use the `underlying_column` function to get the actual values.
    pub fn values(&self) -> &T::Column {
        &self.values
    }

    pub fn underlying_column(&self) -> T::Column {
        debug_assert!(!self.offsets.is_empty());
        let range = *self.offsets.first().unwrap() as usize..*self.offsets.last().unwrap() as usize;
        T::slice_column(&self.values, range)
    }

    // Note: if the array column has been sliced, the offsets may not start at 0.
    // Use the `underlying_offsets` function when construct a new array column.
    pub fn offsets(&self) -> &Buffer<u64> {
        &self.offsets
    }

    pub fn underlying_offsets(&self) -> Buffer<u64> {
        debug_assert!(!self.offsets.is_empty());
        let start_offset = self.offsets.first().unwrap();
        if *start_offset > 0 {
            let sliced_offsets = self
                .offsets
                .iter()
                .map(|&offset| offset - start_offset)
                .collect();
            sliced_offsets
        } else {
            self.offsets.clone()
        }
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

impl<T: ValueType> ArrayColumn<T> {
    pub fn upcast(self, data_type: &DataType) -> ArrayColumn<AnyType> {
        let values_type = data_type.as_array().expect("must array type");
        ArrayColumn {
            values: T::upcast_column_with_type(self.values, values_type),
            offsets: self.offsets,
        }
    }
}

impl ArrayColumn<AnyType> {
    pub fn try_downcast<T: AccessType>(&self) -> Result<ArrayColumn<T>> {
        Ok(ArrayColumn {
            values: T::try_downcast_column(&self.values)?,
            offsets: self.offsets.clone(),
        })
    }
}

pub struct ArrayIterator<'a, T: AccessType> {
    values: &'a T::Column,
    offsets: std::slice::Windows<'a, u64>,
}

impl<T: AccessType> Iterator for ArrayIterator<'_, T> {
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

unsafe impl<T: AccessType> TrustedLen for ArrayIterator<'_, T> {}

#[derive(Debug, Clone, PartialEq)]
pub struct ArrayColumnBuilder<T: ValueType> {
    pub builder: T::ColumnBuilder,
    pub offsets: Vec<u64>,
}

#[derive(Debug)]
pub struct ArrayColumnBuilderMut<'a, T: ValueType> {
    pub(super) builder: T::ColumnBuilderMut<'a>,
    pub(super) offsets: &'a mut Vec<u64>,
}

impl<'a, T: ValueType> From<&'a mut ArrayColumnBuilder<T>> for ArrayColumnBuilderMut<'a, T> {
    fn from(builder: &'a mut ArrayColumnBuilder<T>) -> Self {
        ArrayColumnBuilderMut {
            builder: (&mut builder.builder).into(),
            offsets: &mut builder.offsets,
        }
    }
}

impl<T: ValueType> BuilderExt<ArrayType<T>> for ArrayColumnBuilderMut<'_, T> {
    fn len(&self) -> usize {
        ArrayType::<T>::builder_len_mut(self)
    }

    fn push_item(&mut self, item: <ArrayType<T> as AccessType>::ScalarRef<'_>) {
        ArrayType::<T>::push_item_mut(self, item)
    }

    fn push_repeat(&mut self, item: <ArrayType<T> as AccessType>::ScalarRef<'_>, n: usize) {
        ArrayType::<T>::push_item_repeat_mut(self, item, n);
    }

    fn push_default(&mut self) {
        ArrayType::<T>::push_default_mut(self);
    }

    fn append_column(&mut self, other: &<ArrayType<T> as AccessType>::Column) {
        ArrayType::<T>::append_column_mut(self, other);
    }
}

impl<T: ValueType> ArrayColumnBuilder<T> {
    pub fn as_mut(&mut self) -> ArrayColumnBuilderMut<'_, T> {
        self.into()
    }

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

    pub fn put_item(&mut self, item: T::ScalarRef<'_>) {
        T::push_item(&mut self.builder, item);
    }

    pub fn commit_row(&mut self) {
        self.offsets.push(T::builder_len(&self.builder) as u64);
    }

    pub fn push(&mut self, item: T::Column) {
        self.as_mut().push(item);
    }

    pub fn push_repeat(&mut self, item: &T::Column, n: usize) {
        self.as_mut().push_repeat(item, n);
    }

    pub fn push_default(&mut self) {
        self.as_mut().push_default();
    }

    pub fn append_column(&mut self, other: &ArrayColumn<T>) {
        self.as_mut().append_column(other);
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

    pub fn upcast(self, data_type: &DataType) -> ArrayColumnBuilder<AnyType> {
        ArrayColumnBuilder {
            builder: T::try_upcast_column_builder(self.builder, data_type).unwrap(),
            offsets: self.offsets,
        }
    }
}

impl<T: ValueType> ArrayColumnBuilderMut<'_, T> {
    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    pub fn push(&mut self, item: T::Column) {
        T::append_column_mut(&mut self.builder, &item);
        let len = T::builder_len_mut(&self.builder);
        self.offsets.push(len as u64);
    }

    pub fn push_repeat(&mut self, item: &T::Column, n: usize) {
        if n == 0 {
            return;
        }
        let before = T::builder_len_mut(&self.builder);
        T::append_column_mut(&mut self.builder, item);
        let len = T::builder_len_mut(&self.builder) - before;
        for _ in 1..n {
            T::append_column_mut(&mut self.builder, item);
        }
        self.offsets
            .extend((1..=n).map(|i| (before + len * i) as u64));
    }

    pub fn put_item(&mut self, item: T::ScalarRef<'_>) {
        T::push_item_mut(&mut self.builder, item);
    }

    pub fn commit_row(&mut self) {
        self.offsets.push(T::builder_len_mut(&self.builder) as u64);
    }

    pub fn push_default(&mut self) {
        let len = T::builder_len_mut(&self.builder);
        self.offsets.push(len as u64);
    }

    pub fn append_column(&mut self, other: &ArrayColumn<T>) {
        // the first offset of other column may not be zero
        let other_start = *other.offsets.first().unwrap() as usize;
        let other_end = *other.offsets.last().unwrap() as usize;
        let other_values = T::slice_column(&other.values, other_start..other_end);
        T::append_column_mut(&mut self.builder, &other_values);

        let end = self.offsets.last().cloned().unwrap();
        self.offsets.extend(
            other
                .offsets
                .iter()
                .skip(1)
                .map(|offset| offset + end - (other_start as u64)),
        );
    }
}

impl<T: ReturnType> ArrayColumnBuilder<T> {
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
