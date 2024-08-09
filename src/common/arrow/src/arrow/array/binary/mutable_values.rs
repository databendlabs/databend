// Copyright 2020-2022 Jorge C. Leitão
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

use std::iter::FromIterator;
use std::sync::Arc;

use super::BinaryArray;
use super::MutableBinaryArray;
use crate::arrow::array::physical_binary::*;
use crate::arrow::array::specification::try_check_offsets_bounds;
use crate::arrow::array::Array;
use crate::arrow::array::ArrayAccessor;
use crate::arrow::array::ArrayValuesIter;
use crate::arrow::array::MutableArray;
use crate::arrow::array::TryExtend;
use crate::arrow::array::TryExtendFromSelf;
use crate::arrow::array::TryPush;
use crate::arrow::bitmap::MutableBitmap;
use crate::arrow::datatypes::DataType;
use crate::arrow::error::Error;
use crate::arrow::error::Result;
use crate::arrow::offset::Offset;
use crate::arrow::offset::Offsets;
use crate::arrow::trusted_len::TrustedLen;

/// A [`MutableArray`] that builds a [`BinaryArray`]. It differs
/// from [`MutableBinaryArray`] in that it builds non-null [`BinaryArray`].
#[derive(Debug, Clone)]
pub struct MutableBinaryValuesArray<O: Offset> {
    data_type: DataType,
    offsets: Offsets<O>,
    values: Vec<u8>,
}

impl<O: Offset> From<MutableBinaryValuesArray<O>> for BinaryArray<O> {
    fn from(other: MutableBinaryValuesArray<O>) -> Self {
        BinaryArray::<O>::new(
            other.data_type,
            other.offsets.into(),
            other.values.into(),
            None,
        )
    }
}

impl<O: Offset> From<MutableBinaryValuesArray<O>> for MutableBinaryArray<O> {
    fn from(other: MutableBinaryValuesArray<O>) -> Self {
        MutableBinaryArray::<O>::try_new(other.data_type, other.offsets, other.values, None)
            .expect("MutableBinaryValuesArray is consistent with MutableBinaryArray")
    }
}

impl<O: Offset> Default for MutableBinaryValuesArray<O> {
    fn default() -> Self {
        Self::new()
    }
}

impl<O: Offset> MutableBinaryValuesArray<O> {
    /// Returns an empty [`MutableBinaryValuesArray`].
    pub fn new() -> Self {
        Self {
            data_type: Self::default_data_type(),
            offsets: Offsets::new(),
            values: Vec::<u8>::new(),
        }
    }

    /// Returns a [`MutableBinaryValuesArray`] created from its internal representation.
    ///
    /// # Errors
    /// This function returns an error iff:
    /// * The last offset is not equal to the values' length.
    /// * The `data_type`'s [`crate::arrow::datatypes::PhysicalType`] is not equal to either `Binary` or `LargeBinary`.
    /// # Implementation
    /// This function is `O(1)`
    pub fn try_new(data_type: DataType, offsets: Offsets<O>, values: Vec<u8>) -> Result<Self> {
        try_check_offsets_bounds(&offsets, values.len())?;

        if data_type.to_physical_type() != Self::default_data_type().to_physical_type() {
            return Err(Error::oos(
                "MutableBinaryValuesArray can only be initialized with DataType::Binary or DataType::LargeBinary",
            ));
        }

        Ok(Self {
            data_type,
            offsets,
            values,
        })
    }

    /// Returns the default [`DataType`] of this container: [`DataType::Utf8`] or [`DataType::LargeUtf8`]
    /// depending on the generic [`Offset`].
    pub fn default_data_type() -> DataType {
        BinaryArray::<O>::default_data_type()
    }

    /// Initializes a new [`MutableBinaryValuesArray`] with a pre-allocated capacity of items.
    pub fn with_capacity(capacity: usize) -> Self {
        Self::with_capacities(capacity, 0)
    }

    /// Initializes a new [`MutableBinaryValuesArray`] with a pre-allocated capacity of items and values.
    pub fn with_capacities(capacity: usize, values: usize) -> Self {
        Self {
            data_type: Self::default_data_type(),
            offsets: Offsets::<O>::with_capacity(capacity),
            values: Vec::<u8>::with_capacity(values),
        }
    }

    /// returns its values.
    #[inline]
    pub fn values(&self) -> &Vec<u8> {
        &self.values
    }

    /// returns its offsets.
    #[inline]
    pub fn offsets(&self) -> &Offsets<O> {
        &self.offsets
    }

    /// Reserves `additional` elements and `additional_values` on the values.
    #[inline]
    pub fn reserve(&mut self, additional: usize, additional_values: usize) {
        self.offsets.reserve(additional);
        self.values.reserve(additional_values);
    }

    /// Returns the capacity in number of items
    pub fn capacity(&self) -> usize {
        self.offsets.capacity()
    }

    /// Returns the length of this array
    #[inline]
    pub fn len(&self) -> usize {
        self.offsets.len_proxy()
    }

    /// Returns `true` if the array has a length of 0.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Pushes a new item to the array.
    /// # Panic
    /// This operation panics iff the length of all values (in bytes) exceeds `O` maximum value.
    #[inline]
    pub fn push<T: AsRef<[u8]>>(&mut self, value: T) {
        self.try_push(value).unwrap()
    }

    /// Pop the last entry from [`MutableBinaryValuesArray`].
    /// This function returns `None` iff this array is empty.
    pub fn pop(&mut self) -> Option<Vec<u8>> {
        if self.is_empty() {
            return None;
        }
        self.offsets.pop()?;
        let start = self.offsets.last().to_usize();
        let value = self.values.split_off(start);
        Some(value.to_vec())
    }

    /// Returns the value of the element at index `i`.
    /// # Panic
    /// This function panics iff `i >= self.len`.
    #[inline]
    pub fn value(&self, i: usize) -> &[u8] {
        assert!(i < self.len());
        unsafe { self.value_unchecked(i) }
    }

    /// Returns the value of the element at index `i`.
    /// # Safety
    /// This function is safe iff `i < self.len`.
    #[inline]
    pub unsafe fn value_unchecked(&self, i: usize) -> &[u8] {
        // soundness: the invariant of the function
        let (start, end) = self.offsets.start_end(i);

        // soundness: the invariant of the struct
        self.values.get_unchecked(start..end)
    }

    /// Returns an iterator of `&[u8]`
    pub fn iter(&self) -> ArrayValuesIter<Self> {
        ArrayValuesIter::new(self)
    }

    /// Shrinks the capacity of the [`MutableBinaryValuesArray`] to fit its current length.
    pub fn shrink_to_fit(&mut self) {
        self.values.shrink_to_fit();
        self.offsets.shrink_to_fit();
    }

    /// Extract the low-end APIs from the [`MutableBinaryValuesArray`].
    pub fn into_inner(self) -> (DataType, Offsets<O>, Vec<u8>) {
        (self.data_type, self.offsets, self.values)
    }
}

impl<O: Offset> MutableArray for MutableBinaryValuesArray<O> {
    fn len(&self) -> usize {
        self.len()
    }

    fn validity(&self) -> Option<&MutableBitmap> {
        None
    }

    fn as_box(&mut self) -> Box<dyn Array> {
        let (data_type, offsets, values) = std::mem::take(self).into_inner();
        BinaryArray::new(data_type, offsets.into(), values.into(), None).boxed()
    }

    fn as_arc(&mut self) -> Arc<dyn Array> {
        let (data_type, offsets, values) = std::mem::take(self).into_inner();
        BinaryArray::new(data_type, offsets.into(), values.into(), None).arced()
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }

    #[inline]
    fn push_null(&mut self) {
        self.push::<&[u8]>(b"")
    }

    fn reserve(&mut self, additional: usize) {
        self.reserve(additional, 0)
    }

    fn shrink_to_fit(&mut self) {
        self.shrink_to_fit()
    }
}

impl<O: Offset, P: AsRef<[u8]>> FromIterator<P> for MutableBinaryValuesArray<O> {
    fn from_iter<I: IntoIterator<Item = P>>(iter: I) -> Self {
        let (offsets, values) = values_iter(iter.into_iter());
        Self::try_new(Self::default_data_type(), offsets, values).unwrap()
    }
}

impl<O: Offset> MutableBinaryValuesArray<O> {
    pub(crate) unsafe fn extend_from_trusted_len_iter<I, P>(
        &mut self,
        validity: &mut MutableBitmap,
        iterator: I,
    ) where
        P: AsRef<[u8]>,
        I: Iterator<Item = Option<P>>,
    {
        extend_from_trusted_len_iter(&mut self.offsets, &mut self.values, validity, iterator);
    }

    /// Extends the [`MutableBinaryValuesArray`] from a [`TrustedLen`]
    #[inline]
    pub fn extend_trusted_len<I, P>(&mut self, iterator: I)
    where
        P: AsRef<[u8]>,
        I: TrustedLen<Item = P>,
    {
        unsafe { self.extend_trusted_len_unchecked(iterator) }
    }

    /// Extends [`MutableBinaryValuesArray`] from an iterator of trusted len.
    /// # Safety
    /// The iterator must be trusted len.
    #[inline]
    pub unsafe fn extend_trusted_len_unchecked<I, P>(&mut self, iterator: I)
    where
        P: AsRef<[u8]>,
        I: Iterator<Item = P>,
    {
        extend_from_trusted_len_values_iter(&mut self.offsets, &mut self.values, iterator);
    }

    /// Creates a [`MutableBinaryValuesArray`] from a [`TrustedLen`]
    #[inline]
    pub fn from_trusted_len_iter<I, P>(iterator: I) -> Self
    where
        P: AsRef<[u8]>,
        I: TrustedLen<Item = P>,
    {
        // soundness: I is `TrustedLen`
        unsafe { Self::from_trusted_len_iter_unchecked(iterator) }
    }

    /// Returns a new [`MutableBinaryValuesArray`] from an iterator of trusted length.
    /// # Safety
    /// The iterator must be [`TrustedLen`](https://doc.rust-lang.org/std/iter/trait.TrustedLen.html).
    /// I.e. that `size_hint().1` correctly reports its length.
    #[inline]
    pub unsafe fn from_trusted_len_iter_unchecked<I, P>(iterator: I) -> Self
    where
        P: AsRef<[u8]>,
        I: Iterator<Item = P>,
    {
        let (offsets, values) = trusted_len_values_iter(iterator);
        Self::try_new(Self::default_data_type(), offsets, values).unwrap()
    }

    /// Returns a new [`MutableBinaryValuesArray`] from an iterator.
    /// # Error
    /// This operation errors iff the total length in bytes on the iterator exceeds `O`'s maximum value.
    /// (`i32::MAX` or `i64::MAX` respectively).
    pub fn try_from_iter<P: AsRef<[u8]>, I: IntoIterator<Item = P>>(iter: I) -> Result<Self> {
        let iterator = iter.into_iter();
        let (lower, _) = iterator.size_hint();
        let mut array = Self::with_capacity(lower);
        for item in iterator {
            array.try_push(item)?;
        }
        Ok(array)
    }

    /// Extend with a fallible iterator
    pub fn extend_fallible<T, I, E>(&mut self, iter: I) -> std::result::Result<(), E>
    where
        E: std::error::Error,
        I: IntoIterator<Item = std::result::Result<T, E>>,
        T: AsRef<[u8]>,
    {
        let mut iter = iter.into_iter();
        self.reserve(iter.size_hint().0, 0);
        iter.try_for_each(|x| {
            self.push(x?);
            Ok(())
        })
    }
}

impl<O: Offset, T: AsRef<[u8]>> Extend<T> for MutableBinaryValuesArray<O> {
    fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        extend_from_values_iter(&mut self.offsets, &mut self.values, iter.into_iter());
    }
}

impl<O: Offset, T: AsRef<[u8]>> TryExtend<T> for MutableBinaryValuesArray<O> {
    fn try_extend<I: IntoIterator<Item = T>>(&mut self, iter: I) -> Result<()> {
        let mut iter = iter.into_iter();
        self.reserve(iter.size_hint().0, 0);
        iter.try_for_each(|x| self.try_push(x))
    }
}

impl<O: Offset, T: AsRef<[u8]>> TryPush<T> for MutableBinaryValuesArray<O> {
    #[inline]
    fn try_push(&mut self, value: T) -> Result<()> {
        let bytes = value.as_ref();
        self.values.extend_from_slice(bytes);
        self.offsets.try_push_usize(bytes.len())
    }
}

unsafe impl<'a, O: Offset> ArrayAccessor<'a> for MutableBinaryValuesArray<O> {
    type Item = &'a [u8];

    #[inline]
    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        self.value_unchecked(index)
    }

    #[inline]
    fn len(&self) -> usize {
        self.len()
    }
}

impl<O: Offset> TryExtendFromSelf for MutableBinaryValuesArray<O> {
    fn try_extend_from_self(&mut self, other: &Self) -> Result<()> {
        self.values.extend_from_slice(&other.values);
        self.offsets.try_extend_from_self(&other.offsets)
    }
}