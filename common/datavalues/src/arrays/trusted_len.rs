// Copyright 2020 Datafuse Labs.
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

use std::borrow::Borrow;

use common_arrow::arrow::array::*;
use common_arrow::arrow::buffer::Buffer;
use common_arrow::arrow::trusted_len::TrustedLen;

use super::DFAsRef;
use crate::prelude::*;
use crate::utils::NoNull;

pub struct TrustMyLength<I: Iterator<Item = J>, J> {
    iter: I,
    len: usize,
}

impl<I, J> TrustMyLength<I, J>
where I: Iterator<Item = J>
{
    #[inline]
    pub fn new(iter: I, len: usize) -> Self {
        Self { iter, len }
    }
}

impl<I, J> Iterator for TrustMyLength<I, J>
where I: Iterator<Item = J>
{
    type Item = J;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len, Some(self.len))
    }
}

impl<I, J> ExactSizeIterator for TrustMyLength<I, J> where I: Iterator<Item = J> {}

impl<I, J> DoubleEndedIterator for TrustMyLength<I, J>
where I: Iterator<Item = J> + DoubleEndedIterator
{
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        self.iter.next_back()
    }
}

unsafe impl<I, J> TrustedLen for TrustMyLength<I, J> where I: Iterator<Item = J> {}

pub trait CustomIterTools: Iterator {
    fn fold_first_<F>(mut self, f: F) -> Option<Self::Item>
    where
        Self: Sized,
        F: FnMut(Self::Item, Self::Item) -> Self::Item,
    {
        let first = self.next()?;
        Some(self.fold(first, f))
    }

    fn trust_my_length(self, length: usize) -> TrustMyLength<Self, Self::Item>
    where Self: Sized {
        TrustMyLength::new(self, length)
    }

    fn collect_trusted<T: FromTrustedLenIterator<Self::Item>>(self) -> T
    where Self: Sized + TrustedLen {
        FromTrustedLenIterator::from_iter_trusted_length(self)
    }
}

pub trait CustomIterToolsSized: Iterator + Sized {}

impl<T: ?Sized> CustomIterTools for T where T: Iterator {}

pub trait FromTrustedLenIterator<A>: Sized {
    fn from_iter_trusted_length<T: IntoIterator<Item = A>>(iter: T) -> Self
    where T::IntoIter: TrustedLen;
}

impl<T> FromTrustedLenIterator<Option<T::Native>> for DataArray<T>
where T: DFPrimitiveType
{
    fn from_iter_trusted_length<I: IntoIterator<Item = Option<T::Native>>>(iter: I) -> Self {
        let iter = iter.into_iter();

        let arr = unsafe {
            PrimitiveArray::from_trusted_len_iter_unchecked(iter).to(T::data_type().to_arrow())
        };
        Self::from_arrow_array(arr)
    }
}

// NoNull is only a wrapper needed for specialization
impl<T> FromTrustedLenIterator<T::Native> for NoNull<DataArray<T>>
where T: DFPrimitiveType
{
    // We use AlignedVec because it is way faster than Arrows builder. We can do this because we
    // know we don't have null values.
    fn from_iter_trusted_length<I: IntoIterator<Item = T::Native>>(iter: I) -> Self {
        let iter = iter.into_iter();
        let values = unsafe { Buffer::from_trusted_len_iter_unchecked(iter) };
        let arr = PrimitiveArray::from_data(T::data_type().to_arrow(), values, None);

        NoNull::new(DataArray::<T>::from_arrow_array(arr))
    }
}
impl<Ptr> FromTrustedLenIterator<Ptr> for DFListArray
where Ptr: Borrow<Series>
{
    fn from_iter_trusted_length<I: IntoIterator<Item = Ptr>>(iter: I) -> Self {
        let iter = iter.into_iter();
        iter.collect()
    }
}

impl<Ptr> FromTrustedLenIterator<Option<Ptr>> for DFListArray
where Ptr: Borrow<Series>
{
    fn from_iter_trusted_length<I: IntoIterator<Item = Option<Ptr>>>(iter: I) -> Self {
        let iter = iter.into_iter();
        iter.collect()
    }
}

impl FromTrustedLenIterator<Option<bool>> for DFBooleanArray {
    fn from_iter_trusted_length<I: IntoIterator<Item = Option<bool>>>(iter: I) -> Self
    where I::IntoIter: TrustedLen {
        let iter = iter.into_iter();
        let arr = BooleanArray::from_trusted_len_iter(iter);
        Self::from_arrow_array(arr)
    }
}

impl FromTrustedLenIterator<bool> for DFBooleanArray {
    fn from_iter_trusted_length<I: IntoIterator<Item = bool>>(iter: I) -> Self
    where I::IntoIter: TrustedLen {
        let iter = iter.into_iter();
        let arr = BooleanArray::from_trusted_len_values_iter(iter);
        Self::from_arrow_array(arr)
    }
}

impl FromTrustedLenIterator<bool> for NoNull<DFBooleanArray> {
    fn from_iter_trusted_length<I: IntoIterator<Item = bool>>(iter: I) -> Self {
        let iter = iter.into_iter();
        iter.collect()
    }
}
impl<Ptr> FromTrustedLenIterator<Ptr> for DFUtf8Array
where Ptr: DFAsRef<str>
{
    fn from_iter_trusted_length<I: IntoIterator<Item = Ptr>>(iter: I) -> Self {
        let iter = iter.into_iter();
        iter.collect()
    }
}
