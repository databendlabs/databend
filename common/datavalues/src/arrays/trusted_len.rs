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

pub trait CustomIterTools: Iterator {
    fn fold_first_<F>(mut self, f: F) -> Option<Self::Item>
    where
        Self: Sized,
        F: FnMut(Self::Item, Self::Item) -> Self::Item,
    {
        let first = self.next()?;
        Some(self.fold(first, f))
    }

    fn collect_trusted<T: FromTrustedLenIterator<Self::Item>>(self) -> T
    where Self: Sized + TrustedLen {
        FromTrustedLenIterator::from_iter_trusted_length(self)
    }
}

impl<T: ?Sized> CustomIterTools for T where T: Iterator {}

pub trait FromTrustedLenIterator<A>: Sized {
    fn from_iter_trusted_length<T: TrustedLen<Item = A>>(iter: T) -> Self;
}

impl<T> FromTrustedLenIterator<Option<T>> for DFPrimitiveArray<T>
where T: DFPrimitiveType
{
    fn from_iter_trusted_length<I: TrustedLen<Item = Option<T>>>(iter: I) -> Self {
        let arr = unsafe {
            PrimitiveArray::from_trusted_len_iter_unchecked(iter).to(T::data_type().to_arrow())
        };
        Self::new(arr)
    }
}

// NoNull is only a wrapper needed for specialization
impl<T> FromTrustedLenIterator<T> for NoNull<DFPrimitiveArray<T>>
where T: DFPrimitiveType
{
    // We use AlignedVec because it is way faster than Arrows builder. We can do this because we
    // know we don't have null values.
    fn from_iter_trusted_length<I: TrustedLen<Item = T>>(iter: I) -> Self {
        let values = unsafe { Buffer::from_trusted_len_iter_unchecked(iter) };
        let arr = PrimitiveArray::from_data(T::data_type().to_arrow(), values, None);

        NoNull::new(arr.into())
    }
}
impl<Ptr> FromTrustedLenIterator<Ptr> for DFListArray
where Ptr: Borrow<Series>
{
    fn from_iter_trusted_length<I: TrustedLen<Item = Ptr>>(iter: I) -> Self {
        iter.collect()
    }
}

impl<Ptr> FromTrustedLenIterator<Option<Ptr>> for DFListArray
where Ptr: Borrow<Series>
{
    fn from_iter_trusted_length<I: TrustedLen<Item = Option<Ptr>>>(iter: I) -> Self {
        iter.collect()
    }
}

impl FromTrustedLenIterator<Option<bool>> for DFBooleanArray {
    fn from_iter_trusted_length<I: TrustedLen<Item = Option<bool>>>(iter: I) -> Self {
        let arr = BooleanArray::from_trusted_len_iter(iter);
        Self::new(arr)
    }
}

impl FromTrustedLenIterator<bool> for DFBooleanArray {
    fn from_iter_trusted_length<I: TrustedLen<Item = bool>>(iter: I) -> Self {
        let arr = BooleanArray::from_trusted_len_values_iter(iter);
        Self::new(arr)
    }
}

impl FromTrustedLenIterator<bool> for NoNull<DFBooleanArray> {
    fn from_iter_trusted_length<I: TrustedLen<Item = bool>>(iter: I) -> Self {
        iter.collect()
    }
}
impl<Ptr> FromTrustedLenIterator<Ptr> for DFStringArray
where Ptr: DFAsRef<[u8]>
{
    fn from_iter_trusted_length<I: TrustedLen<Item = Ptr>>(iter: I) -> Self {
        iter.collect()
    }
}

impl<Ptr> FromTrustedLenIterator<Option<Ptr>> for DFStringArray
where Ptr: DFAsRef<[u8]>
{
    fn from_iter_trusted_length<I: TrustedLen<Item = Option<Ptr>>>(iter: I) -> Self {
        iter.collect()
    }
}
