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

use super::DictionaryKey;
use crate::arrow::array::Array;
use crate::arrow::array::PrimitiveArray;
use crate::arrow::array::Utf8Array;
use crate::arrow::error::Error;
use crate::arrow::error::Result;
use crate::arrow::trusted_len::TrustedLen;
use crate::arrow::types::Offset;

pub trait DictValue {
    type IterValue<'this>
    where Self: 'this;

    /// # Safety
    /// Will not do any bound checks but must check validity.
    unsafe fn get_unchecked(&self, item: usize) -> Self::IterValue<'_>;

    /// Take a [`dyn Array`] an try to downcast it to the type of `DictValue`.
    fn downcast_values(array: &dyn Array) -> Result<&Self>
    where Self: Sized;
}

impl<O: Offset> DictValue for Utf8Array<O> {
    type IterValue<'a> = &'a str;

    unsafe fn get_unchecked(&self, item: usize) -> Self::IterValue<'_> {
        self.value_unchecked(item)
    }

    fn downcast_values(array: &dyn Array) -> Result<&Self>
    where Self: Sized {
        array
            .as_any()
            .downcast_ref::<Self>()
            .ok_or_else(|| {
                Error::InvalidArgumentError("could not convert array to dictionary value".into())
            })
            .inspect(|arr| {
                assert_eq!(
                    arr.null_count(),
                    0,
                    "null values in values not supported in iteration"
                );
            })
    }
}

/// Iterator of values of an `ListArray`.
pub struct DictionaryValuesIterTyped<'a, K: DictionaryKey, V: DictValue> {
    keys: &'a PrimitiveArray<K>,
    values: &'a V,
    index: usize,
    end: usize,
}

impl<'a, K: DictionaryKey, V: DictValue> DictionaryValuesIterTyped<'a, K, V> {
    pub(super) unsafe fn new(keys: &'a PrimitiveArray<K>, values: &'a V) -> Self {
        Self {
            keys,
            values,
            index: 0,
            end: keys.len(),
        }
    }
}

impl<'a, K: DictionaryKey, V: DictValue> Iterator for DictionaryValuesIterTyped<'a, K, V> {
    type Item = V::IterValue<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            return None;
        }
        let old = self.index;
        self.index += 1;
        unsafe {
            let key = self.keys.value_unchecked(old);
            let idx = key.as_usize();
            Some(self.values.get_unchecked(idx))
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.end - self.index, Some(self.end - self.index))
    }
}

unsafe impl<'a, K: DictionaryKey, V: DictValue> TrustedLen for DictionaryValuesIterTyped<'a, K, V> {}

impl<'a, K: DictionaryKey, V: DictValue> DoubleEndedIterator
    for DictionaryValuesIterTyped<'a, K, V>
{
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            None
        } else {
            self.end -= 1;
            unsafe {
                let key = self.keys.value_unchecked(self.end);
                let idx = key.as_usize();
                Some(self.values.get_unchecked(idx))
            }
        }
    }
}
