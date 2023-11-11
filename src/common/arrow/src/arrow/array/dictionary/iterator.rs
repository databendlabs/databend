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

use super::DictionaryArray;
use super::DictionaryKey;
use crate::arrow::bitmap::utils::BitmapIter;
use crate::arrow::bitmap::utils::ZipValidity;
use crate::arrow::scalar::Scalar;
use crate::arrow::trusted_len::TrustedLen;

/// Iterator of values of an `ListArray`.
pub struct DictionaryValuesIter<'a, K: DictionaryKey> {
    array: &'a DictionaryArray<K>,
    index: usize,
    end: usize,
}

impl<'a, K: DictionaryKey> DictionaryValuesIter<'a, K> {
    #[inline]
    pub fn new(array: &'a DictionaryArray<K>) -> Self {
        Self {
            array,
            index: 0,
            end: array.len(),
        }
    }
}

impl<'a, K: DictionaryKey> Iterator for DictionaryValuesIter<'a, K> {
    type Item = Box<dyn Scalar>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            return None;
        }
        let old = self.index;
        self.index += 1;
        Some(self.array.value(old))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.end - self.index, Some(self.end - self.index))
    }
}

unsafe impl<'a, K: DictionaryKey> TrustedLen for DictionaryValuesIter<'a, K> {}

impl<'a, K: DictionaryKey> DoubleEndedIterator for DictionaryValuesIter<'a, K> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            None
        } else {
            self.end -= 1;
            Some(self.array.value(self.end))
        }
    }
}

type ValuesIter<'a, K> = DictionaryValuesIter<'a, K>;
type ZipIter<'a, K> = ZipValidity<Box<dyn Scalar>, ValuesIter<'a, K>, BitmapIter<'a>>;

impl<'a, K: DictionaryKey> IntoIterator for &'a DictionaryArray<K> {
    type Item = Option<Box<dyn Scalar>>;
    type IntoIter = ZipIter<'a, K>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
