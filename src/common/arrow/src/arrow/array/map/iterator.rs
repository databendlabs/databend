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

use super::MapArray;
use crate::arrow::array::Array;
use crate::arrow::bitmap::utils::BitmapIter;
use crate::arrow::bitmap::utils::ZipValidity;
use crate::arrow::trusted_len::TrustedLen;

/// Iterator of values of an [`ListArray`].
#[derive(Clone, Debug)]
pub struct MapValuesIter<'a> {
    array: &'a MapArray,
    index: usize,
    end: usize,
}

impl<'a> MapValuesIter<'a> {
    #[inline]
    pub fn new(array: &'a MapArray) -> Self {
        Self {
            array,
            index: 0,
            end: array.len(),
        }
    }
}

impl<'a> Iterator for MapValuesIter<'a> {
    type Item = Box<dyn Array>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            return None;
        }
        let old = self.index;
        self.index += 1;
        // Safety:
        // self.end is maximized by the length of the array
        Some(unsafe { self.array.value_unchecked(old) })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.end - self.index, Some(self.end - self.index))
    }
}

unsafe impl<'a> TrustedLen for MapValuesIter<'a> {}

impl<'a> DoubleEndedIterator for MapValuesIter<'a> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            None
        } else {
            self.end -= 1;
            // Safety:
            // self.end is maximized by the length of the array
            Some(unsafe { self.array.value_unchecked(self.end) })
        }
    }
}

impl<'a> IntoIterator for &'a MapArray {
    type Item = Option<Box<dyn Array>>;
    type IntoIter = ZipValidity<Box<dyn Array>, MapValuesIter<'a>, BitmapIter<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> MapArray {
    /// Returns an iterator of `Option<Box<dyn Array>>`
    pub fn iter(&'a self) -> ZipValidity<Box<dyn Array>, MapValuesIter<'a>, BitmapIter<'a>> {
        ZipValidity::new_with_validity(MapValuesIter::new(self), self.validity())
    }

    /// Returns an iterator of `Box<dyn Array>`
    pub fn values_iter(&'a self) -> MapValuesIter<'a> {
        MapValuesIter::new(self)
    }
}
