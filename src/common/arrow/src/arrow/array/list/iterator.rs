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

use super::ListArray;
use crate::arrow::array::Array;
use crate::arrow::array::ArrayAccessor;
use crate::arrow::array::ArrayValuesIter;
use crate::arrow::bitmap::utils::BitmapIter;
use crate::arrow::bitmap::utils::ZipValidity;
use crate::arrow::offset::Offset;

unsafe impl<'a, O: Offset> ArrayAccessor<'a> for ListArray<O> {
    type Item = Box<dyn Array>;

    #[inline]
    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        self.value_unchecked(index)
    }

    #[inline]
    fn len(&self) -> usize {
        self.len()
    }
}

/// Iterator of values of a [`ListArray`].
pub type ListValuesIter<'a, O> = ArrayValuesIter<'a, ListArray<O>>;

type ZipIter<'a, O> = ZipValidity<Box<dyn Array>, ListValuesIter<'a, O>, BitmapIter<'a>>;

impl<'a, O: Offset> IntoIterator for &'a ListArray<O> {
    type Item = Option<Box<dyn Array>>;
    type IntoIter = ZipIter<'a, O>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, O: Offset> ListArray<O> {
    /// Returns an iterator of `Option<Box<dyn Array>>`
    pub fn iter(&'a self) -> ZipIter<'a, O> {
        ZipValidity::new_with_validity(ListValuesIter::new(self), self.validity.as_ref())
    }

    /// Returns an iterator of `Box<dyn Array>`
    pub fn values_iter(&'a self) -> ListValuesIter<'a, O> {
        ListValuesIter::new(self)
    }
}

struct Iter<T, I: Iterator<Item = Option<T>>> {
    current: i32,
    offsets: std::vec::IntoIter<i32>,
    values: I,
}

impl<T, I: Iterator<Item = Option<T>> + Clone> Iterator for Iter<T, I> {
    type Item = Option<std::iter::Take<std::iter::Skip<I>>>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.offsets.next();
        next.map(|next| {
            let length = next - self.current;
            let iter = self
                .values
                .clone()
                .skip(self.current as usize)
                .take(length as usize);
            self.current = next;
            Some(iter)
        })
    }
}
