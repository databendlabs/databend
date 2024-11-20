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

use std::iter::TrustedLen;

use crate::bitmap::Bitmap;
use crate::bitmap::TrueIdxIter;

/// Sealed trait representing assess to a value of an array.
/// # Safety
/// Implementers of this trait guarantee that
/// `value_unchecked` is safe when called up to `len`
#[allow(clippy::missing_safety_doc)]
pub unsafe trait ColumnAccessor<'a> {
    type Item: 'a;
    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item;
    fn len(&self) -> usize;
}

/// Iterator of values of an [`ColumnAccessor`].
#[derive(Debug, Clone)]
pub struct ColumnValuesIter<'a, A: ColumnAccessor<'a>> {
    array: &'a A,
    index: usize,
    end: usize,
}

impl<'a, A: ColumnAccessor<'a>> ColumnValuesIter<'a, A> {
    /// Creates a new [`ColumnValuesIter`]
    #[inline]
    pub fn new(array: &'a A) -> Self {
        Self {
            array,
            index: 0,
            end: array.len(),
        }
    }
}

impl<'a, A: ColumnAccessor<'a>> Iterator for ColumnValuesIter<'a, A> {
    type Item = A::Item;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            return None;
        }
        let old = self.index;
        self.index += 1;
        Some(unsafe { self.array.value_unchecked(old) })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.end - self.index, Some(self.end - self.index))
    }

    #[inline]
    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        let new_index = self.index + n;
        if new_index > self.end {
            self.index = self.end;
            None
        } else {
            self.index = new_index;
            self.next()
        }
    }
}

impl<'a, A: ColumnAccessor<'a>> DoubleEndedIterator for ColumnValuesIter<'a, A> {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            None
        } else {
            self.end -= 1;
            Some(unsafe { self.array.value_unchecked(self.end) })
        }
    }
}

unsafe impl<'a, A: ColumnAccessor<'a>> TrustedLen for ColumnValuesIter<'a, A> {}
impl<'a, A: ColumnAccessor<'a>> ExactSizeIterator for ColumnValuesIter<'a, A> {}

pub struct NonNullValuesIter<'a, A: ?Sized> {
    accessor: &'a A,
    idxs: TrueIdxIter<'a>,
}

impl<'a, A: ColumnAccessor<'a> + ?Sized> NonNullValuesIter<'a, A> {
    pub fn new(accessor: &'a A, validity: Option<&'a Bitmap>) -> Self {
        Self {
            idxs: TrueIdxIter::new(accessor.len(), validity),
            accessor,
        }
    }
}

impl<'a, A: ColumnAccessor<'a> + ?Sized> Iterator for NonNullValuesIter<'a, A> {
    type Item = A::Item;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(i) = self.idxs.next() {
            return Some(unsafe { self.accessor.value_unchecked(i) });
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.idxs.size_hint()
    }
}

unsafe impl<'a, A: ColumnAccessor<'a> + ?Sized> TrustedLen for NonNullValuesIter<'a, A> {}
