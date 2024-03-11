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

use crate::arrow::bitmap::Bitmap;
use crate::arrow::bitmap::TrueIdxIter;
use crate::arrow::trusted_len::TrustedLen;

mod private {
    pub trait Sealed {}

    impl<'a, T: super::ArrayAccessor<'a>> Sealed for T {}
}

/// Sealed trait representing assess to a value of an array.
/// # Safety
/// Implementers of this trait guarantee that
/// `value_unchecked` is safe when called up to `len`
pub unsafe trait ArrayAccessor<'a>: private::Sealed {
    type Item: 'a;
    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item;
    fn len(&self) -> usize;
}

/// Iterator of values of an [`ArrayAccessor`].
#[derive(Debug, Clone)]
pub struct ArrayValuesIter<'a, A: ArrayAccessor<'a>> {
    array: &'a A,
    index: usize,
    end: usize,
}

impl<'a, A: ArrayAccessor<'a>> ArrayValuesIter<'a, A> {
    /// Creates a new [`ArrayValuesIter`]
    #[inline]
    pub fn new(array: &'a A) -> Self {
        Self {
            array,
            index: 0,
            end: array.len(),
        }
    }
}

impl<'a, A: ArrayAccessor<'a>> Iterator for ArrayValuesIter<'a, A> {
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

impl<'a, A: ArrayAccessor<'a>> DoubleEndedIterator for ArrayValuesIter<'a, A> {
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

unsafe impl<'a, A: ArrayAccessor<'a>> TrustedLen for ArrayValuesIter<'a, A> {}
impl<'a, A: ArrayAccessor<'a>> ExactSizeIterator for ArrayValuesIter<'a, A> {}

pub struct NonNullValuesIter<'a, A: ?Sized> {
    accessor: &'a A,
    idxs: TrueIdxIter<'a>,
}

impl<'a, A: ArrayAccessor<'a> + ?Sized> NonNullValuesIter<'a, A> {
    pub fn new(accessor: &'a A, validity: Option<&'a Bitmap>) -> Self {
        Self {
            idxs: TrueIdxIter::new(accessor.len(), validity),
            accessor,
        }
    }
}

impl<'a, A: ArrayAccessor<'a> + ?Sized> Iterator for NonNullValuesIter<'a, A> {
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

unsafe impl<'a, A: ArrayAccessor<'a> + ?Sized> TrustedLen for NonNullValuesIter<'a, A> {}
