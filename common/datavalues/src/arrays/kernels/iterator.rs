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

use common_arrow::arrow::array::*;
use common_arrow::arrow::bitmap::utils::BitmapIter;
use common_arrow::arrow::bitmap::utils::ZipValidity;
use common_arrow::arrow::trusted_len::TrustedLen;

use crate::prelude::*;
use crate::series::Series;

impl<'a, T: DFPrimitiveType> IntoIterator for &'a DFPrimitiveArray<T> {
    type Item = Option<&'a T>;
    type IntoIter = ZipValidity<'a, &'a T, std::slice::Iter<'a, T>>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, T: DFPrimitiveType> DFPrimitiveArray<T> {
    /// constructs a new iterator
    #[inline]
    pub fn iter(&'a self) -> ZipValidity<'a, &'a T, std::slice::Iter<'a, T>> {
        self.array.iter()
    }

    pub fn into_no_null_iter(
        &'a self,
    ) -> impl TrustedLen<Item = &'a T> + Send + Sync + ExactSizeIterator {
        // .copied was significantly slower in benchmark, next call did not inline?
        self.array.values().iter()
    }
}

/// The no null iterator for a BooleanArray
pub struct BoolIterNoNull<'a> {
    array: &'a BooleanArray,
    current: usize,
    current_end: usize,
}
unsafe impl<'a> TrustedLen for BoolIterNoNull<'a> {}

impl<'a> IntoIterator for &'a DFBooleanArray {
    type Item = Option<bool>;
    type IntoIter = ZipValidity<'a, bool, BitmapIter<'a>>;
    fn into_iter(self) -> Self::IntoIter {
        self.array.iter()
    }
}

impl<'a> BoolIterNoNull<'a> {
    /// create a new iterator
    pub fn new(array: &'a BooleanArray) -> Self {
        BoolIterNoNull {
            array,
            current: 0,
            current_end: array.len(),
        }
    }
}

impl<'a> Iterator for BoolIterNoNull<'a> {
    type Item = bool;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.current_end {
            None
        } else {
            let old = self.current;
            self.current += 1;
            unsafe { Some(self.array.value_unchecked(old)) }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.array.len() - self.current,
            Some(self.array.len() - self.current),
        )
    }
}

impl DFBooleanArray {
    pub fn into_no_null_iter(&self) -> impl TrustedLen<Item = bool> + '_ + Send + Sync {
        BoolIterNoNull::new(self.get_inner())
    }
}

impl<'a> IntoIterator for &'a DFUtf8Array {
    type Item = Option<&'a str>;
    type IntoIter = ZipValidity<'a, &'a str, Utf8ValuesIter<'a, i64>>;
    fn into_iter(self) -> Self::IntoIter {
        self.array.iter()
    }
}

/// all arrays have known size.
impl<'a> ExactSizeIterator for Utf8IterNoNull<'a> {}
unsafe impl<'a> TrustedLen for Utf8IterNoNull<'a> {}

pub struct Utf8IterNoNull<'a> {
    array: &'a LargeUtf8Array,
    current: usize,
    current_end: usize,
}

impl<'a> Utf8IterNoNull<'a> {
    /// create a new iterator
    pub fn new(array: &'a LargeUtf8Array) -> Self {
        Utf8IterNoNull {
            array,
            current: 0,
            current_end: array.len(),
        }
    }
}

impl<'a> Iterator for Utf8IterNoNull<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.current_end {
            None
        } else {
            let old = self.current;
            self.current += 1;
            unsafe { Some(self.array.value_unchecked(old)) }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.array.len() - self.current,
            Some(self.array.len() - self.current),
        )
    }
}

impl DFUtf8Array {
    pub fn into_no_null_iter<'a>(&'a self) -> impl TrustedLen<Item = &'a str> + '_ + Send + Sync {
        Utf8IterNoNull::new(self.get_inner())
    }
}

pub struct ListIter<'a> {
    array: &'a LargeListArray,
    current: usize,
    current_end: usize,
}

impl<'a> ListIter<'a> {
    /// create a new iterator
    pub fn new(array: &'a LargeListArray) -> Self {
        Self {
            array,
            current: 0,
            current_end: array.len(),
        }
    }
}

impl<'a> Iterator for ListIter<'a> {
    type Item = Option<Series>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.current_end {
            None
        } else {
            let old = self.current;
            self.current += 1;
            match self.array.is_null(old) {
                true => Some(None),
                false => {
                    let array: ArrayRef = Arc::from(unsafe { self.array.value_unchecked(old) });
                    Some(Some(array.into_series()))
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.array.len() - self.current,
            Some(self.array.len() - self.current),
        )
    }
}

pub struct ListIterNoNull<'a> {
    array: &'a LargeListArray,
    current: usize,
    current_end: usize,
}

impl<'a> ListIterNoNull<'a> {
    /// create a new iterator
    pub fn new(array: &'a LargeListArray) -> Self {
        ListIterNoNull {
            array,
            current: 0,
            current_end: array.len(),
        }
    }
}

impl<'a> Iterator for ListIterNoNull<'a> {
    type Item = Series;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.current_end {
            None
        } else {
            let old = self.current;
            self.current += 1;
            let array: ArrayRef = Arc::from(unsafe { self.array.value_unchecked(old) });
            Some(array.into_series())
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.array.len() - self.current,
            Some(self.array.len() - self.current),
        )
    }
}

/// all arrays have known size.
impl<'a> ExactSizeIterator for ListIter<'a> {}
unsafe impl<'a> TrustedLen for ListIter<'a> {}

impl<'a> ExactSizeIterator for ListIterNoNull<'a> {}
unsafe impl<'a> TrustedLen for ListIterNoNull<'a> {}

impl<'a> IntoIterator for &'a DFListArray {
    type Item = Option<Series>;
    type IntoIter = ListIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        ListIter::new(&self.array)
    }
}

impl DFListArray {
    pub fn into_no_null_iter(&self) -> impl TrustedLen<Item = Series> + '_ + Send + Sync {
        ListIterNoNull::new(self.get_inner())
    }
}
/// Trait for DFArrays that don't have null values.
/// The result is the most efficient implementation `Iterator`, according to the number of chunks.
pub trait IntoNoNullIterator {
    type Item;
    type IntoIter: Iterator<Item = Self::Item>;

    fn into_no_null_iter(self) -> Self::IntoIter;
}

/// Wrapper struct to convert an iterator of type `T` into one of type `Option<T>`.  It is useful to make the
/// `IntoIterator` trait, in which every iterator shall return an `Option<T>`.
pub struct SomeIterator<I>(I)
where I: Iterator;

impl<I> Iterator for SomeIterator<I>
where I: Iterator
{
    type Item = Option<I::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(Some)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}
