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

use std::sync::Arc;

use common_arrow::arrow::array::*;
use common_arrow::arrow::trusted_len::TrustedLen;

use crate::prelude::*;
use crate::series::Series;

/// A `DFIterator` is an iterator over a `DFArray` which contains DF types. A `DFIterator`
/// must implement   `DoubleEndedIterator`.
pub trait DFIterator: DoubleEndedIterator + Send + Sync {}
unsafe impl<'a, I> TrustedLen for Box<dyn DFIterator<Item = I> + 'a> {}

/// Implement DFIterator for every iterator that implements the needed traits.
impl<T: ?Sized> DFIterator for T where T: ExactSizeIterator + DoubleEndedIterator + Send + Sync + TrustedLen
{}

impl<'a, T> IntoIterator for &'a DataArray<T>
where T: DFNumericType
{
    type Item = Option<T::Native>;
    type IntoIter = Box<dyn DFIterator<Item = Self::Item> + 'a>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(
            self.downcast_iter()
                .map(|x| x.copied())
                .trust_my_length(self.len()),
        )
    }
}
/// The no null iterator for a BooleanArray
pub struct BoolIterNoNull<'a> {
    array: &'a BooleanArray,
    current: usize,
    current_end: usize,
}

impl<'a> IntoIterator for &'a DFBooleanArray {
    type Item = Option<bool>;
    type IntoIter = Box<dyn DFIterator<Item = Self::Item> + 'a>;
    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.downcast_iter().trust_my_length(self.len()))
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

impl<'a> DoubleEndedIterator for BoolIterNoNull<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.current_end == self.current {
            None
        } else {
            self.current_end -= 1;
            unsafe { Some(self.array.value_unchecked(self.current_end)) }
        }
    }
}

impl DFBooleanArray {
    pub fn into_no_null_iter(
        &self,
    ) -> impl Iterator<Item = bool> + '_ + Send + Sync + DoubleEndedIterator {
        BoolIterNoNull::new(self.downcast_ref())
    }
}

impl<'a> IntoIterator for &'a DFUtf8Array {
    type Item = Option<&'a str>;
    type IntoIter = Box<dyn DFIterator<Item = Self::Item> + 'a>;
    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.downcast_iter())
    }
}

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

impl<'a> DoubleEndedIterator for Utf8IterNoNull<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.current_end == self.current {
            None
        } else {
            self.current_end -= 1;
            unsafe { Some(self.array.value_unchecked(self.current_end)) }
        }
    }
}

/// all arrays have known size.
impl<'a> ExactSizeIterator for Utf8IterNoNull<'a> {}

impl DFUtf8Array {
    pub fn into_no_null_iter<'a>(
        &'a self,
    ) -> impl Iterator<Item = &'a str> + '_ + Send + Sync + DoubleEndedIterator {
        Utf8IterNoNull::new(self.downcast_ref())
    }
}

impl<'a> IntoIterator for &'a DFListArray {
    type Item = Option<Series>;
    type IntoIter = Box<dyn DFIterator<Item = Self::Item> + 'a>;
    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.downcast_iter().trust_my_length(self.len()))
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

impl<'a> DoubleEndedIterator for ListIterNoNull<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.current_end == self.current {
            None
        } else {
            self.current_end -= 1;

            let array: ArrayRef =
                Arc::from(unsafe { self.array.value_unchecked(self.current_end) });
            Some(array.into_series())
        }
    }
}

/// all arrays have known size.
impl<'a> ExactSizeIterator for ListIterNoNull<'a> {}

impl DFListArray {
    pub fn into_no_null_iter(
        &self,
    ) -> impl Iterator<Item = Series> + '_ + Send + Sync + DoubleEndedIterator {
        ListIterNoNull::new(self.downcast_ref())
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

impl<I> DoubleEndedIterator for SomeIterator<I>
where I: DoubleEndedIterator
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back().map(Some)
    }
}
