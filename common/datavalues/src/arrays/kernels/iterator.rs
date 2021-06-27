use std::convert::TryFrom;
use std::ops::Deref;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::array::BooleanArray;
use common_arrow::arrow::array::LargeListArray;
use common_arrow::arrow::array::LargeStringArray;

use crate::series::IntoSeries;
use crate::series::Series;
use crate::series::SeriesFrom;
use crate::DFBooleanArray;
use crate::DFListArray;
use crate::DFStringArray;

/// A `DFIterator` is an iterator over a `DFArray` which contains DF types. A `DFIterator`
/// must implement   `DoubleEndedIterator`.
pub trait DFIterator: DoubleEndedIterator + Send + Sync {}
/// Implement DFIterator for every iterator that implements the needed traits.
impl<T: ?Sized> DFIterator for T where T: DoubleEndedIterator + Send + Sync {}

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
        Box::new(self.downcast_iter())
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
    #[allow(clippy::wrong_self_convention)]
    pub fn into_no_null_iter(
        &self,
    ) -> impl Iterator<Item = bool> + '_ + Send + Sync + DoubleEndedIterator {
        BoolIterNoNull::new(self.downcast_ref())
    }
}

impl<'a> IntoIterator for &'a DFStringArray {
    type Item = Option<&'a str>;
    type IntoIter = Box<dyn DFIterator<Item = Self::Item> + 'a>;
    fn into_iter(self) -> Self::IntoIter {
        Box::new(self.downcast_iter())
    }
}

pub struct Utf8IterNoNull<'a> {
    array: &'a LargeStringArray,
    current: usize,
    current_end: usize,
}

impl<'a> Utf8IterNoNull<'a> {
    /// create a new iterator
    pub fn new(array: &'a LargeStringArray) -> Self {
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

impl DFStringArray {
    #[allow(clippy::wrong_self_convention)]
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
        Box::new(self.downcast_iter())
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
            let array = unsafe { self.array.value_unchecked(old) };
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

            let array = unsafe { self.array.value_unchecked(self.current_end) };
            Some(array.into_series())
        }
    }
}

/// all arrays have known size.
impl<'a> ExactSizeIterator for ListIterNoNull<'a> {}

impl DFListArray {
    #[allow(clippy::wrong_self_convention)]
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
