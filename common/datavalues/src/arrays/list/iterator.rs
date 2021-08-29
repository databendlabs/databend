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
