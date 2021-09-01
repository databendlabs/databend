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
use common_arrow::arrow::bitmap::utils::ZipValidity;
use common_arrow::arrow::trusted_len::TrustedLen;

use crate::prelude::*;

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
        Utf8IterNoNull::new(self.inner())
    }
}
