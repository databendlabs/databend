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
