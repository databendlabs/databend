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

use super::Bitmap;
use crate::arrow::trusted_len::TrustedLen;

/// This crates' equivalent of [`std::vec::IntoIter`] for [`Bitmap`].
#[derive(Debug, Clone)]
pub struct IntoIter {
    values: Bitmap,
    index: usize,
    end: usize,
}

impl IntoIter {
    /// Creates a new [`IntoIter`] from a [`Bitmap`]
    #[inline]
    pub fn new(values: Bitmap) -> Self {
        let end = values.len();
        Self {
            values,
            index: 0,
            end,
        }
    }
}

impl Iterator for IntoIter {
    type Item = bool;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            return None;
        }
        let old = self.index;
        self.index += 1;
        Some(unsafe { self.values.get_bit_unchecked(old) })
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

impl DoubleEndedIterator for IntoIter {
    #[inline]
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            None
        } else {
            self.end -= 1;
            Some(unsafe { self.values.get_bit_unchecked(self.end) })
        }
    }
}

unsafe impl TrustedLen for IntoIter {}
