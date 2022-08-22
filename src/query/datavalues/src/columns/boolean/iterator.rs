// Copyright 2021 Datafuse Labs.
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

use common_arrow::arrow::bitmap::Bitmap;

use crate::prelude::*;

impl<'a> BooleanColumn {
    pub fn iter(&'a self) -> BitmapValuesIter<'a> {
        BitmapValuesIter::<'a> {
            values: self.values(),
            index: 0,
            end: self.len(),
        }
    }
}

/// An iterator over bits according to the [LSB](https://en.wikipedia.org/wiki/Bit_numbering#Least_significant_bit),
/// i.e. the bytes `[4u8, 128u8]` correspond to `[false, false, true, false, ..., true]`.
#[derive(Debug, Clone)]
pub struct BitmapValuesIter<'a> {
    values: &'a Bitmap,
    index: usize,
    end: usize,
}

impl<'a> Iterator for BitmapValuesIter<'a> {
    type Item = bool;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.end {
            return None;
        }
        let old = self.index;
        self.index += 1;
        // See comment in `new`
        Some(unsafe { self.values.get_bit_unchecked(old) })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let exact = self.end - self.index;
        (exact, Some(exact))
    }
}

impl<'a> DoubleEndedIterator for BitmapValuesIter<'a> {
    #[inline]
    fn next_back(&mut self) -> Option<bool> {
        if self.index == self.end {
            None
        } else {
            self.end -= 1;
            // See comment in `new`; end was first decreased
            Some(unsafe { self.values.get_bit_unchecked(self.end) })
        }
    }
}

impl<'a> ExactSizeIterator for BitmapValuesIter<'a> {
    fn len(&self) -> usize {
        self.end - self.index
    }
}

unsafe impl TrustedLen for BitmapValuesIter<'_> {}
