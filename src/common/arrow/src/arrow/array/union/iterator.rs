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

use super::UnionArray;
use crate::arrow::scalar::Scalar;
use crate::arrow::trusted_len::TrustedLen;

#[derive(Debug, Clone)]
pub struct UnionIter<'a> {
    array: &'a UnionArray,
    current: usize,
}

impl<'a> UnionIter<'a> {
    #[inline]
    pub fn new(array: &'a UnionArray) -> Self {
        Self { array, current: 0 }
    }
}

impl<'a> Iterator for UnionIter<'a> {
    type Item = Box<dyn Scalar>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.current == self.array.len() {
            None
        } else {
            let old = self.current;
            self.current += 1;
            Some(unsafe { self.array.value_unchecked(old) })
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.array.len() - self.current;
        (len, Some(len))
    }
}

impl<'a> IntoIterator for &'a UnionArray {
    type Item = Box<dyn Scalar>;
    type IntoIter = UnionIter<'a>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> UnionArray {
    /// constructs a new iterator
    #[inline]
    pub fn iter(&'a self) -> UnionIter<'a> {
        UnionIter::new(self)
    }
}

impl<'a> std::iter::ExactSizeIterator for UnionIter<'a> {}

unsafe impl<'a> TrustedLen for UnionIter<'a> {}
