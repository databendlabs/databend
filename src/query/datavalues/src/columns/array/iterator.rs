// Copyright 2022 Datafuse Labs.
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

use crate::prelude::*;

#[derive(Debug, Clone)]
pub struct ArrayValueIter<'a> {
    column: &'a ArrayColumn,
    index: usize,
}

impl<'a> ArrayValueIter<'a> {
    /// Creates a new [`ArrayValueIter`]
    pub fn new(column: &'a ArrayColumn) -> Self {
        Self { column, index: 0 }
    }
}

impl<'a> Iterator for ArrayValueIter<'a> {
    type Item = <ArrayValue as Scalar>::RefType<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let old = self.index;
        if self.index >= self.column.len() {
            return None;
        } else {
            self.index += 1;
        }
        Some(ArrayValueRef::Indexed {
            column: self.column,
            idx: old,
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.column.len() - self.index,
            Some(self.column.len() - self.index),
        )
    }
}

impl<'a> ExactSizeIterator for ArrayValueIter<'a> {
    fn len(&self) -> usize {
        self.column.len() - self.index
    }
}

unsafe impl TrustedLen for ArrayValueIter<'_> {}

impl<'a> ArrayColumn {
    pub fn iter(&'a self) -> ArrayValueIter<'a> {
        ArrayValueIter::new(self)
    }
}
