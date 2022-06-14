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
pub struct StructValueIter<'a> {
    column: &'a StructColumn,
    index: usize,
}

impl<'a> StructValueIter<'a> {
    /// Creates a new [`StructValueIter`]
    pub fn new(column: &'a StructColumn) -> Self {
        Self { column, index: 0 }
    }
}

impl<'a> Iterator for StructValueIter<'a> {
    type Item = <StructValue as Scalar>::RefType<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let old = self.index;
        if self.index >= self.column.len() {
            return None;
        } else {
            self.index += 1;
        }
        Some(StructValueRef::Indexed {
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

impl<'a> ExactSizeIterator for StructValueIter<'a> {
    fn len(&self) -> usize {
        self.column.len() - self.index
    }
}

unsafe impl TrustedLen for StructValueIter<'_> {}

impl<'a> StructColumn {
    pub fn iter(&'a self) -> StructValueIter<'a> {
        StructValueIter::new(self)
    }
}
