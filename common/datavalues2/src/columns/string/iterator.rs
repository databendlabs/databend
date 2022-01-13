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


use crate::prelude::*;




/// Iterator over slices of `&[u8]`.
#[derive(Debug, Clone)]
pub struct StringValueIter<'a> {
    column: &'a StringColumn,
    index: usize,
}

impl<'a> StringValueIter<'a> {
    /// Creates a new [`StringValueIter`]
    pub fn new(column: &'a StringColumn) -> Self {
        Self { column, index: 0 }
    }
}

impl<'a> Iterator for StringValueIter<'a> {
    type Item = &'a [u8];

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.column.len() {
            return None;
        } else {
            self.index += 1;
        }
        Some(unsafe { self.column.value_unchecked(self.index - 1) })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.column.len() - self.index,
            Some(self.column.len() - self.index),
        )
    }
}



impl<'a> StringColumn {
    pub fn iter(&'a self) -> StringValueIter<'a> {
        StringValueIter::new(self)
    }
}


