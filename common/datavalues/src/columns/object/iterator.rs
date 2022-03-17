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
pub struct ObjectValueIter<'a, T: ObjectType> {
    column: &'a ObjectColumn<T>,
    index: usize,
}

impl<'a, T: ObjectType> ObjectValueIter<'a, T> {
    /// Creates a new [`ObjectValueIter`]
    pub fn new(column: &'a ObjectColumn<T>) -> Self {
        Self { column, index: 0 }
    }
}

impl<'a, T> Iterator for ObjectValueIter<'a, T>
where T: Scalar + ObjectType
{
    type Item = T::RefType<'a>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let index = self.index;
        if self.index >= self.column.len() {
            return None;
        } else {
            self.index += 1;
        }
        self.column.values.get(index).map(|c| c.as_scalar_ref())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.column.len() - self.index,
            Some(self.column.len() - self.index),
        )
    }
}

impl<'a, T: ObjectType> ExactSizeIterator for ObjectValueIter<'a, T> {
    fn len(&self) -> usize {
        self.column.len() - self.index
    }
}

unsafe impl<T: ObjectType> TrustedLen for ObjectValueIter<'_, T> {}

impl<'a, T: ObjectType> ObjectColumn<T> {
    pub fn iter(&'a self) -> ObjectValueIter<'a, T> {
        ObjectValueIter::new(self)
    }
}
