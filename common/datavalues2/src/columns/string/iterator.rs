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

impl<'a> ExactSizeIterator for StringValueIter<'a> {
    fn len(&self) -> usize {
        self.column.len() - self.index
    }
}

unsafe impl TrustedLen for StringValueIter<'_> {}

impl<'a> StringColumn {
    pub fn iter(&'a self) -> StringValueIter<'a> {
        StringValueIter::new(self)
    }
}

pub trait NewColumn<N> {
    /// create non-nullable column by values
    fn new_from_slice<P: AsRef<[N]>>(v: P) -> Self;
    fn new_from_iter(it: impl Iterator<Item = N>) -> Self;
}

impl<S> NewColumn<S> for StringColumn
where S: AsRef<[u8]>
{
    fn new_from_slice<P: AsRef<[S]>>(slice: P) -> Self {
        let slice = slice.as_ref();
        let values_size = slice.iter().fold(0, |acc, s| acc + s.as_ref().len());
        let mut builder = MutableStringColumn::with_values_capacity(values_size, slice.len());

        slice.iter().for_each(|val| {
            builder.append_value(val.as_ref());
        });
        builder.finish()
    }

    /// Create a new DataArray from an iterator.
    fn new_from_iter(it: impl Iterator<Item = S>) -> Self {
        let cap = get_iter_capacity(&it);
        let mut builder = MutableStringColumn::with_capacity(cap);
        it.for_each(|v| builder.append_value(v));
        builder.finish()
    }
}
