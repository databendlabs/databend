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

use super::BinaryColumn;
use super::builder::BinaryColumnBuilder;
use crate::iterator::ColumnAccessor;
use crate::iterator::ColumnValuesIter;

unsafe impl<'a> ColumnAccessor<'a> for BinaryColumn {
    type Item = &'a [u8];

    #[inline]
    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item { unsafe {
        self.index_unchecked(index)
    }}

    #[inline]
    fn len(&self) -> usize {
        self.offsets().len() - 1
    }
}

/// Iterator of values of an [`BinaryArray`].
pub type BinaryColumnIter<'a> = ColumnValuesIter<'a, BinaryColumn>;

impl<'a> IntoIterator for &'a BinaryColumn {
    type Item = &'a [u8];
    type IntoIter = BinaryColumnIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

unsafe impl<'a> ColumnAccessor<'a> for BinaryColumnBuilder {
    type Item = &'a [u8];

    #[inline]
    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item { unsafe {
        self.index_unchecked(index)
    }}

    #[inline]
    fn len(&self) -> usize {
        self.offsets.len() - 1
    }
}

/// Iterator of values of an [`BinaryColumnBuilder`].
pub type BinaryColumnBuilderIter<'a> = ColumnValuesIter<'a, BinaryColumnBuilder>;
