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

use super::builder::FixedSizeBinaryColumnBuilder;
use super::FixedSizeBinaryColumn;
use crate::iterator::ColumnAccessor;
use crate::iterator::ColumnValuesIter;

unsafe impl<'a> ColumnAccessor<'a> for FixedSizeBinaryColumn {
    type Item = &'a [u8];

    #[inline]
    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        self.index_unchecked(index)
    }

    #[inline]
    fn len(&self) -> usize {
        self.len()
    }
}

/// Iterator of values of an [`BinaryArray`].
pub type FixedSizeBinaryColumnIter<'a> = ColumnValuesIter<'a, FixedSizeBinaryColumn>;

impl<'a> IntoIterator for &'a FixedSizeBinaryColumn {
    type Item = &'a [u8];
    type IntoIter = FixedSizeBinaryColumnIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

unsafe impl<'a> ColumnAccessor<'a> for FixedSizeBinaryColumnBuilder {
    type Item = &'a [u8];

    #[inline]
    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        self.index_unchecked(index)
    }

    #[inline]
    fn len(&self) -> usize {
        self.len()
    }
}

/// Iterator of values of an [`FixedSizeBinaryColumnBuilder`].
pub type FixedSizeBinaryColumnBuilderIter<'a> = ColumnValuesIter<'a, FixedSizeBinaryColumnBuilder>;
