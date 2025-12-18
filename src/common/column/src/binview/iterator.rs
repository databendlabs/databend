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

use super::BinaryViewColumnGeneric;
use crate::binview::ViewType;
use crate::binview::builder::BinaryViewColumnBuilder;
use crate::iterator::ColumnAccessor;
use crate::iterator::ColumnValuesIter;

unsafe impl<'a, T: ViewType + ?Sized> ColumnAccessor<'a> for BinaryViewColumnGeneric<T> {
    type Item = &'a T;

    #[inline]
    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        unsafe { self.value_unchecked(index) }
    }

    #[inline]
    fn len(&self) -> usize {
        self.views.len()
    }
}

/// Iterator of values of an [`BinaryArray`].
pub type BinaryViewColumnIter<'a, T> = ColumnValuesIter<'a, BinaryViewColumnGeneric<T>>;

impl<'a, T: ViewType + ?Sized> IntoIterator for &'a BinaryViewColumnGeneric<T> {
    type Item = &'a T;
    type IntoIter = BinaryViewColumnIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

unsafe impl<'a, T: ViewType + ?Sized> ColumnAccessor<'a> for BinaryViewColumnBuilder<T> {
    type Item = &'a T;

    #[inline]
    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        unsafe { self.value_unchecked(index) }
    }

    #[inline]
    fn len(&self) -> usize {
        self.views().len()
    }
}

/// Iterator of values of an [`BinaryViewColumnBuilder`].
pub type BinaryViewBuilderIter<'a, T> = ColumnValuesIter<'a, BinaryViewColumnBuilder<T>>;
