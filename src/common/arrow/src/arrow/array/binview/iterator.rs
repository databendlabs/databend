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

use crate::arrow::array::binview::mutable::MutableBinaryViewArray;
use crate::arrow::array::binview::BinaryViewArrayGeneric;
use crate::arrow::array::binview::ViewType;
use crate::arrow::array::ArrayAccessor;
use crate::arrow::array::ArrayValuesIter;
use crate::arrow::bitmap::utils::BitmapIter;
use crate::arrow::bitmap::utils::ZipValidity;

unsafe impl<'a, T: ViewType + ?Sized> ArrayAccessor<'a> for BinaryViewArrayGeneric<T> {
    type Item = &'a T;

    #[inline]
    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        self.value_unchecked(index)
    }

    #[inline]
    fn len(&self) -> usize {
        self.views.len()
    }
}

/// Iterator of values of an [`BinaryArray`].
pub type BinaryViewValueIter<'a, T> = ArrayValuesIter<'a, BinaryViewArrayGeneric<T>>;

impl<'a, T: ViewType + ?Sized> IntoIterator for &'a BinaryViewArrayGeneric<T> {
    type Item = Option<&'a T>;
    type IntoIter = ZipValidity<&'a T, BinaryViewValueIter<'a, T>, BitmapIter<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

unsafe impl<'a, T: ViewType + ?Sized> ArrayAccessor<'a> for MutableBinaryViewArray<T> {
    type Item = &'a T;

    #[inline]
    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        self.value_unchecked(index)
    }

    #[inline]
    fn len(&self) -> usize {
        self.views().len()
    }
}

/// Iterator of values of an [`MutableBinaryViewArray`].
pub type MutableBinaryViewValueIter<'a, T> = ArrayValuesIter<'a, MutableBinaryViewArray<T>>;
