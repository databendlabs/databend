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

use super::FixedSizeListArray;
use crate::arrow::array::Array;
use crate::arrow::array::ArrayAccessor;
use crate::arrow::array::ArrayValuesIter;
use crate::arrow::bitmap::utils::BitmapIter;
use crate::arrow::bitmap::utils::ZipValidity;

unsafe impl<'a> ArrayAccessor<'a> for FixedSizeListArray {
    type Item = Box<dyn Array>;

    #[inline]
    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        self.value_unchecked(index)
    }

    #[inline]
    fn len(&self) -> usize {
        self.len()
    }
}

/// Iterator of values of a [`FixedSizeListArray`].
pub type FixedSizeListValuesIter<'a> = ArrayValuesIter<'a, FixedSizeListArray>;

type ZipIter<'a> = ZipValidity<Box<dyn Array>, FixedSizeListValuesIter<'a>, BitmapIter<'a>>;

impl<'a> IntoIterator for &'a FixedSizeListArray {
    type Item = Option<Box<dyn Array>>;
    type IntoIter = ZipIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> FixedSizeListArray {
    /// Returns an iterator of `Option<Box<dyn Array>>`
    pub fn iter(&'a self) -> ZipIter<'a> {
        ZipValidity::new_with_validity(FixedSizeListValuesIter::new(self), self.validity())
    }

    /// Returns an iterator of `Box<dyn Array>`
    pub fn values_iter(&'a self) -> FixedSizeListValuesIter<'a> {
        FixedSizeListValuesIter::new(self)
    }
}
