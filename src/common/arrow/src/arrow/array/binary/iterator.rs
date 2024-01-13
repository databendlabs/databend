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

use super::BinaryArray;
use super::MutableBinaryValuesArray;
use crate::arrow::array::ArrayAccessor;
use crate::arrow::array::ArrayValuesIter;
use crate::arrow::bitmap::utils::BitmapIter;
use crate::arrow::bitmap::utils::ZipValidity;
use crate::arrow::offset::Offset;

unsafe impl<'a, O: Offset> ArrayAccessor<'a> for BinaryArray<O> {
    type Item = &'a [u8];

    #[inline]
    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        self.value_unchecked(index)
    }

    #[inline]
    fn len(&self) -> usize {
        self.len()
    }
}

/// Iterator of values of an [`BinaryArray`].
pub type BinaryValueIter<'a, O> = ArrayValuesIter<'a, BinaryArray<O>>;

impl<'a, O: Offset> IntoIterator for &'a BinaryArray<O> {
    type Item = Option<&'a [u8]>;
    type IntoIter = ZipValidity<&'a [u8], BinaryValueIter<'a, O>, BitmapIter<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

/// Iterator of values of an [`MutableBinaryValuesArray`].
pub type MutableBinaryValuesIter<'a, O> = ArrayValuesIter<'a, MutableBinaryValuesArray<O>>;

impl<'a, O: Offset> IntoIterator for &'a MutableBinaryValuesArray<O> {
    type Item = &'a [u8];
    type IntoIter = MutableBinaryValuesIter<'a, O>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
