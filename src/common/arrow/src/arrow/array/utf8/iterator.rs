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

use super::MutableUtf8Array;
use super::MutableUtf8ValuesArray;
use super::Utf8Array;
use crate::arrow::array::ArrayAccessor;
use crate::arrow::array::ArrayValuesIter;
use crate::arrow::bitmap::utils::BitmapIter;
use crate::arrow::bitmap::utils::ZipValidity;
use crate::arrow::offset::Offset;

unsafe impl<'a, O: Offset> ArrayAccessor<'a> for Utf8Array<O> {
    type Item = &'a str;

    #[inline]
    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        self.value_unchecked(index)
    }

    #[inline]
    fn len(&self) -> usize {
        self.len()
    }
}

/// Iterator of values of an [`Utf8Array`].
pub type Utf8ValuesIter<'a, O> = ArrayValuesIter<'a, Utf8Array<O>>;

impl<'a, O: Offset> IntoIterator for &'a Utf8Array<O> {
    type Item = Option<&'a str>;
    type IntoIter = ZipValidity<&'a str, Utf8ValuesIter<'a, O>, BitmapIter<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

unsafe impl<'a, O: Offset> ArrayAccessor<'a> for MutableUtf8Array<O> {
    type Item = &'a str;

    #[inline]
    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        self.value_unchecked(index)
    }

    #[inline]
    fn len(&self) -> usize {
        self.len()
    }
}

/// Iterator of values of an [`MutableUtf8ValuesArray`].
pub type MutableUtf8ValuesIter<'a, O> = ArrayValuesIter<'a, MutableUtf8ValuesArray<O>>;

impl<'a, O: Offset> IntoIterator for &'a MutableUtf8Array<O> {
    type Item = Option<&'a str>;
    type IntoIter = ZipValidity<&'a str, MutableUtf8ValuesIter<'a, O>, BitmapIter<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

unsafe impl<'a, O: Offset> ArrayAccessor<'a> for MutableUtf8ValuesArray<O> {
    type Item = &'a str;

    #[inline]
    unsafe fn value_unchecked(&'a self, index: usize) -> Self::Item {
        self.value_unchecked(index)
    }

    #[inline]
    fn len(&self) -> usize {
        self.len()
    }
}

impl<'a, O: Offset> IntoIterator for &'a MutableUtf8ValuesArray<O> {
    type Item = &'a str;
    type IntoIter = ArrayValuesIter<'a, MutableUtf8ValuesArray<O>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
