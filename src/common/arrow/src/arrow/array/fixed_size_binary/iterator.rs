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

use super::FixedSizeBinaryArray;
use super::MutableFixedSizeBinaryArray;
use crate::arrow::array::MutableArray;
use crate::arrow::bitmap::utils::BitmapIter;
use crate::arrow::bitmap::utils::ZipValidity;

impl<'a> IntoIterator for &'a FixedSizeBinaryArray {
    type Item = Option<&'a [u8]>;
    type IntoIter = ZipValidity<&'a [u8], std::slice::ChunksExact<'a, u8>, BitmapIter<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> FixedSizeBinaryArray {
    /// constructs a new iterator
    pub fn iter(
        &'a self,
    ) -> ZipValidity<&'a [u8], std::slice::ChunksExact<'a, u8>, BitmapIter<'a>> {
        ZipValidity::new_with_validity(self.values_iter(), self.validity())
    }

    /// Returns iterator over the values of [`FixedSizeBinaryArray`]
    pub fn values_iter(&'a self) -> std::slice::ChunksExact<'a, u8> {
        self.values().chunks_exact(self.size)
    }
}

impl<'a> IntoIterator for &'a MutableFixedSizeBinaryArray {
    type Item = Option<&'a [u8]>;
    type IntoIter = ZipValidity<&'a [u8], std::slice::ChunksExact<'a, u8>, BitmapIter<'a>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a> MutableFixedSizeBinaryArray {
    /// constructs a new iterator
    pub fn iter(
        &'a self,
    ) -> ZipValidity<&'a [u8], std::slice::ChunksExact<'a, u8>, BitmapIter<'a>> {
        ZipValidity::new(self.iter_values(), self.validity().map(|x| x.iter()))
    }

    /// Returns iterator over the values of [`MutableFixedSizeBinaryArray`]
    pub fn iter_values(&'a self) -> std::slice::ChunksExact<'a, u8> {
        self.values().chunks_exact(self.size())
    }
}
