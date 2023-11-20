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

use super::MutablePrimitiveArray;
use super::PrimitiveArray;
use crate::arrow::array::MutableArray;
use crate::arrow::bitmap::utils::BitmapIter;
use crate::arrow::bitmap::utils::ZipValidity;
use crate::arrow::bitmap::IntoIter as BitmapIntoIter;
use crate::arrow::buffer::IntoIter;
use crate::arrow::types::NativeType;

impl<T: NativeType> IntoIterator for PrimitiveArray<T> {
    type Item = Option<T>;
    type IntoIter = ZipValidity<T, IntoIter<T>, BitmapIntoIter>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        let (_, values, validity) = self.into_inner();
        let values = values.into_iter();
        let validity =
            validity.and_then(|validity| (validity.unset_bits() > 0).then(|| validity.into_iter()));
        ZipValidity::new(values, validity)
    }
}

impl<'a, T: NativeType> IntoIterator for &'a PrimitiveArray<T> {
    type Item = Option<&'a T>;
    type IntoIter = ZipValidity<&'a T, std::slice::Iter<'a, T>, BitmapIter<'a>>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, T: NativeType> MutablePrimitiveArray<T> {
    /// Returns an iterator over `Option<T>`
    #[inline]
    pub fn iter(&'a self) -> ZipValidity<&'a T, std::slice::Iter<'a, T>, BitmapIter<'a>> {
        ZipValidity::new(
            self.values().iter(),
            self.validity().as_ref().map(|x| x.iter()),
        )
    }

    /// Returns an iterator of `T`
    #[inline]
    pub fn values_iter(&'a self) -> std::slice::Iter<'a, T> {
        self.values().iter()
    }
}
