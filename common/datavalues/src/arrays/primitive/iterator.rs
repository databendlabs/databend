// Copyright 2020 Datafuse Labs.
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

use common_arrow::arrow::bitmap::utils::ZipValidity;
use common_arrow::arrow::trusted_len::TrustedLen;

use crate::prelude::*;

impl<'a, T: DFPrimitiveType> IntoIterator for &'a DFPrimitiveArray<T> {
    type Item = Option<&'a T>;
    type IntoIter = ZipValidity<'a, &'a T, std::slice::Iter<'a, T>>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, T: DFPrimitiveType> DFPrimitiveArray<T> {
    /// constructs a new iterator
    #[inline]
    pub fn iter(&'a self) -> ZipValidity<'a, &'a T, std::slice::Iter<'a, T>> {
        self.array.iter()
    }

    pub fn into_no_null_iter(
        &'a self,
    ) -> impl TrustedLen<Item = &'a T> + Send + Sync + ExactSizeIterator {
        // .copied was significantly slower in benchmark, next call did not inline?
        self.array.values().iter()
    }
}
