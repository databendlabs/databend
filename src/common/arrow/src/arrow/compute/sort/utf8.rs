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

use super::common;
use super::SortOptions;
use crate::arrow::array::DictionaryArray;
use crate::arrow::array::DictionaryKey;
use crate::arrow::array::PrimitiveArray;
use crate::arrow::array::Utf8Array;
use crate::arrow::offset::Offset;
use crate::arrow::types::Index;

pub(super) fn indices_sorted_unstable_by<I: Index, O: Offset>(
    array: &Utf8Array<O>,
    options: &SortOptions,
    limit: Option<usize>,
) -> PrimitiveArray<I> {
    let get = |idx| unsafe { array.value_unchecked(idx) };
    let cmp = |lhs: &&str, rhs: &&str| lhs.cmp(rhs);
    common::indices_sorted_unstable_by(array.validity(), get, cmp, array.len(), options, limit)
}

pub(super) fn indices_sorted_unstable_by_dictionary<I: Index, K: DictionaryKey, O: Offset>(
    array: &DictionaryArray<K>,
    options: &SortOptions,
    limit: Option<usize>,
) -> PrimitiveArray<I> {
    let keys = array.keys();

    let dict = array
        .values()
        .as_any()
        .downcast_ref::<Utf8Array<O>>()
        .unwrap();

    let get = |index| unsafe {
        // safety: indices_sorted_unstable_by is guaranteed to get items in bounds
        let index = keys.value_unchecked(index);
        // safety: dictionaries are guaranteed to have valid usize keys
        let index = index.as_usize();
        // safety: dictionaries are guaranteed to have keys in bounds
        dict.value_unchecked(index)
    };

    let cmp = |lhs: &&str, rhs: &&str| lhs.cmp(rhs);
    common::indices_sorted_unstable_by(array.validity(), get, cmp, array.len(), options, limit)
}
