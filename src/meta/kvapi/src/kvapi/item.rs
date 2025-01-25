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

use databend_common_meta_types::seq_value::SeqV;

use crate::kvapi::Key;

/// Key-Value item contains key and optional value with seq number.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Item<K: Key> {
    pub key: K,
    pub seqv: Option<SeqV<K::ValueType>>,
}

impl<K: Key> Item<K> {
    pub fn new(key: K, seqv: Option<SeqV<K::ValueType>>) -> Self {
        Item { key, seqv }
    }
}

/// Key-Value item contains key and non-optional value with seq number.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NonEmptyItem<K: Key> {
    pub key: K,
    pub seqv: SeqV<K::ValueType>,
}

impl<K: Key> NonEmptyItem<K> {
    pub fn new(key: K, seqv: SeqV<K::ValueType>) -> Self {
        NonEmptyItem { key, seqv }
    }
}
