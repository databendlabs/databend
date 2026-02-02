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

use databend_common_meta_types::SeqV;

use crate::kvapi;

/// A Key-Value pair for type Key. The value does not have a seq number.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BasicPair<K>
where K: kvapi::Key
{
    key: K,
    value: K::ValueType,
}

impl<K> BasicPair<K>
where K: kvapi::Key
{
    pub fn new(key: K, value: K::ValueType) -> Self {
        Self { key, value }
    }

    pub fn key(&self) -> &K {
        &self.key
    }

    pub fn value(&self) -> &K::ValueType {
        &self.value
    }

    pub fn unpack(self) -> (K, K::ValueType) {
        (self.key, self.value)
    }
}

/// A Key-Value pair for type Key. The value has a seq number.
pub struct Pair<K>
where K: kvapi::Key
{
    key: K,
    seq_v: SeqV<K::ValueType>,
}

impl<K> Pair<K>
where K: kvapi::Key
{
    pub fn new(key: K, seq_v: SeqV<K::ValueType>) -> Self {
        Self { key, seq_v }
    }

    pub fn key(&self) -> &K {
        &self.key
    }

    pub fn seq_value(&self) -> &SeqV<K::ValueType> {
        &self.seq_v
    }
}

/// Same as `Pair`, but the key is a `SeqV` and also have a seq number.
pub struct SeqPair<K>
where K: kvapi::Key
{
    key: SeqV<K>,
    seq_v: SeqV<K::ValueType>,
}

impl<K> SeqPair<K>
where K: kvapi::Key
{
    pub fn new(key: SeqV<K>, seq_v: SeqV<K::ValueType>) -> Self {
        Self { key, seq_v }
    }

    pub fn seq_key(&self) -> &SeqV<K> {
        &self.key
    }

    pub fn seq_value(&self) -> &SeqV<K::ValueType> {
        &self.seq_v
    }

    pub fn into_pair(self) -> Pair<K> {
        Pair {
            key: self.key.data,
            seq_v: self.seq_v,
        }
    }

    pub fn unpack(self) -> (SeqV<K>, SeqV<K::ValueType>) {
        (self.key, self.seq_v)
    }
}

impl<K> From<SeqPair<K>> for Pair<K>
where K: kvapi::Key
{
    fn from(sp: SeqPair<K>) -> Self {
        sp.into_pair()
    }
}
