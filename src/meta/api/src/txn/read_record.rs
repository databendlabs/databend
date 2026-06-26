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

use databend_meta_client::kvapi;
use databend_meta_client::types::SeqV;

/// A record fetched by a transaction read without arming a commit guard.
pub struct ReadRecord<K: kvapi::Key> {
    seq_v: Option<SeqV<K::ValueType>>,
}

impl<K> ReadRecord<K>
where K: kvapi::Key
{
    pub(crate) fn new(seq_v: Option<SeqV<K::ValueType>>) -> Self {
        Self { seq_v }
    }

    /// The version read, `0` if the key was absent.
    pub fn seq(&self) -> u64 {
        self.seq_v.as_ref().map(|s| s.seq).unwrap_or(0)
    }

    /// The full record read — seq, meta, and value — `None` if the key was
    /// absent. Use this to reach the value's `meta`.
    pub fn seq_v(&self) -> Option<&SeqV<K::ValueType>> {
        self.seq_v.as_ref()
    }

    /// The value read, `None` if the key was absent.
    pub fn value(&self) -> Option<&K::ValueType> {
        self.seq_v.as_ref().map(|s| &s.data)
    }

    /// Consume the handle, yielding the value read.
    pub fn into_value(self) -> Option<K::ValueType> {
        self.seq_v.map(|seq_v| seq_v.data)
    }
}
