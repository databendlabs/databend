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

use std::time::Duration;

use databend_meta_kvapi::kvapi;
use databend_meta_types::MatchSeq;
use databend_meta_types::MetaSpec;
use databend_meta_types::Operation;
use databend_meta_types::SeqV;
use databend_meta_types::With;

#[derive(Clone, Debug)]
pub struct UpsertPB<K: kvapi::Key> {
    pub key: K,

    /// Since a sequence number is always positive, using Exact(0) to perform an add-if-absent operation.
    /// - GE(1) to perform an update-any operation.
    /// - Exact(n) to perform an update on some specified version.
    /// - Any to perform an update or insert that always takes effect.
    pub seq: MatchSeq,

    /// The value to set. A `None` indicates to delete it.
    pub value: Operation<K::ValueType>,

    /// Meta data of a value.
    pub value_meta: Option<MetaSpec>,
}

impl<K: kvapi::Key> UpsertPB<K> {
    pub fn new(
        key: K,
        seq: MatchSeq,
        value: Operation<K::ValueType>,
        value_meta: Option<MetaSpec>,
    ) -> Self {
        Self {
            key,
            seq,
            value,
            value_meta,
        }
    }

    pub fn insert(key: K, value: K::ValueType) -> Self {
        Self {
            key,
            seq: MatchSeq::Exact(0),
            value: Operation::Update(value),
            value_meta: None,
        }
    }

    pub fn update(key: K, value: K::ValueType) -> Self {
        Self {
            key,
            seq: MatchSeq::GE(0),
            value: Operation::Update(value),
            value_meta: None,
        }
    }

    /// Update the value only when the seq matches exactly. Note that the meta is not copied.
    pub fn update_exact(key: K, value: SeqV<K::ValueType>) -> Self {
        Self {
            key,
            seq: MatchSeq::Exact(value.seq),
            value: Operation::Update(value.data),
            value_meta: None,
        }
    }

    pub fn delete(key: K) -> Self {
        Self {
            key,
            seq: MatchSeq::GE(1),
            value: Operation::Delete,
            value_meta: None,
        }
    }

    /// Set the time to last for the value.
    /// When the ttl is passed, the value is deleted.
    pub fn with_ttl(self, ttl: Duration) -> Self {
        self.with(MetaSpec::new_ttl(ttl))
    }
}

impl<K: kvapi::Key> With<MatchSeq> for UpsertPB<K> {
    fn with(mut self, seq: MatchSeq) -> Self {
        self.seq = seq;
        self
    }
}

impl<K: kvapi::Key> With<MetaSpec> for UpsertPB<K> {
    fn with(mut self, meta: MetaSpec) -> Self {
        self.value_meta = Some(meta);
        self
    }
}
