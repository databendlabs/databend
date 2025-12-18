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

use std::fmt;
use std::time::Duration;

use display_more::DisplayOptionExt;
use serde::Deserialize;
use serde::Serialize;

use crate::MetaSpec;
use crate::Operation;
use crate::With;
use crate::match_seq::MatchSeq;

/// Update or insert a general purpose kv store
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, deepsize::DeepSizeOf)]
pub struct UpsertKV {
    pub key: String,

    /// Since a sequence number is always positive, using Exact(0) to perform an add-if-absent operation.
    /// - GE(1) to perform an update-any operation.
    /// - Exact(n) to perform an update on some specified version.
    /// - Any to perform an update or insert that always takes effect.
    pub seq: MatchSeq,

    /// The value to set. A `None` indicates to delete it.
    pub value: Operation<Vec<u8>>,

    /// Meta data of a value.
    pub value_meta: Option<MetaSpec>,
}

impl fmt::Display for UpsertKV {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}({:?}) = {:?} ({})",
            self.key,
            self.seq,
            self.value,
            self.value_meta.display()
        )
    }
}

impl UpsertKV {
    pub fn new(
        key: impl ToString,
        seq: MatchSeq,
        value: Operation<Vec<u8>>,
        value_meta: Option<MetaSpec>,
    ) -> Self {
        Self {
            key: key.to_string(),
            seq,
            value,
            value_meta,
        }
    }

    pub fn insert(key: impl ToString, value: &[u8]) -> Self {
        Self {
            key: key.to_string(),
            seq: MatchSeq::Exact(0),
            value: Operation::Update(value.to_vec()),
            value_meta: None,
        }
    }

    pub fn update(key: impl ToString, value: &[u8]) -> Self {
        Self {
            key: key.to_string(),
            seq: MatchSeq::GE(0),
            value: Operation::Update(value.to_vec()),
            value_meta: None,
        }
    }

    pub fn delete(key: impl ToString) -> Self {
        Self {
            key: key.to_string(),
            seq: MatchSeq::GE(1),
            value: Operation::Delete,
            value_meta: None,
        }
    }

    pub fn with_expire_sec(self, expire_at_sec: u64) -> Self {
        self.with(MetaSpec::new_expire(expire_at_sec))
    }

    /// Set the time to last for the value.
    /// When the ttl is passed, the value is deleted.
    pub fn with_ttl(self, ttl: Duration) -> Self {
        self.with(MetaSpec::new_ttl(ttl))
    }
}

impl With<MatchSeq> for UpsertKV {
    fn with(mut self, seq: MatchSeq) -> Self {
        self.seq = seq;
        self
    }
}

impl With<MetaSpec> for UpsertKV {
    fn with(mut self, meta: MetaSpec) -> Self {
        self.value_meta = Some(meta);
        self
    }
}
