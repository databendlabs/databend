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

use crate::protobuf as pb;
use crate::seq_value::KVMeta;
use crate::seq_value::SeqV;

impl From<KVMeta> for pb::KvMeta {
    fn from(m: KVMeta) -> Self {
        Self {
            expire_at: m.get_expire_at_ms().map(|x| x / 1000),
        }
    }
}

impl From<pb::KvMeta> for KVMeta {
    fn from(m: pb::KvMeta) -> Self {
        Self {
            expire_at: m.expire_at,
        }
    }
}

impl pb::KvMeta {
    pub fn new_expire(expire_at: u64) -> Self {
        Self {
            expire_at: Some(expire_at),
        }
    }
}

impl pb::SeqV {
    pub fn new(seq: u64, data: Vec<u8>) -> Self {
        Self {
            seq,
            data,
            meta: None,
        }
    }

    pub fn with_meta(seq: u64, meta: Option<pb::KvMeta>, data: Vec<u8>) -> Self {
        Self { seq, meta, data }
    }
}

impl From<SeqV> for pb::SeqV {
    fn from(sv: SeqV) -> Self {
        Self {
            seq: sv.seq,
            data: sv.data,
            meta: sv.meta.map(pb::KvMeta::from),
        }
    }
}

impl From<pb::SeqV> for SeqV {
    /// Convert from protobuf SeqV to the native SeqV we defined.
    fn from(sv: pb::SeqV) -> Self {
        Self {
            seq: sv.seq,
            data: sv.data,
            meta: sv.meta.map(KVMeta::from),
        }
    }
}
