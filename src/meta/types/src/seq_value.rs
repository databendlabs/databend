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

use std::convert::TryInto;
use std::fmt::Formatter;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use serde::Deserialize;
use serde::Serialize;

use crate::EvalExpireTime;

pub trait SeqValue<V = Vec<u8>> {
    fn seq(&self) -> u64;
    fn value(&self) -> Option<&V>;
    fn meta(&self) -> Option<&KVMeta>;

    /// Return the expire time in millisecond since 1970.
    fn get_expire_at_ms(&self) -> Option<u64> {
        if let Some(meta) = self.meta() {
            meta.get_expire_at_ms()
        } else {
            None
        }
    }

    /// Evaluate and returns the absolute expire time in millisecond since 1970.
    fn eval_expire_at_ms(&self) -> u64 {
        self.meta().eval_expire_at_ms()
    }

    /// Return true if the record is expired.
    fn is_expired(&self, now_ms: u64) -> bool {
        self.eval_expire_at_ms() < now_ms
    }
}

/// The meta data of a record in kv
#[derive(Serialize, Deserialize, Debug, Default, Clone, Eq, PartialEq)]
pub struct KVMeta {
    /// expiration time in second since 1970
    pub(crate) expire_at: Option<u64>,
}

impl KVMeta {
    /// Create a new KVMeta
    pub fn new(expire_at: Option<u64>) -> Self {
        Self { expire_at }
    }

    /// Create a KVMeta with a absolute expiration time in second since 1970-01-01.
    pub fn new_expire(expire_at: u64) -> Self {
        Self {
            expire_at: Some(expire_at),
        }
    }

    /// Returns expire time in millisecond since 1970.
    pub fn get_expire_at_ms(&self) -> Option<u64> {
        self.expire_at.map(|t| t * 1000)
    }
}

impl EvalExpireTime for KVMeta {
    fn eval_expire_at_ms(&self) -> u64 {
        match self.expire_at {
            None => u64::MAX,
            Some(exp_at_sec) => exp_at_sec * 1000,
        }
    }
}

/// Some value bound with a seq number.
///
/// [`SeqV`] is the meta-service API level generic value.
/// Meta-service application uses this type to interact with meta-service.
///
/// Inside the meta-service, the value is stored in the form of `Marked`, which could be a tombstone.
/// A `Marked::TombStone` is converted to `None::<SeqV>` and a `Marked::Normal` is converted to `Some::<SeqV>`.
///
/// A `Marked::TombStone` also has an `internal_seq`, representing the freshness of the tombstone.
/// `internal_seq` will be discarded when `Marked::TombStone` is converted to `None::<SeqV>`.
#[derive(Serialize, Deserialize, Default, Clone, Eq, PartialEq)]
pub struct SeqV<T = Vec<u8>> {
    pub seq: u64,
    pub meta: Option<KVMeta>,
    pub data: T,
}

impl<V> SeqValue<V> for SeqV<V> {
    fn seq(&self) -> u64 {
        self.seq
    }

    fn value(&self) -> Option<&V> {
        Some(&self.data)
    }

    fn meta(&self) -> Option<&KVMeta> {
        self.meta.as_ref()
    }
}

impl<V> SeqValue<V> for Option<SeqV<V>> {
    fn seq(&self) -> u64 {
        self.as_ref().map(|v| v.seq()).unwrap_or(0)
    }

    fn value(&self) -> Option<&V> {
        self.as_ref().and_then(|v| v.value())
    }

    fn meta(&self) -> Option<&KVMeta> {
        self.as_ref().and_then(|v| v.meta())
    }
}

impl<T: std::fmt::Debug> std::fmt::Debug for SeqV<T> {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let mut de = f.debug_struct("SeqV");
        de.field("seq", &self.seq);
        de.field("meta", &self.meta);
        de.field("data", &"[binary]");

        de.finish()
    }
}

pub trait IntoSeqV<T> {
    type Error;
    fn into_seqv(self) -> Result<SeqV<T>, Self::Error>;
}

impl<T, V> IntoSeqV<T> for SeqV<V>
where V: TryInto<T>
{
    type Error = <V as TryInto<T>>::Error;

    fn into_seqv(self) -> Result<SeqV<T>, Self::Error> {
        Ok(SeqV {
            seq: self.seq,
            meta: self.meta,
            data: self.data.try_into()?,
        })
    }
}

impl<T> From<(u64, T)> for SeqV<T> {
    fn from((seq, data): (u64, T)) -> Self {
        Self {
            seq,
            meta: None,
            data,
        }
    }
}

impl<T> SeqV<Option<T>> {
    pub const fn empty() -> Self {
        Self {
            seq: 0,
            meta: None,
            data: None,
        }
    }
}

impl<T> SeqV<T> {
    pub fn new(seq: u64, data: T) -> Self {
        Self {
            seq,
            meta: None,
            data,
        }
    }

    pub fn from_tuple((seq, data): (u64, T)) -> Self {
        Self {
            seq,
            meta: None,
            data,
        }
    }

    /// Create a timestamp in second for expiration control used in SeqV
    pub fn now_sec() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    /// Create a timestamp in millisecond for expiration control used in SeqV
    pub fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    pub fn with_meta(seq: u64, meta: Option<KVMeta>, data: T) -> Self {
        Self { seq, meta, data }
    }

    #[must_use]
    pub fn set_seq(mut self, seq: u64) -> SeqV<T> {
        self.seq = seq;
        self
    }

    #[must_use]
    pub fn set_meta(mut self, m: Option<KVMeta>) -> SeqV<T> {
        self.meta = m;
        self
    }

    #[must_use]
    pub fn set_value(mut self, v: T) -> SeqV<T> {
        self.data = v;
        self
    }
}

// TODO(1): test SeqValue for SeqV and Option<SeqV>
