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
use std::fmt::Formatter;
use std::ops::Deref;
use std::ops::DerefMut;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use serde::Deserialize;
use serde::Serialize;

use crate::KVMeta;
use crate::SeqValue;

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

impl<T> Deref for SeqV<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T> DerefMut for SeqV<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl<V> SeqValue<V> for SeqV<V> {
    fn seq(&self) -> u64 {
        self.seq
    }

    fn value(&self) -> Option<&V> {
        Some(&self.data)
    }

    fn into_value(self) -> Option<V> {
        Some(self.data)
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

    fn into_value(self) -> Option<V> {
        self.map(|v| v.data)
    }

    fn meta(&self) -> Option<&KVMeta> {
        self.as_ref().and_then(|v| v.meta())
    }
}

impl<T: fmt::Debug> fmt::Debug for SeqV<T> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let mut de = f.debug_struct("SeqV");
        de.field("seq", &self.seq);
        de.field("meta", &self.meta);
        de.field("data", &"[binary]");

        de.finish()
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

    /// Convert data to type U and leave seq and meta unchanged.
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> SeqV<U> {
        SeqV {
            seq: self.seq,
            meta: self.meta,
            data: f(self.data),
        }
    }

    /// Try to convert data to type U and leave seq and meta unchanged.
    /// `f` returns an error if the conversion fails.
    pub fn try_map<U, E>(self, f: impl FnOnce(T) -> Result<U, E>) -> Result<SeqV<U>, E> {
        Ok(SeqV {
            seq: self.seq,
            meta: self.meta,
            data: f(self.data)?,
        })
    }
}
