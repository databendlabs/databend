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

//! This mod defines a key space in state machine to store the index of keys with an expiration time.
//!
//! This secondary index is `(expire_time, seq) -> key`, as the key-value's primary index is `key -> (seq, expire_time, value)`.
//! Because `seq` in meta-store is globally unique, it may be used to identify every update to every key.

use std::fmt::Display;
use std::fmt::Formatter;
use std::time::Duration;

use display_more::DisplayUnixTimeStampExt;
use map_api::MapKey;

/// The identifier of the index for kv with expiration.
///
/// Encoding to `exp-/<timestamp>/<primary_key_seq>`.
/// The encoded value is `<ver=1>null<string_key>`. The `null` is json string for empty meta
#[derive(
    Default,
    Debug,
    Clone,
    Copy,
    serde::Serialize,
    serde::Deserialize,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
)]
pub struct ExpireKey {
    /// The time in millisecond when a key will be expired.
    pub time_ms: u64,

    /// The `seq` of the value when the key is written.
    ///
    /// The `seq` of value is globally unique in meta-store.
    pub seq: u64,
}

impl MapKey for ExpireKey {
    type V = String;
}

impl ExpireKey {
    pub fn new(time_ms: u64, seq: u64) -> Self {
        Self { time_ms, seq }
    }

    /// Return true if the provided time in millisecond is expired.
    ///
    /// NOTE that `time_ms` equal to `self.time_ms` is not considered expired.
    pub fn is_expired(&self, time_ms: u64) -> bool {
        time_ms > self.time_ms
    }
}

impl Display for ExpireKey {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "{}={}",
            Duration::from_millis(self.time_ms).display_unix_timestamp_short(),
            self.seq
        )
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_expire_key_display() -> anyhow::Result<()> {
        let ms = 1666670258202;
        let k = ExpireKey::new(ms, 1000);
        assert_eq!("2022-10-25T03:57:38.202=1000", format!("{}", k));

        Ok(())
    }
}
