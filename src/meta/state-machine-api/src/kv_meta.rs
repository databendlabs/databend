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

use display_more::DisplayUnixTimeStampExt;
use map_api::Expirable;
use serde::Deserialize;
use serde::Serialize;

use crate::time_util::flexible_timestamp_to_duration;

/// The metadata of a record in kv
#[derive(Serialize, Deserialize, Debug, Default, Clone, Eq, PartialEq)]
pub struct KVMeta {
    /// Expiration time in **seconds or milliseconds** since Unix epoch (1970-01-01).
    ///
    /// The interpretation depends on the magnitude of the value:
    /// - Values > `100_000_000_000`: treated as milliseconds since epoch
    /// - Values ≤ `100_000_000_000`: treated as seconds since epoch
    ///
    /// See [`adaptable_timestamp_to_duration`]
    pub expire_at: Option<u64>,
}

impl KVMeta {
    /// Create a new KVMeta.
    ///
    /// `expire_at_sec_or_ms` can be either seconds or milliseconds.
    pub fn new(expire_at_sec_or_ms: Option<u64>) -> Self {
        Self {
            expire_at: expire_at_sec_or_ms,
        }
    }

    /// Create a KVMeta with an absolute expiration time since 1970-01-01.
    ///
    /// `expires_at_sec_or_ms` can be either seconds or milliseconds.
    pub fn new_expires_at(expires_at_sec_or_ms: u64) -> Self {
        Self {
            expire_at: Some(expires_at_sec_or_ms),
        }
    }

    /// Returns expire time in millisecond since 1970.
    pub fn get_expire_at_ms(&self) -> Option<u64> {
        self.expires_at_duration_opt().map(|d| d.as_millis() as u64)
    }

    pub fn expires_at_sec_opt(&self) -> Option<u64> {
        self.expire_at
            .map(|x| flexible_timestamp_to_duration(x).as_secs())
    }

    /// Return the absolute expire time in since 1970-01-01 00:00:00.
    pub fn expires_at_duration_opt(&self) -> Option<Duration> {
        self.expire_at.map(flexible_timestamp_to_duration)
    }
}

impl fmt::Display for KVMeta {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.expires_at_duration_opt() {
            Some(expire_at) => write!(
                f,
                "(expires_at: {})",
                expire_at.display_unix_timestamp_short()
            ),
            None => write!(f, "()"),
        }
    }
}

impl Expirable for KVMeta {
    fn expires_at_ms_opt(&self) -> Option<u64> {
        self.expires_at_duration_opt().map(|d| d.as_millis() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kv_meta_expirable_trait() {
        let kv_meta = KVMeta::new(Some(100_000_000_000));
        assert_eq!(kv_meta.expires_at_ms_opt(), Some(100_000_000_000_000));

        let kv_meta = KVMeta::new(Some(100_000_000_001));
        assert_eq!(kv_meta.expires_at_ms_opt(), Some(100_000_000_001));
    }

    #[test]
    fn test_kv_meta_method_get_expire_at_ms() {
        let kv_meta = KVMeta::new(Some(100_000_000_000));
        assert_eq!(kv_meta.get_expire_at_ms(), Some(100_000_000_000_000));

        let kv_meta = KVMeta::new(Some(100_000_000_001));
        assert_eq!(kv_meta.get_expire_at_ms(), Some(100_000_000_001));
    }

    #[test]
    fn test_kv_meta_method_expires_at_duration() {
        let kv_meta = KVMeta::new(Some(100_000_000_000));
        assert_eq!(
            kv_meta.expires_at_duration_opt(),
            Some(Duration::from_secs(100_000_000_000))
        );

        let kv_meta = KVMeta::new(Some(100_000_000_001));
        assert_eq!(
            kv_meta.expires_at_duration_opt(),
            Some(Duration::from_millis(100_000_000_001))
        );
    }

    #[test]
    fn test_kv_meta_display() {
        let kv_meta = KVMeta::new(Some(100_000_000_000));
        assert_eq!(kv_meta.to_string(), "(expires_at: 5138-11-16T09:46:40.000)");

        let kv_meta = KVMeta::new(Some(100_000_000_001));
        assert_eq!(kv_meta.to_string(), "(expires_at: 1973-03-03T09:46:40.001)");

        let kv_meta = KVMeta::new(None);
        assert_eq!(kv_meta.to_string(), "()");
    }
}
