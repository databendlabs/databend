// Copyright 2026 Datafuse Labs
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

use bytes::Bytes;
use databend_common_config::CacheConfig;
use databend_common_config::DiskCacheKeyReloadPolicy;
use databend_common_exception::Result;

use crate::DiskCodec;

/// The disk tier of a kind, part of its `CacheDef`. Only values with a `DiskCodec` can have one.
pub struct Disk<V> {
    pub info: DiskInfo,
    pub codec: Codec<V>,
}

impl<V: DiskCodec> Disk<V> {
    /// A tier in `dir`, of `bytes(config)` bytes, keeping its keys on restart if their files
    /// still exist (`DiskCacheKeyReloadPolicy::Fuzzy`).
    pub const fn new(dir: &'static str, bytes: fn(&CacheConfig) -> u64) -> Self {
        Disk {
            info: DiskInfo {
                dir,
                bytes,
                reload_policy: |_| DiskCacheKeyReloadPolicy::Fuzzy,
            },
            codec: Codec::of(),
        }
    }
}

impl<V> Disk<V> {
    pub const fn reload_policy(
        mut self,
        reload_policy: fn(&CacheConfig) -> DiskCacheKeyReloadPolicy,
    ) -> Self {
        self.info.reload_policy = reload_policy;
        self
    }
}

/// The part of a `Disk` that does not depend on the value type, kept by the registry.
#[derive(Clone, Copy, Debug)]
pub struct DiskInfo {
    /// Directory under `<disk cache path>/<tenant>/`. Kinds keep their pre-refactor directory,
    /// so a restart reuses its files.
    pub dir: &'static str,
    /// Capacity in bytes; at or below 1 KiB the tier is off.
    pub bytes: fn(&CacheConfig) -> u64,
    pub reload_policy: fn(&CacheConfig) -> DiskCacheKeyReloadPolicy,
}

/// How a value is written to and read from its disk tier.
pub struct Codec<V> {
    encode: fn(&V) -> Result<Vec<u8>>,
    decode: fn(Bytes) -> Result<V>,
}

impl<V: DiskCodec> Codec<V> {
    pub const fn of() -> Self {
        Codec {
            encode: V::encode,
            decode: V::decode,
        }
    }
}

impl<V> Codec<V> {
    pub fn encode(&self, value: &V) -> Result<Vec<u8>> {
        (self.encode)(value)
    }

    pub fn decode(&self, bytes: Bytes) -> Result<V> {
        (self.decode)(bytes)
    }
}

impl<V> Clone for Codec<V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<V> Copy for Codec<V> {}
