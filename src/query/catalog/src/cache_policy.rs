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

//! What a read may do with the caches it goes through: the memory caches of decoded values and
//! the disk cache of object bytes, each on its own.
//!
//! A statement has a policy (`TableContextCachePolicy`), and so may a call: readers apply the
//! stricter of the two, `CachePolicy::at_most`. Maintenance statements bypass the disk cache,
//! which serves queries; background reads look the caches up without filling them.

use serde::Deserialize;
use serde::Serialize;

/// What a read may do with one cache.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum CacheFill {
    /// Look the cache up; put what is read on a miss.
    #[default]
    ReadWrite,
    /// Look the cache up; do not put what is read.
    ReadOnly,
    /// Neither look the cache up nor put anything.
    Bypass,
}

impl CacheFill {
    pub fn reads(self) -> bool {
        self != CacheFill::Bypass
    }

    pub fn writes(self) -> bool {
        self == CacheFill::ReadWrite
    }

    /// What a read may do under both `self` and `other`.
    pub fn at_most(self, other: CacheFill) -> CacheFill {
        match (self, other) {
            (CacheFill::Bypass, _) | (_, CacheFill::Bypass) => CacheFill::Bypass,
            (CacheFill::ReadOnly, _) | (_, CacheFill::ReadOnly) => CacheFill::ReadOnly,
            _ => CacheFill::ReadWrite,
        }
    }
}

/// What a read may do with the memory caches and with the disk cache.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CachePolicy {
    pub memory: CacheFill,
    pub disk: CacheFill,
}

impl CachePolicy {
    /// Queries and DML: both caches are read and filled.
    pub const DEFAULT: CachePolicy = CachePolicy {
        memory: CacheFill::ReadWrite,
        disk: CacheFill::ReadWrite,
    };

    /// Background reads, as the block reads of compaction: both caches are read, neither is
    /// filled, so what queries cached stays.
    pub const READ_ONLY: CachePolicy = CachePolicy {
        memory: CacheFill::ReadOnly,
        disk: CacheFill::ReadOnly,
    };

    /// Maintenance statements (ANALYZE, compaction, recluster, refreshes, vacuum, purge): the
    /// memory caches as usual, the disk cache not at all.
    pub const MAINTENANCE: CachePolicy = CachePolicy {
        memory: CacheFill::ReadWrite,
        disk: CacheFill::Bypass,
    };

    /// Reads of storage itself, as recovery and checks: no cache at all.
    pub const BYPASS: CachePolicy = CachePolicy {
        memory: CacheFill::Bypass,
        disk: CacheFill::Bypass,
    };

    /// What a read may do under both `self` and `other`, cache by cache.
    pub fn at_most(self, other: CachePolicy) -> CachePolicy {
        CachePolicy {
            memory: self.memory.at_most(other.memory),
            disk: self.disk.at_most(other.disk),
        }
    }
}
