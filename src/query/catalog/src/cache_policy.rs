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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

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

/// What a statement may still put into the in-memory cache of decoded columns
/// (`memory_cache_table_data`), shared by every scan and thread of the statement on a node.
///
/// A scan larger than the cache cannot stay in it: what it puts in displaces what other
/// statements reuse, and is gone before the statement is over. So a statement may write at most
/// half of the cache's limit, counted as it goes, and a scan expected to read more than that in
/// full ends the statement's writes before it starts. Half, so that a statement under the bar is
/// cached whole in one pass while the other half of the cache keeps what was there. Reads are not
/// affected: what the statement finds in the cache it uses.
///
/// The expectation is an aid, not a requirement: it is charged only for the parts known when
/// a scan is built, and only for the columns the scan reads in full. Where it is missing or
/// low, the running count still bounds what the statement writes.
#[derive(Debug, Default)]
pub struct TableDataCacheWriteBudget {
    expected: AtomicU64,
    written: AtomicU64,
    writes_ended: AtomicBool,
}

/// Why a statement may not write an entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CacheWriteRefusal {
    /// The statement's scans are expected to read more than its allowance.
    Expected,
    /// The statement has written its allowance.
    Written,
}

impl CacheWriteRefusal {
    /// The refusal as a metric label.
    pub fn reason(self) -> &'static str {
        match self {
            CacheWriteRefusal::Expected => "expected_over_allowance",
            CacheWriteRefusal::Written => "written_allowance",
        }
    }
}

impl TableDataCacheWriteBudget {
    /// The share of the cache's limit a statement may fill.
    fn allowance(limit: u64) -> u64 {
        limit / 2
    }

    /// Charges `bytes` a scan is expected to read in full. Past the allowance for `limit`, the
    /// statement writes nothing more.
    pub fn expect(&self, bytes: u64, limit: u64) {
        let expected = self.expected.fetch_add(bytes, Ordering::Relaxed) + bytes;
        if expected > Self::allowance(limit) {
            self.writes_ended.store(true, Ordering::Relaxed);
        }
    }

    /// Whether an entry of `bytes` may be written under `limit`: not once the statement's
    /// writes have ended, nor when it would take the statement past its allowance. Counts the
    /// bytes as written when it may.
    pub fn admit(&self, bytes: u64, limit: u64) -> Result<(), CacheWriteRefusal> {
        if self.writes_ended.load(Ordering::Relaxed) {
            return Err(CacheWriteRefusal::Expected);
        }
        let allowance = Self::allowance(limit);
        let mut written = self.written.load(Ordering::Relaxed);
        loop {
            if written + bytes > allowance {
                return Err(CacheWriteRefusal::Written);
            }
            match self.written.compare_exchange_weak(
                written,
                written + bytes,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(()),
                Err(now) => written = now,
            }
        }
    }

    /// Bytes the statement has written so far.
    pub fn written(&self) -> u64 {
        self.written.load(Ordering::Relaxed)
    }
}
