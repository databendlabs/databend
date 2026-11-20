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

use crate::CachedItem;
use crate::KindLimit;
use crate::Pool;

/// Definition of one cache kind: what it caches, and how the shared backend treats it. Each kind implements it once, next to its value type in `kinds`, and registers in
/// `CacheRegistry::builtin`.
pub trait CacheDef: 'static {
    type Value: CachedItem;

    /// Name in `system.caches`, metrics, `set_cache_capacity` and `cache.kind_max_bytes`.
    const NAME: &'static str;

    /// Pool of the shared backend.
    const POOL: Pool;

    /// Limit in the shared backend unless `cache.kind_max_bytes` sets one, and the limit
    /// `set_cache_capacity(<name>, 'default')` restores.
    const SHARED_DEFAULT: KindLimit = KindLimit::UNLIMITED;

    /// Whether concurrent misses on one key share a load (`MemCache::get`). Only for kinds no
    /// writer invalidates, by `evict` or a `put` under the same key: a reader coming after an
    /// invalidation could join a load started before it, and get the old value.
    const SHARE_LOADS: bool = false;

    /// Whether the object bytes this kind is decoded from are kept in the disk cache. Readers
    /// of a kind without it read storage directly, whatever they ask `DiskCachedStorage`.
    const DISK: bool = false;
}
