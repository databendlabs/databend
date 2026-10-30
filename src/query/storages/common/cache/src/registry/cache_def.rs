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

use crate::CachedItem;
use crate::Disk;
use crate::KindLimit;
use crate::Pool;

/// Definition of one cache kind: what it caches, and how the shared backend and the disk tier
/// treat it. Each kind implements it once, next to its value type in `kinds`, and registers in
/// `CacheRegistry::builtin`.
pub trait CacheDef: 'static {
    type Value: CachedItem;

    /// Name in `system.caches`, metrics, `set_cache_capacity` and `cache.kind_max_bytes`.
    const NAME: &'static str;

    /// Pool of the shared backend.
    const POOL: Pool;

    /// Limit in the shared backend unless `cache.kind_max_bytes` sets one; by default the kind
    /// is bounded only by its pool.
    const SHARED_LIMIT: KindLimit = KindLimit::UNLIMITED;

    /// Disk tier, for kinds that have one.
    const DISK: Option<Disk<Self::Value>> = None;
}
