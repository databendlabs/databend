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

use std::sync::Arc;

use databend_common_base::base::GlobalInstance;
use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::cache_manager::CacheManager;

use crate::io::cache::SnapshotCache;

/// Process-wide caches of fuse tables. Every kind is bound to its value type here.
pub struct FuseCaches {
    pub snapshots: SnapshotCache,
}

impl FuseCaches {
    pub fn new(manager: &CacheManager) -> Arc<Self> {
        Arc::new(FuseCaches {
            snapshots: SnapshotCache::new(
                manager.mem_cache(CacheKind::TableSnapshot),
                manager.mem_cache(CacheKind::TableSnapshotStats),
            ),
        })
    }

    /// Registers the process-wide instance. Called once, right after the cache manager is set up.
    pub fn init(manager: &CacheManager) {
        GlobalInstance::set(Self::new(manager));
    }

    /// Lookups as usual, but values read on a miss are not cached. Writes still go in.
    pub fn read_only(&self) -> Arc<Self> {
        Arc::new(FuseCaches {
            snapshots: self.snapshots.read_only(),
        })
    }

    pub fn instance() -> Arc<Self> {
        GlobalInstance::get()
    }
}
