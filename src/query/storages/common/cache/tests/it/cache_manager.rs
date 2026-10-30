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

use std::collections::HashMap;
use std::sync::Arc;

use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::Pool;
use databend_storages_common_cache::Unit;
use databend_storages_common_cache::cache_manager::CacheManager;
use databend_storages_common_cache::cache_manager::CacheRow;
use databend_storages_common_cache::store::KindStore;
use databend_storages_common_cache::store::LegacyCapacity;
use databend_storages_common_cache::store::LegacyStore;

use crate::fakes::Item;
use crate::fakes::Live;
use crate::fakes::shared_store;

fn row<'a>(rows: &'a [CacheRow], name: &str) -> &'a CacheRow {
    rows.iter().find(|r| r.name == name).unwrap()
}

#[test]
fn test_rows_cover_kinds_and_pools() {
    let store = shared_store(1000, 2000, 0);
    let manager = CacheManager::new(store, HashMap::new());
    let live = Live::default();
    let cache = manager.mem_cache::<Item>(CacheKind::VirtualColumnMeta);
    let held = cache.put("a", live.item(100));
    cache.put("b", live.item(50));
    cache.evict("a");
    for _ in 0..3 {
        cache.lookup("b");
    }
    cache.lookup("missing");

    let rows = manager.rows();
    assert_eq!(rows.len(), CacheKind::ALL.len() + Pool::ALL.len());

    let kind = row(&rows, "memory_cache_virtual_column_file_meta_data");
    assert_eq!(kind.kind, Some(CacheKind::VirtualColumnMeta));
    assert_eq!(
        (kind.num_items, kind.size, kind.in_cache_size),
        (1, 150, 50)
    );
    assert_eq!((kind.capacity, kind.unit), (None, Unit::Bytes));
    assert!(kind.enabled);
    assert!(kind.access >= 4 && kind.hit >= 3 && kind.miss >= 1);

    // Unlimited kind, but its pool has no capacity.
    assert!(!row(&rows, "memory_cache_column_data").enabled);

    let meta = row(&rows, "__pool:meta");
    assert_eq!(
        (meta.capacity, meta.size, meta.in_cache_size),
        (Some(1000), 150, 50)
    );
    assert!(!row(&rows, "__pool:data").enabled);
    drop(held);
}

#[test]
fn test_legacy_rows_have_no_pools() {
    let store: Arc<dyn KindStore> = Arc::new(LegacyStore::new([(
        CacheKind::TableSnapshot,
        LegacyCapacity::Items(10),
    )]));
    let rows = CacheManager::new(store, HashMap::new()).rows();
    assert_eq!(rows.len(), CacheKind::ALL.len());
    let snapshot = row(&rows, "memory_cache_table_snapshot");
    assert_eq!((snapshot.capacity, snapshot.unit), (Some(10), Unit::Count));
    assert!(snapshot.enabled);
    assert!(!row(&rows, "memory_cache_table_statistics").enabled);
}
