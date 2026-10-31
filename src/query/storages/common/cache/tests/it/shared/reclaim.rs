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

use std::cell::Cell;

use databend_storages_common_cache::Pool;

use crate::fakes::Live;
use crate::fakes::cache;
use crate::fakes::data_kind;
use crate::fakes::index_kind;
use crate::fakes::meta_kind;
use crate::fakes::shared_store;

fn resident(store: &dyn databend_storages_common_cache::backend::KindStore, pool: Pool) -> usize {
    store
        .pool_stats()
        .into_iter()
        .find(|s| s.pool == pool)
        .unwrap()
        .resident
}

#[test]
fn test_shrink_order_and_early_stop() {
    let store = shared_store(10_000, 10_000, 10_000);
    let live = Live::default();
    let meta = cache(&store, meta_kind());
    let index = cache(&store, index_kind());
    let data = cache(&store, data_kind());
    for i in 0..10 {
        meta.put(&i.to_string(), live.item(100));
        index.put(&i.to_string(), live.item(100));
        data.put(&i.to_string(), live.item(100));
    }

    let report = store.shrink_for_pressure(&|| resident(&*store, Pool::Data) == 0);
    assert_eq!(report.freed, 1000);
    assert_eq!(resident(&*store, Pool::Index), 1000);
    assert_eq!(resident(&*store, Pool::Meta), 1000);

    let report = store.shrink_for_pressure(&|| false);
    assert_eq!(report.freed, 2000);
    assert_eq!(live.count(), 0);
}

#[test]
fn test_shrink_skips_held_values() {
    let store = shared_store(10_000, 0, 0);
    let live = Live::default();
    let meta = cache(&store, meta_kind());
    let held = meta.put("held", live.item(100));
    meta.put("free", live.item(50));

    let report = store.shrink_for_pressure(&|| false);
    assert_eq!((report.freed, report.evicted), (50, 1));
    assert!(meta.contains("held"));
    drop(held);
    assert_eq!(store.shrink_for_pressure(&|| false).freed, 100);
}

#[test]
fn test_relieved_is_checked_after_each_batch() {
    let store = shared_store(10_000, 0, 0);
    let live = Live::default();
    let meta = cache(&store, meta_kind());
    for i in 0..100 {
        meta.put(&i.to_string(), live.item(10));
    }
    let checks = Cell::new(0);
    let report = store.shrink_for_pressure(&|| {
        checks.set(checks.get() + 1);
        false
    });
    assert_eq!(report.freed, 1000);
    assert_eq!(report.batches, checks.get());
}
