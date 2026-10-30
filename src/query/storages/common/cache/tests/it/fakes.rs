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

use std::borrow::Cow;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;

use databend_common_exception::Result;
use databend_storages_common_cache::CacheKind;
use databend_storages_common_cache::CachedItem;
use databend_storages_common_cache::KindLimit;
use databend_storages_common_cache::MemCache;
use databend_storages_common_cache::PoolCaps;
use databend_storages_common_cache::Reader;
use databend_storages_common_cache::backend::KindStore;
use databend_storages_common_cache::kinds::bloom;
use databend_storages_common_cache::kinds::column;
use databend_storages_common_cache::kinds::segment;
use databend_storages_common_cache::kinds::table;
use databend_storages_common_cache::shared::SharedStore;

/// Kinds the tests store `Item`s in, one or two per pool.
pub fn meta_kind() -> CacheKind {
    CacheKind::of::<table::Snapshot>()
}

pub fn meta2_kind() -> CacheKind {
    CacheKind::of::<segment::Info>()
}

pub fn index_kind() -> CacheKind {
    CacheKind::of::<bloom::Filter>()
}

pub fn data_kind() -> CacheKind {
    CacheKind::of::<column::Data>()
}

/// A cached value of a fixed size that counts how many instances are alive.
pub struct Item {
    pub bytes: usize,
    live: Arc<AtomicUsize>,
}

impl Drop for Item {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::Relaxed);
    }
}

impl CachedItem for Item {
    fn mem_bytes(&self) -> usize {
        self.bytes
    }
}

pub fn cache(store: &Arc<dyn KindStore>, kind: CacheKind) -> MemCache<Item> {
    MemCache::new(store.clone(), kind)
}

#[derive(Clone, Default)]
pub struct Live(Arc<AtomicUsize>);

impl Live {
    pub fn item(&self, bytes: usize) -> Item {
        self.0.fetch_add(1, Ordering::Relaxed);
        Item {
            bytes,
            live: self.0.clone(),
        }
    }

    pub fn count(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
}

/// Reader that produces items of a fixed size and counts reads.
pub struct FakeReader {
    pub live: Live,
    pub bytes: usize,
    pub delay: Duration,
    reads: AtomicUsize,
}

impl FakeReader {
    pub fn new(bytes: usize) -> Self {
        FakeReader {
            live: Live::default(),
            bytes,
            delay: Duration::ZERO,
            reads: AtomicUsize::new(0),
        }
    }

    pub fn slow(bytes: usize, delay: Duration) -> Self {
        FakeReader {
            delay,
            ..Self::new(bytes)
        }
    }

    pub fn reads(&self) -> usize {
        self.reads.load(Ordering::Relaxed)
    }
}

impl Reader for FakeReader {
    type Key = String;
    type Item = Item;

    fn cache_key<'a>(&self, key: &'a String) -> Cow<'a, str> {
        Cow::Borrowed(key)
    }

    async fn read(&self, _key: &String) -> Result<Item> {
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        self.reads.fetch_add(1, Ordering::Relaxed);
        Ok(self.live.item(self.bytes))
    }
}

/// Shared store with the given pool caps; column kinds enabled for testing the Data pool.
pub fn shared_store(meta: usize, index: usize, data: usize) -> Arc<dyn KindStore> {
    Arc::new(SharedStore::new(
        PoolCaps { meta, index, data },
        [
            (CacheKind::of::<column::Data>(), KindLimit::UNLIMITED),
            (CacheKind::of::<column::Array>(), KindLimit::UNLIMITED),
        ],
        4,
    ))
}
