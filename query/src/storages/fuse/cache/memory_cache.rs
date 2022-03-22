//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;

use common_base::tokio::sync::RwLock;
use common_cache::Count;
use common_cache::DefaultHashBuilder;
use common_cache::LruCache;

use crate::storages::fuse::meta::SegmentInfo;
use crate::storages::fuse::meta::TableSnapshot;

type LaCache<K, V> = LruCache<K, V, DefaultHashBuilder, Count>;
pub type MemoryCache<V> = Arc<RwLock<LaCache<String, Arc<V>>>>;

pub fn new_memory_cache<V>(capacity: u64) -> MemoryCache<V> {
    Arc::new(RwLock::new(LruCache::new(capacity)))
}

pub type SegmentInfoCache = MemoryCache<SegmentInfo>;
pub type TableSnapshotCache = MemoryCache<TableSnapshot>;
