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

use crate::providers::HybridCache;
use crate::read::cached_reader::CacheReader;
use crate::InMemoryLruCache;
use crate::read::cached_ttl_reader::CachedTTLReader;

pub type HybridCacheReader<T, L> = CacheReader<L, HybridCache<T>>;
pub type InMemoryItemCacheReader<T, L> = CacheReader<L, InMemoryLruCache<T>>;
pub type InMemoryCacheReader<T, L> = CacheReader<L, InMemoryLruCache<T>>;
pub type InMemoryCacheTTLReader<T, L> = CachedTTLReader<L, InMemoryLruCache<T>>;
