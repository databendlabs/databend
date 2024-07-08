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

#![feature(write_all_vectored)]

mod cache;
mod providers;
mod read;

pub use cache::CacheAccessor;
pub use cache::CacheAccessorExt;
pub use cache::Named;
pub use cache::NamedCache;
pub use databend_common_cache::CountableMeter;
pub use providers::DiskCacheError;
pub use providers::DiskCacheKey;
pub use providers::DiskCacheResult;
pub use providers::InMemoryBytesCacheHolder;
pub use providers::InMemoryCacheBuilder;
pub use providers::InMemoryItemCacheHolder;
pub use providers::LruDiskCache;
pub use providers::LruDiskCacheBuilder;
pub use providers::LruDiskCacheHolder;
pub use providers::TableDataCache;
pub use providers::TableDataCacheBuilder;
pub use providers::TableDataCacheKey;
pub use providers::TABLE_DATA_CACHE_NAME;
pub use read::CacheKey;
pub use read::CachedReader;
pub use read::InMemoryBytesCacheReader;
pub use read::InMemoryCacheReader;
pub use read::InMemoryItemCacheReader;
pub use read::LoadParams;
pub use read::Loader;
