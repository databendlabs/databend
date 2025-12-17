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
#![feature(associated_type_defaults)]
#![feature(assert_matches)]
#![feature(io_const_error)]
#![allow(clippy::collapsible_if, clippy::let_and_return)]

mod cache;
mod cache_items;
mod caches;
mod manager;
mod providers;
mod read;
mod temp_dir;

pub use cache::*;
pub use caches::*;
pub use manager::CacheManager;
pub use providers::DiskCacheAccessor;
pub use providers::DiskCacheBuilder;
pub use providers::DiskCacheError;
pub use providers::DiskCacheKey;
pub use providers::DiskCacheResult;
pub use providers::HybridCache;
pub use providers::InMemoryLruCache;
pub use providers::LruDiskCache;
pub use providers::LruDiskCacheBuilder;
pub use providers::LruDiskCacheHolder;
pub use providers::TableDataCacheKey;
pub use providers::disk_cache::disk_cache_lru::read_cache_content;
pub use read::*;
pub use temp_dir::*;
