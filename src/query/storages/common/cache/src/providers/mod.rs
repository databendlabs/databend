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

pub mod disk_cache;
mod disk_cache_builder;
mod hybrid_cache;
mod memory_cache;

pub use disk_cache::io_result::Error as DiskCacheError;
pub use disk_cache::io_result::Result as DiskCacheResult;
pub use disk_cache::DiskCacheKey;
pub use disk_cache::LruDiskCache;
pub use disk_cache::LruDiskCacheBuilder;
pub use disk_cache::LruDiskCacheHolder;
pub use disk_cache_builder::DiskCacheAccessor;
pub use disk_cache_builder::DiskCacheBuilder;
pub use disk_cache_builder::TableDataCacheKey;
pub use hybrid_cache::HybridCache;
pub use memory_cache::InMemoryLruCache;
