// Copyright 2021 Datafuse Labs.
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

#[macro_use]
extern crate log;
#[cfg(feature = "heapsize")]
extern crate heapsize_;

#[cfg(test)]
mod disk_cache_test;

mod cache;
#[allow(dead_code)]
mod disk_cache;
mod meter;

pub use cache::lru::LruCache;
pub use cache::Cache;
pub use disk_cache::result::Error as DiskCacheError;
pub use disk_cache::result::Result as DiskCacheResult;
pub use disk_cache::DiskCache;
pub use disk_cache::LruDiskCache;
pub use meter::count_meter::Count;
pub use meter::count_meter::CountableMeter;
pub use meter::file_meter::FileSize;
#[cfg(feature = "heapsize")]
pub use meter::heap_meter::HeapSize;
pub use meter::Meter;
