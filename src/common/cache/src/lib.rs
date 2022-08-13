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

#[cfg(feature = "heapsize")]
#[cfg(not(target_os = "macos"))]
extern crate heapsize_;

mod cache;
mod disk_cache;
mod meter;

pub use cache::lru::LruCache;
pub use cache::Cache;
pub use disk_cache::result::Error as DiskCacheError;
pub use disk_cache::result::Result as DiskCacheResult;
pub use disk_cache::DiskCache;
pub use disk_cache::LruDiskCache;
pub use meter::bytes_meter::BytesMeter;
pub use meter::count_meter::Count;
pub use meter::count_meter::CountableMeter;
pub use meter::file_meter::FileSize;
#[cfg(feature = "heapsize")]
#[cfg(not(target_os = "macos"))]
pub use meter::heap_meter::HeapSize;
pub use meter::Meter;
pub use ritelinked::DefaultHashBuilder;
