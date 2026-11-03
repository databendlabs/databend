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

//! The disk cache of raw object bytes. `DiskCachedStorage` is what readers use; `DiskCache`
//! holds the pools (`disk_pool`), the writer (`disk_writer`) and the entry files (`disk_file`).
//! The parts are public for their tests and diagnostics, as `backend` is.

mod disk_cache;
mod disk_cached_storage;
pub mod disk_file;
pub mod disk_pool;
pub mod disk_writer;
mod key;

pub use disk_cache::DiskCache;
pub use disk_cache::DiskCacheStats;
pub use disk_cache::DiskKindEvents;
pub use disk_cache::DiskKindStats;
pub use disk_cache::DiskPoolStats;
pub use disk_cache::split_caps;
pub use disk_cached_storage::DiskCachedStorage;
pub use disk_cached_storage::DiskFill;
pub use disk_cached_storage::UnitsRead;
pub use key::DiskKey;
