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

mod builder;
mod cache;
mod codec;
mod disk_def;
mod disk_tiers;
mod key;
mod lru;

pub use builder::DiskCacheAccessor;
pub use builder::DiskCacheBuilder;
pub use builder::TableDataCacheKey;
pub use cache::io_result::Error as DiskCacheError;
pub use cache::io_result::Result as DiskCacheResult;
pub use cache::*;
pub use codec::DiskCodec;
pub use disk_def::Codec;
pub use disk_def::Disk;
pub use disk_def::DiskInfo;
pub(crate) use disk_tiers::disk_tiers;
pub use key::DiskCacheKey;
pub use lru::LruDiskCache;
pub use lru::LruDiskCacheBuilder;
pub use lru::LruDiskCacheHolder;
pub use lru::read_cache_content;
