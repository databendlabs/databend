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
#![feature(io_const_error)]
#![allow(clippy::collapsible_if, clippy::let_and_return)]

//! Process-wide caches. `registry` lists the cache kinds; `CacheManager` builds them on the
//! legacy backend (`legacy`, one LRU per kind) or the shared one (`shared`, S3-FIFO pools),
//! with optional disk tiers (`disk`). Readers get a typed `MemCache` per kind (`handle`) and
//! load through `read`. `local` holds LRU caches owned by a single component.

mod accessor;
pub mod backend;
pub mod cache_manager;
mod disk;
mod handle;
mod items;
pub mod legacy;
mod local;
mod manager;
mod read;
mod registry;
pub mod shared;
mod temp_dir;

pub use accessor::CacheAccessor;
pub use backend::KindLimit;
pub use backend::Unit;
pub use disk::*;
pub use handle::MemCache;
pub use handle::cache_ref::CacheRef;
pub use handle::cache_ref::Reservation;
pub use handle::cache_ref::Usage;
pub use items::*;
pub use local::CacheValue;
pub use local::InMemoryLruCache;
pub use manager::CacheManager;
pub use read::Reader;
pub use read::SingleFlight;
pub use read::TTLReader;
pub use read::TTLValue;
pub use registry::CacheKind;
pub use registry::Pool;
pub use shared::PoolCaps;
pub use temp_dir::*;
