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

//! A read cache for small, hot meta-service key ranges.
//!
//! Every query reads its user and the global settings from meta-service. With a TTL set, the
//! result of such a read is kept in memory and answers the same read until it is that old.
//!
//! - `registry` lists the cached ranges; it is the only part that knows what a range holds.
//! - [`CachedKvApi`] wraps a `KVApi`, so the managers on top of it do not know about the cache.
//! - A TTL of zero, the initial value, answers nothing from memory.
//!
//! Consistency:
//!
//! - A change made by another process is seen at most one TTL later.
//! - A write through [`CachedKvApi`] drops the results of its range, so this process reads its
//!   own writes at once. A read that started before the write does not store what it loaded.
//! - An absent user is not kept, so a user created elsewhere is found at once.
//! - Reads followed by a sequence-conditioned write must not use [`CachedKvApi::reading`].

mod cache;
mod cached_kv_api;
mod registry;
mod ttl_cache;

pub use cache::MetaReadCache;
pub use cache::RangeStats;
pub use cached_kv_api::CachedKvApi;
