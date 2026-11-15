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

//! A process-wide read cache for small, hot meta-service prefixes.
//!
//! Every query reads the user, the global settings and the cluster nodes from meta-service. These
//! prefixes are small and rarely change, so they are mirrored locally by one watch stream each
//! (see `databend-meta-plugin-cache`), and reads are answered from memory.
//!
//! Consistency rules, all enforced here so that callers do not need to know about the cache:
//!
//! - A read is answered from memory only if the prefix is ready, not suspected stale, every write
//!   this process made under the read range has been observed, and the result is non-empty.
//!   Otherwise it falls back to an ordinary meta-service read.
//! - Absent keys and empty lists are answered from memory only for global settings, where
//!   "absent" means "default"; for users and cluster nodes they fall back.
//! - Writes through [`CachedKvApi::fencing`] record fences: a later read of the same key or range
//!   bypasses the cache until the watch stream delivers that write (read-your-writes per process).
//! - A background verifier compares each prefix with a leader read every `verify_interval`,
//!   all prefixes concurrently. A mismatch is re-checked one second later; a second mismatch in a
//!   row, a fence not observed within one interval, or a verification read that fails or takes
//!   over 3 s marks the prefix suspect (all reads fall back) and forces the watch stream to
//!   re-fetch. This bounds staleness to about `verify_interval` + 3 s when a stream silently
//!   stops, e.g. a half-open connection or a lagging follower.
//!
//! Reads that are followed by a sequence-conditioned write must not use [`CachedKvApi::reading`].

mod cache;
mod cached_kv_api;
mod cached_prefix;
mod prefix_cache;
mod verifier;

pub use cache::MetaReadCache;
pub use cached_kv_api::CachedKvApi;
pub use cached_prefix::CachedPrefix;
pub use prefix_cache::PrefixStats;
