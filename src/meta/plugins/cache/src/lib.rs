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

#![allow(clippy::uninlined_format_args)]

//! A distributed cache implementation that maintains a local view of data stored in a meta-service.
//!
//! Features:
//! - Automatic synchronization with meta-service
//! - Safe concurrent access
//! - Event-based updates
//! - Safe reconnection
//! - Consistent initialization
//! - Sequence-based consistency
//!
//! # Example
//!
//! ```rust
//! use databend_meta_cache::Cache;
//! use databend_meta_client::MetaClient;
//!
//! let meta_client = MetaClient::new("127.0.0.1:9191");
//! let cache = Cache::new("my_prefix", meta_client).await?;
//!
//! // Get a value
//! let value = cache.try_get("key").await?;
//!
//! // List directory contents
//! let entries = cache.try_list_dir("dir").await?;
//! ```
//!
//! # Cache Key Structure
//!
//! ```text
//! <prefix>/foo
//! <prefix>/..
//! <prefix>/..
//! ```
//!
//! - `<prefix>` is a user-defined string to identify a cache instance.
//!
//! # Initialization Process
//!
//! When a [`Cache`] is created, it:
//! 1. Creates a new instance with the specified prefix
//! 2. Spawns a background task to watch for changes
//! 3. Establishes a watch stream with initial flush
//! 4. Fetches and processes initial data
//! 5. Maintains continuous synchronization
//!
//! # Cache Update Process
//!
//! The cache update process works as follows:
//! 1. Watcher monitors the watch stream for changes
//! 2. On receiving an event, it applies the update atomically
//! 3. Updates are applied based on event type (insert/update/delete)
//! 4. Sequence numbers are tracked for consistency
//!
//! # Error Handling
//!
//! The cache handles connection errors automatically with exponential backoff.
//! If the connection is lost, it will attempt to reconnect with increasing delays.
//!
//! ```text
//! | cache +----> spawn()-. (1)
//! |       + o-.          |
//! |       |   |          |
//! |       |   |          v                                watch Stream
//! |       |   `------->o KV-Change-Watcher (task) <---------------------------.
//! |       |   cancel               |                                          |
//! |       |                        |                                          |
//! |       |   In memory BTree      |                                          |
//! |       +-+ <prefix>/foo    <--+ |                                          |
//! |         | <prefix>/...    <--+-+                                          |
//! |         | <prefix>/...    <--+ |                                          |
//! |         |                      |                                          |
//! |         + last_seq: u64   <----'                    Meta-Service          |
//! |                                                     <prefix>/foo     --+  |
//! |                                                 .-> <prefix>/...     --+--'
//! |                                                 |   <prefix>/...     --'
//! |                                                 |
//! |                                ... -------------'
//! |                                    Update by other threads
//! ```

mod cache;
mod meta_cache_types;
pub mod meta_client_source;

pub use cache::Cache;
