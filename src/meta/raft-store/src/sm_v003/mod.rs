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

#[allow(clippy::module_inception)]
mod sm_v003;
mod sm_v003_kv_api;
mod snapshot_store_error;
mod snapshot_store_v003;
mod writer_v003;

pub mod adapter;
pub mod open_snapshot;
pub mod received;
pub mod receiver_v003;
pub mod snapshot_loader;
pub mod write_entry;
pub mod writer_stat;

#[cfg(test)]
mod compact_immutable_levels_test;
#[cfg(test)]
mod compact_with_db_test;
#[cfg(test)]
mod sm_v003_test;

pub use sm_v003::SMV003;
pub use snapshot_store_error::SnapshotStoreError;
pub use snapshot_store_v003::SnapshotStoreV003;
pub use snapshot_store_v003::SnapshotStoreV004;
pub use write_entry::WriteEntry;
pub use writer_v003::WriterV003;
