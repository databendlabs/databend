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

mod store_bare;
mod store_inner;
mod to_storage_error;

#[cfg(feature = "raft-store-defensive")]
use common_meta_sled_store::openraft::StoreExt;
#[cfg(feature = "raft-store-defensive")]
use common_meta_types::TypeConfig;
pub use store_bare::RaftStoreBare;
pub use store_inner::StoreInner;
pub use to_storage_error::ToStorageError;

/// Implements `RaftStorage` and provides defensive check.
///
/// It is used to discover unexpected invalid data read or write, which is potentially a bug.
#[cfg(feature = "raft-store-defensive")]
pub type RaftStore = StoreExt<TypeConfig, RaftStoreBare>;

#[cfg(not(feature = "raft-store-defensive"))]
pub(crate) type RaftStore = RaftStoreBare;
