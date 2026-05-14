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

//! IO bridge: opendal::Operator → object_store::ObjectStore → VortexReadAt
//!
//! object_store_opendal provides OpendalStore which implements ObjectStore on top of
//! opendal::Operator. vortex-io's ObjectStoreReadAt then wraps that into VortexReadAt.
//! This gives us a zero-copy path from Databend's opendal-based storage to Vortex's IO layer.

use std::sync::Arc;

use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;
use object_store_opendal::OpendalStore;
use opendal::Operator;
use vortex_io::object_store::ObjectStoreReadAt;
use vortex_io::runtime::Handle;

/// Build a VortexReadAt from a Databend opendal Operator and a file path.
///
/// The chain is:
///   opendal::Operator
///     → object_store_opendal::OpendalStore (implements ObjectStore)
///       → vortex_io::ObjectStoreReadAt (implements VortexReadAt)
pub fn make_vortex_reader(
    operator: Operator,
    path: &str,
    handle: Handle,
) -> ObjectStoreReadAt {
    let store: Arc<dyn ObjectStore> = Arc::new(OpendalStore::new(operator));
    let object_path = ObjectPath::from(path);
    ObjectStoreReadAt::new(store, object_path, handle)
}

/// Build a VortexWrite sink backed by an in-memory Vec<u8>.
/// The caller is responsible for extracting the bytes after writing.
pub fn make_memory_writer() -> Vec<u8> {
    Vec::new()
}
