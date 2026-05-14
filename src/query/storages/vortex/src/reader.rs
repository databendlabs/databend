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

//! Read a Vortex file and return Arrow IPC bytes.
//!
//! The caller (Databend fuse, using arrow 56) passes an opendal Operator and a path.
//! We open the Vortex file via object_store_opendal, scan the requested columns,
//! convert to RecordBatches (arrow 58), serialize to Arrow IPC bytes, and return
//! those bytes to the caller. The caller then deserializes with arrow 56.

use std::sync::Arc;

use arrow_array::RecordBatch;
use futures::TryStreamExt;
use object_store::ObjectStore;
use object_store_opendal::OpendalStore;
use opendal::Operator;
use vortex_array::arrow::IntoArrowArray;
use vortex_array::stream::ArrayStreamExt;
use vortex_file::OpenOptionsSessionExt;
use vortex_file::register_default_encodings;
use vortex_session::VortexSession;

use crate::error::VortexResult;
use crate::error::VortexStorageError;
use crate::schema::record_batches_to_ipc_bytes;

/// Read a Vortex file and return Arrow IPC stream bytes.
///
/// # Arguments
/// * `operator`       - Databend opendal Operator for the storage backend.
/// * `path`           - Path to the `.vortex` file within the operator's namespace.
/// * `projected_cols` - Optional list of column indices to project. `None` reads all columns.
///
/// # Returns
/// Arrow IPC stream bytes (arrow 58 format, readable by arrow 56 on the Databend side).
pub async fn read_vortex_file(
    operator: Operator,
    path: &str,
    projected_cols: Option<Vec<usize>>,
) -> VortexResult<Vec<u8>> {
    // 1. Build a VortexSession with default encodings
    let session = VortexSession::empty();
    register_default_encodings(&session);

    // 2. Wrap opendal Operator as an object_store ObjectStore
    let store: Arc<dyn ObjectStore> = Arc::new(OpendalStore::new(operator));

    // 3. Open the Vortex file via object_store (reads only the footer — O(1) IO)
    let vortex_file = session
        .open_options()
        .open_object_store(&store, path)
        .await
        .map_err(VortexStorageError::Vortex)?;

    // 4. Build the scan, optionally applying column projection
    let mut scan_builder = vortex_file
        .scan()
        .map_err(VortexStorageError::Vortex)?;

    if let Some(cols) = projected_cols {
        scan_builder = scan_builder.with_indices(cols);
    }

    // 5. Execute the scan → stream of Vortex arrays
    let array_stream = scan_builder
        .execute()
        .await
        .map_err(VortexStorageError::Vortex)?;

    // 6. Convert each Vortex array chunk → RecordBatch (arrow 58)
    let batches: Vec<RecordBatch> = array_stream
        .map_err(VortexStorageError::Vortex)
        .and_then(|array| async move {
            array
                .into_arrow_record_batch()
                .map_err(VortexStorageError::Vortex)
        })
        .try_collect()
        .await?;

    if batches.is_empty() {
        return Ok(Vec::new());
    }

    // 7. Serialize RecordBatches → Arrow IPC bytes (readable by arrow 56)
    let schema = batches[0].schema();
    record_batches_to_ipc_bytes(&schema, &batches)
}
