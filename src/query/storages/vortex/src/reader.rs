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
//! The caller passes an opendal Operator and a path. We open the Vortex file via
//! object_store_opendal, scan the requested columns, convert to RecordBatches,
//! serialize to Arrow IPC bytes, and return those bytes to the caller.
//!
//! Statistics-based pruning is handled entirely by Fuse's BlockMeta — we never
//! read per-column statistics from the Vortex file itself.

use std::sync::Arc;

use arrow_array::RecordBatch;
use futures::TryStreamExt;
use object_store::ObjectStore;
use object_store_opendal::OpendalStore;
use opendal::Operator;
use vortex_array::ExecutionCtx;
use vortex_array::arrow::ArrowArrayExecutor;
use vortex_array::stream::ArrayStreamExt;
use vortex_file::OpenOptionsSessionExt;
use vortex_file::VortexOpenOptions;
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
/// * `projected_cols` - Optional column indices (0-based) to project. `None` reads all columns.
///
/// # Returns
/// Arrow IPC stream bytes, readable by Databend's arrow IPC reader.
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

    // 3. Open the Vortex file via object_store integration.
    //    open_object_store reads only the footer (O(1) IO), not the data.
    let vortex_file = VortexOpenOptions::new(session.clone())
        .open_object_store(&store, path)
        .await
        .map_err(VortexStorageError::Vortex)?;

    // 4. Build the scan with optional column projection
    let mut scan_builder = vortex_file.scan();
    if let Some(indices) = projected_cols {
        scan_builder = scan_builder.with_indices(indices);
    }

    // 5. Execute the scan → stream of Vortex arrays
    let array_stream = scan_builder
        .execute()
        .await
        .map_err(VortexStorageError::Vortex)?;

    // 6. Derive the Arrow schema from the Vortex DType
    let dtype = array_stream.dtype().clone();
    let arrow_schema = dtype
        .to_arrow_schema()
        .map_err(VortexStorageError::Vortex)?;

    // 7. Collect all arrays from the stream
    let arrays: Vec<_> = array_stream
        .map_err(VortexStorageError::Vortex)
        .try_collect()
        .await?;

    if arrays.is_empty() {
        return Ok(Vec::new());
    }

    // 8. Convert each Vortex array → RecordBatch via ArrowArrayExecutor
    let mut ctx = ExecutionCtx::new(session);
    let mut batches: Vec<RecordBatch> = Vec::with_capacity(arrays.len());
    for array in arrays {
        let batch = array
            .execute_record_batch(&arrow_schema, &mut ctx)
            .map_err(VortexStorageError::Vortex)?;
        batches.push(batch);
    }

    // 9. Serialize RecordBatches → Arrow IPC bytes
    record_batches_to_ipc_bytes(&arrow_schema, &batches)
}
