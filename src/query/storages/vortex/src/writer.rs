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

//! Write a Vortex file from Arrow IPC bytes.
//!
//! The caller serializes its DataBlock into Arrow IPC bytes and passes them here.
//! We deserialize, convert to Vortex arrays, and write a self-contained .vortex file.
//!
//! Column statistics are NOT written into the Vortex file — Fuse's own BlockMeta
//! already carries that information in the segment metadata.

use arrow_array::RecordBatch;
use vortex_array::ArrayRef;
use vortex_array::arrow::FromArrowArray;
use vortex_array::stream::ArrayStreamAdapter;
use vortex_file::VortexWriteOptions;
use vortex_file::register_default_encodings;
use vortex_session::VortexSession;

use crate::error::VortexResult;
use crate::error::VortexStorageError;
use crate::schema::ipc_bytes_to_record_batches;

/// Write Arrow IPC bytes as a Vortex file into `out`.
///
/// # Arguments
/// * `ipc_bytes` - Arrow IPC stream bytes produced by Databend.
/// * `out`       - Output buffer; will contain a complete `.vortex` file on success.
///
/// # Returns
/// Number of rows written.
pub async fn write_vortex_file(ipc_bytes: &[u8], out: &mut Vec<u8>) -> VortexResult<u64> {
    // 1. Deserialize IPC bytes → RecordBatches
    let batches = ipc_bytes_to_record_batches(ipc_bytes)?;
    if batches.is_empty() {
        return Ok(0);
    }

    let row_count: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();

    // 2. Build a VortexSession with default encodings (ALP, FastLanes, FSST, ZigZag, etc.)
    let session = VortexSession::empty();
    register_default_encodings(&session);

    // 3. Convert RecordBatches → Vortex Arrays via FromArrowArray
    let arrays: Vec<ArrayRef> = batches
        .into_iter()
        .map(record_batch_to_vortex)
        .collect::<VortexResult<_>>()?;

    // 4. Capture the dtype from the first array (all batches share the same schema)
    let dtype = arrays[0].dtype().clone();

    // 5. Build an ArrayStream from the arrays using ArrayStreamAdapter
    let stream = futures::stream::iter(
        arrays
            .into_iter()
            .map(Ok::<ArrayRef, vortex_error::VortexError>),
    );
    let array_stream = ArrayStreamAdapter::new(dtype, stream);

    // 6. Write to the output buffer.
    //    Statistics are excluded — Fuse BlockMeta owns that data.
    VortexWriteOptions::new(session)
        .write(out, array_stream)
        .await
        .map_err(VortexStorageError::Vortex)?;

    Ok(row_count)
}

/// Convert a single RecordBatch into a Vortex ArrayRef via FromArrowArray.
fn record_batch_to_vortex(batch: RecordBatch) -> VortexResult<ArrayRef> {
    ArrayRef::from_arrow(batch, true).map_err(VortexStorageError::Vortex)
}
