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
//! The caller (Databend fuse, using arrow 56) serializes its DataBlock into Arrow IPC
//! bytes and passes them here. We deserialize with arrow 58, convert to a Vortex
//! ArrayStream, compress, and write a Vortex file into the output buffer.
//!
//! Column statistics are NOT written into the Vortex file — they are stored externally
//! in Fuse's BlockMeta.col_stats. This keeps the Vortex file as a pure data container.

use arrow_array::RecordBatch;
use futures::stream;
use vortex_array::ArrayRef;
use vortex_array::arrow::FromArrowArray;
use vortex_array::stream::ArrayStreamExt;
use vortex_file::WriteOptionsSessionExt;
use vortex_file::register_default_encodings;
use vortex_session::VortexSession;

use crate::error::VortexResult;
use crate::error::VortexStorageError;
use crate::schema::ipc_bytes_to_record_batches;

/// Write Arrow IPC bytes as a Vortex file into `out`.
///
/// # Arguments
/// * `ipc_bytes` - Arrow IPC stream bytes produced by Databend (arrow 56).
/// * `out`       - Output buffer; will contain a complete `.vortex` file on success.
///
/// # Returns
/// Number of rows written.
pub async fn write_vortex_file(ipc_bytes: &[u8], out: &mut Vec<u8>) -> VortexResult<u64> {
    // 1. Deserialize IPC bytes → RecordBatches (arrow 58)
    let batches = ipc_bytes_to_record_batches(ipc_bytes)?;
    if batches.is_empty() {
        return Ok(0);
    }

    let row_count: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();

    // 2. Build a VortexSession with default encodings (ALP, FastLanes, FSST, etc.)
    let session = VortexSession::empty();
    register_default_encodings(&session);

    // 3. Convert RecordBatches → Vortex Arrays
    // ArrayRef::from_arrow(RecordBatch, nullable) converts a RecordBatch to a StructArray.
    let arrays: Vec<ArrayRef> = batches
        .into_iter()
        .map(|batch| ArrayRef::from_arrow(batch, false).map_err(VortexStorageError::Vortex))
        .collect::<VortexResult<_>>()?;

    // 4. Build an ArrayStream from the arrays
    let dtype = arrays[0].dtype().clone();
    let array_stream = stream::iter(arrays.into_iter().map(Ok)).into_array_stream(dtype);

    // 5. Write to the output buffer using the session's write options.
    // write() takes ownership of the sink and stream, compresses, and writes the .vortex file.
    session
        .write_options()
        .write(out, array_stream)
        .await
        .map_err(VortexStorageError::Vortex)?;

    Ok(row_count)
}
