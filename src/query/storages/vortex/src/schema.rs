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

//! Schema and data conversion via Arrow IPC.
//!
//! Databend uses arrow 56; Vortex requires arrow 58. We bridge the gap using the
//! Arrow IPC binary format, which is stable across minor versions. The caller
//! serializes a RecordBatch with arrow 56 into IPC bytes, passes those bytes here,
//! and we deserialize with arrow 58 before handing off to Vortex.

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::Schema;

use crate::error::VortexResult;
use crate::error::VortexStorageError;

/// Deserialize Arrow IPC stream bytes (written by arrow 56) into a list of
/// RecordBatches using arrow 58.
///
/// The IPC stream format is stable across arrow versions so this is safe.
pub fn ipc_bytes_to_record_batches(ipc_bytes: &[u8]) -> VortexResult<Vec<RecordBatch>> {
    let cursor = std::io::Cursor::new(ipc_bytes);
    let reader = StreamReader::try_new(cursor, None)
        .map_err(|e| VortexStorageError::Other(format!("IPC stream read error: {e}")))?;

    let mut batches = Vec::new();
    for batch in reader {
        let batch = batch
            .map_err(|e| VortexStorageError::Other(format!("IPC batch read error: {e}")))?;
        batches.push(batch);
    }
    Ok(batches)
}

/// Serialize a list of RecordBatches (arrow 58) into Arrow IPC stream bytes.
/// These bytes can be deserialized by arrow 56 on the Databend side.
pub fn record_batches_to_ipc_bytes(
    schema: &Schema,
    batches: &[RecordBatch],
) -> VortexResult<Vec<u8>> {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, schema)
            .map_err(|e| VortexStorageError::Other(format!("IPC stream write error: {e}")))?;
        for batch in batches {
            writer
                .write(batch)
                .map_err(|e| VortexStorageError::Other(format!("IPC batch write error: {e}")))?;
        }
        writer
            .finish()
            .map_err(|e| VortexStorageError::Other(format!("IPC stream finish error: {e}")))?;
    }
    Ok(buf)
}
