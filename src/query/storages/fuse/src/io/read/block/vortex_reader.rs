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

//! Read a Vortex-format block file and return a Databend DataBlock.
//!
//! Flow:
//!   opendal::Operator + path
//!     → databend-storages-vortex::read_vortex_file (arrow 58 + vortex)
//!       → Arrow IPC bytes (stable binary format)
//!         → RecordBatch(es) (arrow 58, Databend side)
//!           → DataBlock(s) → concatenated DataBlock

use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;

use arrow_ipc::reader::StreamReader;
use arrow_schema::Field;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::FieldIndex;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::types::DataType;
use databend_storages_vortex::read_vortex_file as vortex_read;
use opendal::Operator;

/// Read a Vortex block file and return a DataBlock with the projected columns.
///
/// # Arguments
/// * `operator`        - opendal Operator for the storage backend.
/// * `path`            - Path to the `.vortex` file.
/// * `projected_schema`- The projected table schema (only requested columns).
/// * `project_indices` - Map from field index → (column_id, arrow Field, DataType).
/// * `num_rows`        - Expected row count (from BlockMeta).
pub async fn read_vortex_block(
    operator: Operator,
    path: &str,
    projected_schema: &TableSchemaRef,
    project_indices: &Arc<BTreeMap<FieldIndex, (ColumnId, Field, DataType)>>,
    num_rows: usize,
) -> Result<DataBlock> {
    // Build the column projection list (0-based indices into the Vortex file's schema).
    let projected_cols: Vec<usize> = project_indices.keys().copied().collect();
    let projected_cols = if projected_cols.is_empty() {
        None
    } else {
        Some(projected_cols)
    };

    // Read the Vortex file → Arrow IPC bytes.
    let ipc_bytes = vortex_read(operator, path, projected_cols)
        .await
        .map_err(|e| ErrorCode::StorageOther(format!("Vortex read error: {e}")))?;

    if ipc_bytes.is_empty() {
        return Ok(DataBlock::new(vec![], num_rows));
    }

    // Deserialize Arrow IPC bytes → RecordBatches.
    // A Vortex file may have multiple chunks, each becoming one IPC batch.
    let cursor = Cursor::new(&ipc_bytes);
    let reader = StreamReader::try_new(cursor, None)
        .map_err(|e| ErrorCode::StorageOther(format!("Vortex: IPC stream read error: {e}")))?;

    let data_schema = projected_schema.as_ref().into();
    let mut blocks: Vec<DataBlock> = Vec::new();

    for batch_result in reader {
        let batch = batch_result
            .map_err(|e| ErrorCode::StorageOther(format!("Vortex: IPC batch read error: {e}")))?;
        let block = DataBlock::from_record_batch(&data_schema, &batch)?;
        blocks.push(block);
    }

    match blocks.len() {
        0 => Ok(DataBlock::new(vec![], num_rows)),
        1 => Ok(blocks.remove(0)),
        _ => DataBlock::concat(&blocks),
    }
}
