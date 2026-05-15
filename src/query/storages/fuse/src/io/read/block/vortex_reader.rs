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
//!     → databend-storages-vortex::read_vortex_file (vortex + arrow 58)
//!       → Arrow IPC bytes (stable binary format)
//!         → RecordBatch (arrow 58, Databend side)
//!           → DataBlock
//!
//! Type fixup: Vortex stores Boolean columns as Int8 internally. When reading
//! back via Arrow IPC, Boolean columns may arrive as Int8. We cast them back
//! to Boolean using the projected_schema as the source of truth.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_cast::cast;
use arrow_ipc::reader::StreamReader;
use arrow_schema::DataType as ArrowDataType;
use arrow_schema::Field;
use arrow_schema::Schema as ArrowSchema;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
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
    // Build the column projection list from the projected schema field names.
    // Vortex uses field names for projection (not positional indices).
    let projected_names: Option<Vec<String>> = if project_indices.is_empty() {
        None
    } else {
        Some(
            projected_schema
                .fields()
                .iter()
                .map(|f| f.name().to_string())
                .collect(),
        )
    };

    // Read the Vortex file → Arrow IPC bytes.
    let ipc_bytes = vortex_read(operator, path, projected_names)
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

    // Build the expected Arrow schema from projected_schema so we can cast
    // columns that Vortex may have changed type on (e.g. Boolean → Int8).
    let expected_arrow_schema: ArrowSchema = projected_schema.as_ref().into();

    let data_schema = projected_schema.as_ref().into();
    let mut blocks: Vec<DataBlock> = Vec::new();

    for batch_result in reader {
        let batch = batch_result
            .map_err(|e| ErrorCode::StorageOther(format!("Vortex: IPC batch read error: {e}")))?;

        // Cast any columns whose type doesn't match the expected schema.
        let batch = cast_batch_to_schema(batch, &expected_arrow_schema)?;

        let block = DataBlock::from_record_batch(&data_schema, &batch)?;
        blocks.push(block);
    }

    match blocks.len() {
        0 => Ok(DataBlock::new(vec![], num_rows)),
        1 => Ok(blocks.remove(0)),
        _ => DataBlock::concat(&blocks),
    }
}

/// Cast columns in `batch` to match `expected_schema` where types differ.
///
/// Vortex may store Boolean as Int8 internally. This function casts such
/// columns back to their expected Arrow type using arrow_cast::cast.
fn cast_batch_to_schema(batch: RecordBatch, expected: &ArrowSchema) -> Result<RecordBatch> {
    // Fast path: if schemas already match, return as-is.
    if batch.schema().fields().len() == expected.fields().len()
        && batch
            .schema()
            .fields()
            .iter()
            .zip(expected.fields().iter())
            .all(|(a, b)| a.data_type() == b.data_type())
    {
        return Ok(batch);
    }

    let mut new_columns = Vec::with_capacity(batch.num_columns());
    let mut new_fields = Vec::with_capacity(batch.num_columns());

    for (i, col) in batch.columns().iter().enumerate() {
        let expected_field = &expected.fields()[i];
        let expected_type = expected_field.data_type();

        let new_col = if col.data_type() != expected_type {
            cast(col.as_ref(), expected_type).map_err(|e| {
                ErrorCode::StorageOther(format!(
                    "Vortex: failed to cast column '{}' from {:?} to {:?}: {e}",
                    expected_field.name(),
                    col.data_type(),
                    expected_type,
                ))
            })?
        } else {
            col.clone()
        };

        new_fields.push(expected_field.clone());
        new_columns.push(new_col);
    }

    let new_schema = Arc::new(ArrowSchema::new(new_fields));
    RecordBatch::try_new(new_schema, new_columns)
        .map_err(|e| ErrorCode::StorageOther(format!("Vortex: failed to rebuild RecordBatch: {e}")))
}
