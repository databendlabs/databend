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

use std::sync::Arc;

use arrow_array::RecordBatch;
use futures::TryStreamExt;
use opendal::Operator;
use vortex_array::ArrayRef;
use vortex_array::ExecutionCtx;
use vortex_array::arrow::ArrowArrayExecutor;
use vortex_array::dtype::DType;
use vortex_array::dtype::Nullability;
use vortex_array::dtype::StructFields;
use vortex_array::expr::root;
use vortex_array::expr::select;
use vortex_file::OpenOptionsSessionExt;
use vortex_file::register_default_encodings;
use vortex_io::session::RuntimeSessionExt;
use vortex_session::VortexSession;

use crate::error::VortexResult;
use crate::error::VortexStorageError;
use crate::io::OpendalVortexReader;
use crate::schema::record_batches_to_ipc_bytes;

/// Read a Vortex file and return Arrow IPC stream bytes.
///
/// # Arguments
/// * `operator`        - Databend opendal Operator for the storage backend.
/// * `path`            - Path to the `.vortex` file.
/// * `projected_names` - Optional column names to project. `None` reads all columns.
pub async fn read_vortex_file(
    operator: Operator,
    path: &str,
    projected_names: Option<Vec<String>>,
) -> VortexResult<Vec<u8>> {
    // 1. Build session with default encodings and Tokio runtime handle.
    //    with_tokio() registers TokioRuntime::current() so that ScanBuilder's
    //    spawn_blocking calls are driven by the ambient Tokio multi-thread runtime
    //    rather than an undriven smol executor.
    let session = VortexSession::empty().with_tokio();
    register_default_encodings(&session);

    // 2. Build VortexReadAt from opendal Operator
    let reader = Arc::new(OpendalVortexReader::new(operator, path));

    // 3. Open the Vortex file (reads only the footer — O(1) IO)
    let vortex_file = session
        .open_options()
        .open(reader)
        .await
        .map_err(VortexStorageError::Vortex)?;

    // 4. Derive Arrow schema from the file's original DType (written at ingest time).
    //    We must NOT use array_stream.dtype() because Vortex compression can change
    //    the physical dtype (e.g. bool → constant/bitpacked int).  The file footer
    //    always preserves the logical schema.
    let arrow_schema = Arc::new({
        let file_dtype = match &projected_names {
            Some(names) => {
                // Build a projected dtype that only includes the requested fields.
                let fields = vortex_file
                    .dtype()
                    .as_struct_fields_opt()
                    .ok_or_else(|| {
                        VortexStorageError::Other(
                            "Vortex file dtype is not a struct".into(),
                        )
                    })?;
                // project() takes a &[FieldName] where FieldName wraps Arc<str>
                let field_names: Vec<vortex_array::dtype::FieldName> =
                    names.iter().map(|n| vortex_array::dtype::FieldName::from(n.as_str())).collect();
                let projected = fields
                    .project(field_names.as_slice())
                    .map_err(VortexStorageError::Vortex)?;
                DType::Struct(projected, Nullability::NonNullable)
            }
            None => vortex_file.dtype().clone(),
        };
        file_dtype
            .to_arrow_schema()
            .map_err(VortexStorageError::Vortex)?
    });

    // 5. Build scan with optional column projection via select() expression
    let scan_builder = vortex_file
        .scan()
        .map_err(VortexStorageError::Vortex)?;

    let scan_builder = if let Some(names) = projected_names {
        // select(field_names, root()) projects the named columns
        let name_refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
        scan_builder.with_projection(select(name_refs, root()))
    } else {
        scan_builder
    };

    // 6. Execute the scan → ArrayStream
    let array_stream = scan_builder
        .into_array_stream()
        .map_err(VortexStorageError::Vortex)?;

    // 7. Collect all arrays
    let arrays: Vec<ArrayRef> = array_stream
        .map_err(VortexStorageError::Vortex)
        .try_collect()
        .await?;

    if arrays.is_empty() {
        return Ok(Vec::new());
    }

    // 8. Convert Vortex arrays → RecordBatches using the logical Arrow schema.
    //    execute_record_batch casts the physical (compressed) representation back
    //    to the logical Arrow types declared in arrow_schema.
    let mut ctx = ExecutionCtx::new(session);
    let mut batches: Vec<RecordBatch> = Vec::with_capacity(arrays.len());
    for array in arrays {
        let batch: RecordBatch = array
            .execute_record_batch(&arrow_schema, &mut ctx)
            .map_err(VortexStorageError::Vortex)?;
        batches.push(batch);
    }

    // 9. Serialize to Arrow IPC bytes
    record_batches_to_ipc_bytes(&arrow_schema, &batches)
}
