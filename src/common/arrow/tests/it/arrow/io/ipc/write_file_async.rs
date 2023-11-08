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

use std::io::Cursor;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::chunk::Chunk;
use common_arrow::arrow::datatypes::Schema;
use common_arrow::arrow::error::Result;
use common_arrow::arrow::io::ipc::read;
use common_arrow::arrow::io::ipc::write::file_async::FileSink;
use common_arrow::arrow::io::ipc::write::WriteOptions;
use common_arrow::arrow::io::ipc::IpcField;
use futures::io::Cursor as AsyncCursor;
use futures::SinkExt;

use crate::io::ipc::common::read_arrow_stream;
use crate::io::ipc::common::read_gzip_json;

async fn write_(
    schema: &Schema,
    ipc_fields: &[IpcField],
    batches: &[Chunk<Box<dyn Array>>],
) -> Result<Vec<u8>> {
    let mut result = AsyncCursor::new(vec![]);

    let options = WriteOptions { compression: None };
    let mut sink = FileSink::new(
        &mut result,
        schema.clone(),
        Some(ipc_fields.to_vec()),
        options,
    );
    for batch in batches {
        sink.feed((batch, Some(ipc_fields)).into()).await?;
    }
    sink.close().await?;
    drop(sink);
    Ok(result.into_inner())
}

async fn test_file(version: &str, file_name: &str) -> Result<()> {
    let (schema, ipc_fields, batches) = read_arrow_stream(version, file_name, None);

    let result = write_(&schema, &ipc_fields, &batches).await?;

    let mut reader = Cursor::new(result);
    let metadata = read::read_file_metadata(&mut reader)?;
    let reader = read::FileReader::new(reader, metadata, None, None);

    let schema = &reader.metadata().schema;
    let ipc_fields = reader.metadata().ipc_schema.fields.clone();

    // read expected JSON output
    let (expected_schema, expected_ipc_fields, expected_batches) =
        read_gzip_json(version, file_name).unwrap();

    assert_eq!(schema, &expected_schema);
    assert_eq!(ipc_fields, expected_ipc_fields);

    let batches = reader.collect::<Result<Vec<_>>>()?;

    assert_eq!(batches, expected_batches);
    Ok(())
}

#[tokio::test]
async fn write_async() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_primitive").await
}
