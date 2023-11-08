use std::io::Cursor;

use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;
use arrow2::error::Result;
use arrow2::io::ipc::read;
use arrow2::io::ipc::write::stream_async;
use arrow2::io::ipc::write::stream_async::StreamSink;
use arrow2::io::ipc::IpcField;
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

    let options = stream_async::WriteOptions { compression: None };
    let mut sink = StreamSink::new(&mut result, schema, Some(ipc_fields.to_vec()), options);
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
    let metadata = read::read_stream_metadata(&mut reader)?;
    let reader = read::StreamReader::new(reader, metadata, None);

    let schema = &reader.metadata().schema;
    let ipc_fields = reader.metadata().ipc_schema.fields.clone();

    // read expected JSON output
    let (expected_schema, expected_ipc_fields, expected_batches) =
        read_gzip_json(version, file_name).unwrap();

    assert_eq!(schema, &expected_schema);
    assert_eq!(ipc_fields, expected_ipc_fields);

    let batches = reader
        .map(|x| x.map(|x| x.unwrap()))
        .collect::<Result<Vec<_>>>()?;

    assert_eq!(batches, expected_batches);
    Ok(())
}

#[tokio::test]
async fn write_async() -> Result<()> {
    test_file("1.0.0-littleendian", "generated_primitive").await
}
