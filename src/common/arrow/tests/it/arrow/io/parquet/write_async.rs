use ahash::AHashMap;
use arrow2::array::Float32Array;
use arrow2::array::Int32Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::DataType;
use arrow2::datatypes::Field;
use arrow2::datatypes::Schema;
use arrow2::error::Result;
use arrow2::io::parquet::read::infer_schema;
use arrow2::io::parquet::read::read_columns_many_async;
use arrow2::io::parquet::read::read_metadata_async;
use arrow2::io::parquet::read::RowGroupDeserializer;
use arrow2::io::parquet::write::CompressionOptions;
use arrow2::io::parquet::write::Encoding;
use arrow2::io::parquet::write::Version;
use arrow2::io::parquet::write::WriteOptions;
use futures::future::BoxFuture;
use futures::io::Cursor;
use futures::SinkExt;

use super::FileSink;

#[tokio::test]
async fn test_parquet_async_roundtrip() {
    let mut data = vec![];
    for i in 0..5 {
        let a1 = Int32Array::from(&[Some(i), None, Some(i + 1)]);
        let a2 = Float32Array::from(&[None, Some(i as f32), None]);
        let chunk = Chunk::new(vec![a1.boxed(), a2.boxed()]);
        data.push(chunk);
    }
    let schema = Schema::from(vec![
        Field::new("a1", DataType::Int32, true),
        Field::new("a2", DataType::Float32, true),
    ]);
    let encoding = vec![vec![Encoding::Plain], vec![Encoding::Plain]];
    let options = WriteOptions {
        write_statistics: true,
        compression: CompressionOptions::Uncompressed,
        version: Version::V2,
        data_pagesize_limit: None,
    };

    let mut buffer = Cursor::new(Vec::new());
    let mut sink = FileSink::try_new(&mut buffer, schema.clone(), encoding, options).unwrap();
    sink.metadata
        .insert(String::from("key"), Some("value".to_string()));
    for chunk in &data {
        sink.feed(chunk.clone()).await.unwrap();
    }
    sink.close().await.unwrap();
    drop(sink);

    buffer.set_position(0);
    let metadata = read_metadata_async(&mut buffer).await.unwrap();
    let kv = AHashMap::<String, Option<String>>::from_iter(
        metadata
            .key_value_metadata()
            .to_owned()
            .unwrap()
            .into_iter()
            .map(|kv| (kv.key, kv.value)),
    );
    assert_eq!(kv.get("key").unwrap(), &Some("value".to_string()));
    let read_schema = infer_schema(&metadata).unwrap();
    assert_eq!(read_schema, schema);
    let factory = || Box::pin(futures::future::ready(Ok(buffer.clone()))) as BoxFuture<_>;

    let mut out = vec![];
    for group in &metadata.row_groups {
        let column_chunks =
            read_columns_many_async(factory, group, schema.fields.clone(), None, None, None)
                .await
                .unwrap();
        let chunks = RowGroupDeserializer::new(column_chunks, group.num_rows(), None);
        let mut chunks = chunks.collect::<Result<Vec<_>>>().unwrap();
        out.append(&mut chunks);
    }

    for i in 0..5 {
        assert_eq!(data[i], out[i]);
    }
}
