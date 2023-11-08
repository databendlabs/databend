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

use ahash::AHashMap;
use common_arrow::arrow::array::Float32Array;
use common_arrow::arrow::array::Int32Array;
use common_arrow::arrow::chunk::Chunk;
use common_arrow::arrow::datatypes::DataType;
use common_arrow::arrow::datatypes::Field;
use common_arrow::arrow::datatypes::Schema;
use common_arrow::arrow::error::Result;
use common_arrow::arrow::io::parquet::read::infer_schema;
use common_arrow::arrow::io::parquet::read::read_columns_many_async;
use common_arrow::arrow::io::parquet::read::read_metadata_async;
use common_arrow::arrow::io::parquet::read::RowGroupDeserializer;
use common_arrow::arrow::io::parquet::write::CompressionOptions;
use common_arrow::arrow::io::parquet::write::Encoding;
use common_arrow::arrow::io::parquet::write::Version;
use common_arrow::arrow::io::parquet::write::WriteOptions;
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
