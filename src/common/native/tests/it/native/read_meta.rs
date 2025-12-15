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

use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::infer_schema_type;
use databend_common_native::ColumnMeta;
use databend_common_native::CommonCompression;
use databend_common_native::read::reader::read_meta;
use databend_common_native::write::NativeWriter;
use databend_common_native::write::WriteOptions;

use super::io::WRITE_PAGE;
use super::io::new_test_column;

fn write_data(dest: &mut Vec<u8>) -> Vec<ColumnMeta> {
    let chunk = new_test_column();
    let fields: Vec<TableField> = chunk
        .iter()
        .map(|col| TableField::new("name", infer_schema_type(&col.data_type()).unwrap()))
        .collect();

    let mut writer = NativeWriter::new(dest, TableSchema::new(fields), WriteOptions {
        default_compression: CommonCompression::Lz4,
        max_page_size: Some(WRITE_PAGE),
        ..Default::default()
    })
    .unwrap();

    writer.start().unwrap();
    writer.write(&chunk).unwrap();
    writer.finish().unwrap();

    writer.metas
}

#[test]
fn test_read_meta() -> std::io::Result<()> {
    let mut buf = Vec::new();
    let expected_meta = write_data(&mut buf);

    let mut reader = std::io::Cursor::new(buf);
    let meta = read_meta(&mut reader).unwrap();

    assert_eq!(expected_meta, meta);

    Ok(())
}

// TODO(xuanwo): bring this test back when we extract trait for ReadAt.
//
// #[test]
// fn test_read_meta_async() -> Result<()> {
//     async_std::task::block_on(test_read_meta_async_impl())
// }
//
// async fn test_read_meta_async_impl() -> Result<()> {
//     let mut buf: Vec<u8> = Vec::new();
//     let expected_meta = write_data(&mut buf);
//     let len = buf.len();
//
//     let mut reader = async_std::io::Cursor::new(buf);
//
//     // With `total_len`.
//     let meta = read_meta_async(&mut reader, len).await?;
//     assert_eq!(expected_meta, meta);
//     Ok(())
// }
