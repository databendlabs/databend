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

use databend_common_arrow::arrow::datatypes::Field;
use databend_common_arrow::arrow::datatypes::Schema;
use databend_common_arrow::arrow::error::Result;
use databend_common_arrow::native::read::reader::read_meta;
use databend_common_arrow::native::write::NativeWriter;
use databend_common_arrow::native::write::WriteOptions;
use databend_common_arrow::native::ColumnMeta;
use databend_common_arrow::native::CommonCompression;

use crate::native::io::new_test_chunk;
use crate::native::io::WRITE_PAGE;

fn write_data(dest: &mut Vec<u8>) -> Vec<ColumnMeta> {
    let chunk = new_test_chunk();
    let fields: Vec<Field> = chunk
        .iter()
        .map(|array| {
            Field::new(
                "name",
                array.data_type().clone(),
                array.validity().is_some(),
            )
        })
        .collect();

    let mut writer = NativeWriter::new(dest, Schema::from(fields), WriteOptions {
        default_compression: CommonCompression::Lz4,
        max_page_size: Some(WRITE_PAGE),
        ..Default::default()
    });

    writer.start().unwrap();
    writer.write(&chunk).unwrap();
    writer.finish().unwrap();

    writer.metas
}

#[test]
fn test_read_meta() -> Result<()> {
    let mut buf = Vec::new();
    let expected_meta = write_data(&mut buf);

    let mut reader = std::io::Cursor::new(buf);
    let meta = read_meta(&mut reader)?;

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
