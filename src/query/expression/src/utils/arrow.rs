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
use std::io::Read;
use std::io::Seek;
use std::io::Write;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_ipc::reader::FileReaderBuilder;
use arrow_ipc::writer::FileWriter;
use arrow_ipc::writer::IpcWriteOptions;
use arrow_ipc::CompressionType;
use arrow_schema::Schema;
use databend_common_column::bitmap::Bitmap;
use databend_common_column::bitmap::MutableBitmap;
use databend_common_column::buffer::Buffer;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::Column;
use crate::DataField;

pub fn bitmap_into_mut(bitmap: Bitmap) -> MutableBitmap {
    bitmap
        .into_mut()
        .map_left(|bitmap| {
            let mut builder = MutableBitmap::new();
            builder.extend_from_bitmap(&bitmap);
            builder
        })
        .into_inner()
}

pub fn repeat_bitmap(bitmap: &mut Bitmap, n: usize) -> MutableBitmap {
    let mut builder = MutableBitmap::new();
    for _ in 0..n {
        builder.extend_from_bitmap(bitmap);
    }
    builder
}

pub fn append_bitmap(bitmap: &mut MutableBitmap, other: &Bitmap) {
    bitmap.extend_from_bitmap(other)
}

pub fn buffer_into_mut<T: Clone>(mut buffer: Buffer<T>) -> Vec<T> {
    unsafe {
        buffer
            .get_mut()
            .map(std::mem::take)
            .unwrap_or_else(|| buffer.to_vec())
    }
}

pub fn serialize_column(col: &Column) -> Vec<u8> {
    let mut buffer = Vec::new();
    write_column(col, &mut buffer).unwrap();
    buffer
}

pub fn write_column(
    col: &Column,
    w: &mut impl Write,
) -> std::result::Result<(), arrow_schema::ArrowError> {
    let field = col.arrow_field();
    let schema = Schema::new(vec![field]);
    let mut writer = FileWriter::try_new_with_options(
        w,
        &schema,
        IpcWriteOptions::default().try_with_compression(Some(CompressionType::LZ4_FRAME))?,
    )?;

    let batch = RecordBatch::try_new(Arc::new(schema), vec![col.clone().into_arrow_rs()])?;

    writer.write(&batch)?;
    writer.finish()
}

pub fn deserialize_column(bytes: &[u8]) -> Result<Column> {
    let mut cursor = Cursor::new(bytes);
    read_column(&mut cursor)
}

fn read_column<R: Read + Seek>(r: &mut R) -> Result<Column> {
    let mut reader = FileReaderBuilder::new().build(r)?;
    let schema = reader.schema();
    let f = DataField::try_from(schema.field(0))?;

    let col = reader
        .next()
        .ok_or_else(|| ErrorCode::Internal("expected one arrow array"))??
        .remove_column(0);

    Column::from_arrow_rs(col, f.data_type())
}

pub fn and_validities(lhs: Option<Bitmap>, rhs: Option<Bitmap>) -> Option<Bitmap> {
    match (lhs, rhs) {
        (Some(lhs), None) => Some(lhs),
        (None, Some(rhs)) => Some(rhs),
        (None, None) => None,
        (Some(lhs), Some(rhs)) => Some((&lhs) & (&rhs)),
    }
}

pub fn or_validities(lhs: Option<Bitmap>, rhs: Option<Bitmap>) -> Option<Bitmap> {
    match (lhs, rhs) {
        (Some(lhs), None) => Some(lhs),
        (None, Some(rhs)) => Some(rhs),
        (None, None) => None,
        (Some(lhs), Some(rhs)) => Some((&lhs) | (&rhs)),
    }
}
