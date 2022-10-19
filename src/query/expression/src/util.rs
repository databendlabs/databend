// Copyright 2022 Datafuse Labs.
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

use chrono_tz::Tz;
use common_arrow::arrow::array::Array;
use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_arrow::arrow::buffer::Buffer;
use common_arrow::arrow::chunk::Chunk as ArrowChunk;
use common_arrow::arrow::datatypes::DataType as ArrowDataType;
use common_arrow::arrow::datatypes::Field;
use common_arrow::arrow::datatypes::Schema;
use common_arrow::arrow::io::ipc::read::read_file_metadata;
use common_arrow::arrow::io::ipc::read::FileReader;
use common_arrow::arrow::io::ipc::write::FileWriter;
use common_arrow::arrow::io::ipc::write::WriteOptions as IpcWriteOptions;
use common_arrow::arrow::io::parquet::write::transverse;
use common_arrow::arrow::io::parquet::write::RowGroupIterator;
use common_arrow::arrow::io::parquet::write::WriteOptions;
use common_arrow::parquet::compression::CompressionOptions;
use common_arrow::parquet::encoding::Encoding;
use common_arrow::parquet::metadata::ThriftFileMetaData;
use common_arrow::parquet::write::Version;
use common_arrow::write_parquet_file;
use common_exception::ErrorCode;
use common_exception::Result as ExceptionResult;

use crate::error::Result;
use crate::types::AnyType;
use crate::types::DataType;
use crate::Chunk;
use crate::Column;
use crate::DataSchema;
use crate::Evaluator;
use crate::FunctionRegistry;
use crate::RawExpr;
use crate::Span;
use crate::Value;

pub fn column_merge_validity(column: &Column, bitmap: Option<Bitmap>) -> Option<Bitmap> {
    match column {
        Column::Nullable(c) => match bitmap {
            None => Some(c.validity.clone()),
            Some(v) => Some(&c.validity & (&v)),
        },
        _ => bitmap,
    }
}

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

pub fn append_bitmap(bitmap: &mut MutableBitmap, other: &MutableBitmap) {
    bitmap.extend_from_slice(other.as_slice(), 0, other.len());
}

pub fn constant_bitmap(value: bool, len: usize) -> MutableBitmap {
    let mut builder = MutableBitmap::new();
    builder.extend_constant(len, value);
    builder
}

pub fn buffer_into_mut<T: Clone>(mut buffer: Buffer<T>) -> Vec<T> {
    buffer
        .get_mut()
        .map(std::mem::take)
        .unwrap_or_else(|| buffer.to_vec())
}

pub fn serialize_arrow_array(col: Box<dyn Array>) -> Vec<u8> {
    let mut buffer = Vec::new();
    let schema = Schema::from(vec![Field::new("col", col.data_type().clone(), true)]);
    let mut writer = FileWriter::new(&mut buffer, schema, None, IpcWriteOptions::default());
    writer.start().unwrap();
    writer
        .write(&common_arrow::arrow::chunk::Chunk::new(vec![col]), None)
        .unwrap();
    writer.finish().unwrap();
    buffer
}

pub fn deserialize_arrow_array(bytes: &[u8]) -> Option<Box<dyn Array>> {
    let mut cursor = Cursor::new(bytes);
    let metadata = read_file_metadata(&mut cursor).ok()?;
    let mut reader = FileReader::new(cursor, metadata, None, None);
    let col = reader.next()?.ok()?.into_arrays().remove(0);
    Some(col)
}

pub const fn concat_array<T, const A: usize, const B: usize>(a: &[T; A], b: &[T; B]) -> [T; A + B] {
    let mut result = std::mem::MaybeUninit::uninit();
    let dest = result.as_mut_ptr() as *mut T;
    unsafe {
        std::ptr::copy_nonoverlapping(a.as_ptr(), dest, A);
        std::ptr::copy_nonoverlapping(b.as_ptr(), dest.add(A), B);
        result.assume_init()
    }
}

/// A convenient shortcut to evaluate a scalar function.
pub fn eval_function(
    span: Span,
    fn_name: &str,
    args: impl Iterator<Item = (Value<AnyType>, DataType)>,
    tz: Tz,
    num_rows: usize,
    fn_registry: &FunctionRegistry,
) -> Result<(Value<AnyType>, DataType)> {
    let (args, cols) = args
        .enumerate()
        .map(|(id, (val, ty))| {
            (
                RawExpr::ColumnRef {
                    span: span.clone(),
                    id,
                    data_type: ty.clone(),
                },
                (val, ty),
            )
        })
        .unzip();
    let raw_expr = RawExpr::FunctionCall {
        span,
        name: fn_name.to_string(),
        params: vec![],
        args,
    };
    let (expr, ty) = crate::type_check::check(&raw_expr, fn_registry)?;
    let chunk = Chunk::new(cols, num_rows);
    let evaluator = Evaluator::new(&chunk, tz);
    Ok((evaluator.run(&expr)?, ty))
}

pub fn serialize_chunks_with_compression(
    chunks: Vec<Chunk>,
    schema: impl AsRef<DataSchema>,
    buf: &mut Vec<u8>,
    compression: CompressionOptions,
) -> ExceptionResult<(u64, ThriftFileMetaData)> {
    let arrow_schema = schema.as_ref().to_arrow();

    let row_group_write_options = WriteOptions {
        write_statistics: false,
        compression,
        version: Version::V2,
    };
    let batches = chunks
        .into_iter()
        .map(ArrowChunk::try_from)
        .collect::<ExceptionResult<Vec<_>>>()?;

    let encoding_map = |data_type: &ArrowDataType| match data_type {
        ArrowDataType::Dictionary(..) => Encoding::RleDictionary,
        _ => col_encoding(data_type),
    };

    let encodings: Vec<Vec<_>> = arrow_schema
        .fields
        .iter()
        .map(|f| transverse(&f.data_type, encoding_map))
        .collect::<Vec<_>>();

    let row_groups = RowGroupIterator::try_new(
        batches.into_iter().map(Ok),
        &arrow_schema,
        row_group_write_options,
        encodings,
    )?;

    use common_arrow::parquet::write::WriteOptions as FileWriteOption;
    let options = FileWriteOption {
        write_statistics: false,
        version: Version::V2,
    };

    match write_parquet_file(buf, row_groups, arrow_schema.clone(), options) {
        Ok(result) => Ok(result),
        Err(cause) => Err(ErrorCode::ParquetError(cause.to_string())),
    }
}

pub fn serialize_chunks(
    chunks: Vec<Chunk>,
    schema: impl AsRef<DataSchema>,
    buf: &mut Vec<u8>,
) -> ExceptionResult<(u64, ThriftFileMetaData)> {
    serialize_chunks_with_compression(chunks, schema, buf, CompressionOptions::Lz4Raw)
}

fn col_encoding(_data_type: &ArrowDataType) -> Encoding {
    // Although encoding does work, parquet2 has not implemented decoding of DeltaLengthByteArray yet, we fallback to Plain
    // From parquet2: Decoding "DeltaLengthByteArray"-encoded required V2 pages is not yet implemented for Binary.
    //
    // match data_type {
    //    ArrowDataType::Binary
    //    | ArrowDataType::LargeBinary
    //    | ArrowDataType::Utf8
    //    | ArrowDataType::LargeUtf8 => Encoding::DeltaLengthByteArray,
    //    _ => Encoding::Plain,
    //}
    Encoding::Plain
}
