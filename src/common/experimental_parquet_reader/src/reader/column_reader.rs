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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DecimalDataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::Column;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::Compression;
use parquet2::compression::Compression as ParquetCompression;
use parquet2::metadata::Descriptor;
use parquet2::read::PageMetaData;
use parquet2::schema::types::PhysicalType;
use parquet2::schema::types::PrimitiveType;

use crate::column::new_decimal128_iter;
use crate::column::new_decimal256_iter;
use crate::column::new_decimal64_iter;
use crate::column::new_int32_iter;
use crate::column::new_int64_iter;
use crate::column::DateIter;
use crate::column::IntegerMetadata;
use crate::column::StringIter;
use crate::reader::decompressor::Decompressor;
use crate::reader::page_reader::PageReader;

pub type ColumnIter<'a> = Box<dyn Iterator<Item = Result<Column>> + Send + Sync + 'a>;

pub fn data_chunk_to_col_iter<'a>(
    meta: &ColumnMeta,
    chunk: &'a [u8],
    rows: usize,
    column_descriptor: &Descriptor,
    field: TableField,
    compression: &Compression,
) -> Result<ColumnIter<'a>> {
    let pages = {
        let meta = meta.as_parquet().unwrap();
        let page_meta_data = PageMetaData {
            column_start: meta.offset,
            num_values: meta.num_values as i64,
            compression: to_parquet_compression(compression)?,
            descriptor: (*column_descriptor).clone(),
        };
        let pages = PageReader::new_with_page_meta(chunk, page_meta_data, usize::MAX);
        Decompressor::new(pages, vec![])
    };

    let typ = &column_descriptor.primitive_type;

    pages_to_column_iter(pages, typ, field, rows, None)
}

fn pages_to_column_iter<'a>(
    column: Decompressor<'a>,
    types: &PrimitiveType,
    field: TableField,
    num_rows: usize,
    chunk_size: Option<usize>,
) -> Result<ColumnIter<'a>> {
    let pages = column;
    let parquet_physical_type = &types.physical_type;

    let (inner_data_type, is_nullable) = match &field.data_type {
        TableDataType::Nullable(inner) => (inner.as_ref(), true),
        other => (other, false),
    };

    match (parquet_physical_type, inner_data_type) {
        (PhysicalType::Int32, TableDataType::Number(NumberDataType::Int32)) => {
            Ok(Box::new(new_int32_iter(pages, num_rows, is_nullable, chunk_size)))
        }
        (PhysicalType::Int64, TableDataType::Number(NumberDataType::Int64)) => {
            Ok(Box::new(new_int64_iter(pages, num_rows, is_nullable, chunk_size)))
        }
        (PhysicalType::ByteArray, TableDataType::String) => {
            Ok(Box::new(StringIter::new(pages, num_rows, chunk_size)))
        }
        (PhysicalType::Int32, TableDataType::Decimal(DecimalDataType::Decimal64(_))) => {
            unimplemented!("coming soon")
        }
        (PhysicalType::Int64, TableDataType::Decimal(DecimalDataType::Decimal64(decimal_size))) => {
            Ok(Box::new(new_decimal64_iter(
                pages,
                num_rows,
                decimal_size.precision(),
                decimal_size.scale(),
                is_nullable,
                chunk_size,
            )))
        }
        (PhysicalType::FixedLenByteArray(_), TableDataType::Decimal(DecimalDataType::Decimal128(decimal_size))) => {
            Ok(Box::new(new_decimal128_iter(
                pages,
                num_rows,
                decimal_size.precision(),
                decimal_size.scale(),
                is_nullable,
                chunk_size,
            )))
        }
        (PhysicalType::FixedLenByteArray(_), TableDataType::Decimal(DecimalDataType::Decimal256(decimal_size))) => {
            Ok(Box::new(new_decimal256_iter(
                pages,
                num_rows,
                decimal_size.precision(),
                decimal_size.scale(),
                is_nullable,
                chunk_size,
            )))
        }
        (PhysicalType::Int32, TableDataType::Date) => {
            Ok(Box::new(DateIter::new(
                pages,
                num_rows,
                is_nullable,
                IntegerMetadata,
                chunk_size,
            )))
        }
        (physical_type, table_data_type) => Err(ErrorCode::StorageOther(format!(
            "Unsupported combination: parquet_physical_type={:?}, field_data_type={:?}, nullable={}",
            physical_type, table_data_type, is_nullable
        ))),
    }
}

fn to_parquet_compression(meta_compression: &Compression) -> Result<ParquetCompression> {
    match meta_compression {
        Compression::Lz4 => Err(ErrorCode::StorageOther(
            "Legacy compression algorithm [Lz4] is no longer supported.",
        )),
        Compression::Lz4Raw => Ok(ParquetCompression::Lz4Raw),
        Compression::Snappy => Ok(ParquetCompression::Snappy),
        Compression::Zstd => Ok(ParquetCompression::Zstd),
        Compression::Gzip => Ok(ParquetCompression::Gzip),
        Compression::None => Ok(ParquetCompression::Uncompressed),
    }
}
