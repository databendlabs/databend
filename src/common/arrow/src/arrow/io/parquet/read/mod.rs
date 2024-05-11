// Copyright 2020-2022 Jorge C. Leitão
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

//! APIs to read from Parquet format.
#![allow(clippy::type_complexity)]

mod deserialize;
mod file;
pub mod indexes;
mod row_group;
pub mod schema;
pub mod statistics;

use std::io::Read;
use std::io::Seek;

pub use deserialize::column_iter_to_arrays;
pub use deserialize::create_list;
pub use deserialize::create_map;
pub use deserialize::get_page_iterator;
pub use deserialize::init_nested;
pub use deserialize::n_columns;
pub use deserialize::nested_column_iter_to_arrays;
pub use deserialize::InitNested;
pub use deserialize::NestedArrayIter;
pub use deserialize::NestedState;
pub use deserialize::StructIterator;
pub use file::FileReader;
pub use file::RowGroupReader;
pub use parquet2::error::Error as ParquetError;
pub use parquet2::fallible_streaming_iterator;
pub use parquet2::metadata::ColumnChunkMetaData;
pub use parquet2::metadata::ColumnDescriptor;
pub use parquet2::metadata::RowGroupMetaData;
pub use parquet2::page::CompressedDataPage;
pub use parquet2::page::DataPageHeader;
pub use parquet2::page::Page;
pub use parquet2::read::decompress;
pub use parquet2::read::get_column_iterator;
pub use parquet2::read::read_columns_indexes as _read_columns_indexes;
pub use parquet2::read::read_metadata as _read_metadata;
pub use parquet2::read::read_pages_locations;
pub use parquet2::read::BasicDecompressor;
pub use parquet2::read::Decompressor;
pub use parquet2::read::MutStreamingIterator;
pub use parquet2::read::PageFilter;
pub use parquet2::read::PageReader;
pub use parquet2::read::ReadColumnIterator;
pub use parquet2::read::State;
// re-exports of parquet2's relevant APIs
#[cfg(feature = "io_parquet_async")]
#[cfg_attr(docsrs, doc(cfg(feature = "io_parquet_async")))]
pub use parquet2::read::{get_page_stream, read_metadata_async as _read_metadata_async};
pub use parquet2::schema::types::GroupLogicalType;
pub use parquet2::schema::types::ParquetType;
pub use parquet2::schema::types::PhysicalType;
pub use parquet2::schema::types::PrimitiveConvertedType;
pub use parquet2::schema::types::PrimitiveLogicalType;
pub use parquet2::schema::types::TimeUnit as ParquetTimeUnit;
pub use parquet2::types::int96_to_i64_ns;
pub use parquet2::FallibleStreamingIterator;
pub use row_group::*;
pub use schema::infer_schema;
pub use schema::FileMetaData;

use crate::arrow::array::Array;
use crate::arrow::error::Result;
use crate::arrow::types::i256;
use crate::arrow::types::NativeType;

/// Trait describing a [`FallibleStreamingIterator`] of [`Page`]
pub trait Pages:
    FallibleStreamingIterator<Item = Page, Error = ParquetError> + Send + Sync
{
}

impl<I: FallibleStreamingIterator<Item = Page, Error = ParquetError> + Send + Sync> Pages for I {}

/// Type def for a sharable, boxed dyn [`Iterator`] of arrays
pub type ArrayIter<'a> = Box<dyn Iterator<Item = Result<Box<dyn Array>>> + Send + Sync + 'a>;

/// Reads parquets' metadata synchronously.
pub fn read_metadata<R: Read + Seek>(reader: &mut R) -> Result<FileMetaData> {
    Ok(_read_metadata(reader)?)
}

/// Reads parquets' metadata asynchronously.
#[cfg(feature = "io_parquet_async")]
#[cfg_attr(docsrs, doc(cfg(feature = "io_parquet_async")))]
pub async fn read_metadata_async(reader: opendal::Reader, file_size: u64) -> Result<FileMetaData> {
    Ok(_read_metadata_async(reader, file_size).await?)
}

fn convert_days_ms(value: &[u8]) -> crate::arrow::types::days_ms {
    crate::arrow::types::days_ms(
        i32::from_le_bytes(value[4..8].try_into().unwrap()),
        i32::from_le_bytes(value[8..12].try_into().unwrap()),
    )
}

fn convert_i128(value: &[u8], n: usize) -> i128 {
    // Copy the fixed-size byte value to the start of a 16 byte stack
    // allocated buffer, then use an arithmetic right shift to fill in
    // MSBs, which accounts for leading 1's in negative (two's complement)
    // values.
    let mut bytes = [0u8; 16];
    bytes[..n].copy_from_slice(value);
    i128::from_be_bytes(bytes) >> (8 * (16 - n))
}

fn convert_i256(value: &[u8]) -> i256 {
    if value[0] >= 128 {
        let mut neg_bytes = [255u8; 32];
        neg_bytes[32 - value.len()..].copy_from_slice(value);
        i256::from_be_bytes(neg_bytes)
    } else {
        let mut bytes = [0u8; 32];
        bytes[32 - value.len()..].copy_from_slice(value);
        i256::from_be_bytes(bytes)
    }
}
