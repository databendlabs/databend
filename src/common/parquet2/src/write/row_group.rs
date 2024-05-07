// Copyright [2021] [Jorge C Leitao]
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

use std::io::Write;

#[cfg(feature = "async")]
use futures::AsyncWrite;
use parquet_format_safe::ColumnChunk;
use parquet_format_safe::RowGroup;

use super::column_chunk::write_column_chunk;
#[cfg(feature = "async")]
use super::column_chunk::write_column_chunk_async;
use super::page::is_data_page;
use super::page::PageWriteSpec;
use super::DynIter;
use super::DynStreamingIterator;
use crate::error::Error;
use crate::error::Result;
use crate::metadata::ColumnChunkMetaData;
use crate::metadata::ColumnDescriptor;
use crate::page::CompressedPage;

pub struct ColumnOffsetsMetadata {
    pub dictionary_page_offset: Option<i64>,
    pub data_page_offset: Option<i64>,
}

impl ColumnOffsetsMetadata {
    pub fn from_column_chunk(column_chunk: &ColumnChunk) -> ColumnOffsetsMetadata {
        ColumnOffsetsMetadata {
            dictionary_page_offset: column_chunk
                .meta_data
                .as_ref()
                .map(|meta| meta.dictionary_page_offset)
                .unwrap_or(None),
            data_page_offset: column_chunk
                .meta_data
                .as_ref()
                .map(|meta| meta.data_page_offset),
        }
    }

    pub fn from_column_chunk_metadata(
        column_chunk_metadata: &ColumnChunkMetaData,
    ) -> ColumnOffsetsMetadata {
        ColumnOffsetsMetadata {
            dictionary_page_offset: column_chunk_metadata.dictionary_page_offset(),
            data_page_offset: Some(column_chunk_metadata.data_page_offset()),
        }
    }

    pub fn calc_row_group_file_offset(&self) -> Option<i64> {
        self.dictionary_page_offset
            .filter(|x| *x > 0_i64)
            .or(self.data_page_offset)
    }
}

fn compute_num_rows(columns: &[(ColumnChunk, Vec<PageWriteSpec>)]) -> Result<i64> {
    columns
        .get(0)
        .map(|(_, specs)| {
            let mut num_rows = 0;
            specs
                .iter()
                .filter(|x| is_data_page(x))
                .try_for_each(|spec| {
                    num_rows += spec.num_rows.ok_or_else(|| {
                        Error::oos("All data pages must declare the number of rows on it")
                    })? as i64;
                    Result::Ok(())
                })?;
            Result::Ok(num_rows)
        })
        .unwrap_or(Ok(0))
}

pub fn write_row_group<
    'a,
    W,
    E, // external error any of the iterators may emit
>(
    writer: &mut W,
    mut offset: u64,
    descriptors: &[ColumnDescriptor],
    columns: DynIter<'a, std::result::Result<DynStreamingIterator<'a, CompressedPage, E>, E>>,
    ordinal: usize,
) -> Result<(RowGroup, Vec<Vec<PageWriteSpec>>, u64)>
where
    W: Write,
    Error: From<E>,
    E: std::error::Error,
{
    let column_iter = descriptors.iter().zip(columns);

    let initial = offset;
    let columns = column_iter
        .map(|(descriptor, page_iter)| {
            let (column, page_specs, size) =
                write_column_chunk(writer, offset, descriptor, page_iter?)?;
            offset += size;
            Ok((column, page_specs))
        })
        .collect::<Result<Vec<_>>>()?;
    let bytes_written = offset - initial;

    let num_rows = compute_num_rows(&columns)?;

    // compute row group stats
    let file_offset = columns
        .get(0)
        .map(|(column_chunk, _)| {
            ColumnOffsetsMetadata::from_column_chunk(column_chunk).calc_row_group_file_offset()
        })
        .unwrap_or(None);

    let total_byte_size = columns
        .iter()
        .map(|(c, _)| c.meta_data.as_ref().unwrap().total_uncompressed_size)
        .sum();
    let total_compressed_size = columns
        .iter()
        .map(|(c, _)| c.meta_data.as_ref().unwrap().total_compressed_size)
        .sum();

    let (columns, specs) = columns.into_iter().unzip();

    Ok((
        RowGroup {
            columns,
            total_byte_size,
            num_rows,
            sorting_columns: None,
            file_offset,
            total_compressed_size: Some(total_compressed_size),
            ordinal: ordinal.try_into().ok(),
        },
        specs,
        bytes_written,
    ))
}

#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
pub async fn write_row_group_async<
    'a,
    W,
    E, // external error any of the iterators may emit
>(
    writer: &mut W,
    mut offset: u64,
    descriptors: &[ColumnDescriptor],
    columns: DynIter<'a, std::result::Result<DynStreamingIterator<'a, CompressedPage, E>, E>>,
    ordinal: usize,
) -> Result<(RowGroup, Vec<Vec<PageWriteSpec>>, u64)>
where
    W: AsyncWrite + Unpin + Send,
    Error: From<E>,
    E: std::error::Error,
{
    let column_iter = descriptors.iter().zip(columns);

    let initial = offset;
    let mut columns = vec![];
    for (descriptor, page_iter) in column_iter {
        let (column, page_specs, size) =
            write_column_chunk_async(writer, offset, descriptor, page_iter?).await?;
        offset += size;
        columns.push((column, page_specs));
    }
    let bytes_written = offset - initial;

    let num_rows = compute_num_rows(&columns)?;

    // compute row group stats
    let file_offset = columns
        .get(0)
        .map(|(column_chunk, _)| {
            ColumnOffsetsMetadata::from_column_chunk(column_chunk).calc_row_group_file_offset()
        })
        .unwrap_or(None);

    let total_byte_size = columns
        .iter()
        .map(|(c, _)| c.meta_data.as_ref().unwrap().total_uncompressed_size)
        .sum();
    let total_compressed_size = columns
        .iter()
        .map(|(c, _)| c.meta_data.as_ref().unwrap().total_compressed_size)
        .sum();

    let (columns, specs) = columns.into_iter().unzip();

    Ok((
        RowGroup {
            columns,
            total_byte_size,
            num_rows: num_rows as i64,
            sorting_columns: None,
            file_offset,
            total_compressed_size: Some(total_compressed_size),
            ordinal: ordinal.try_into().ok(),
        },
        specs,
        bytes_written,
    ))
}
