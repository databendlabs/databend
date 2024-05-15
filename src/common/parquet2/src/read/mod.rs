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

mod column;
mod compression;
mod indexes;
pub mod levels;
mod metadata;
mod page;
#[cfg(feature = "async")]
mod stream;

use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::sync::Arc;

pub use column::*;
pub use compression::decompress;
pub use compression::BasicDecompressor;
pub use compression::Decompressor;
pub use indexes::read_columns_indexes;
pub use indexes::read_pages_locations;
pub use metadata::deserialize_metadata;
pub use metadata::read_metadata;
pub use metadata::read_metadata_with_size;
#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
pub use page::get_page_stream;
#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
pub use page::get_page_stream_from_column_start;
pub use page::IndexedPageReader;
pub use page::PageFilter;
pub use page::PageIterator;
pub use page::PageMetaData;
pub use page::PageReader;
#[cfg(feature = "async")]
#[cfg_attr(docsrs, doc(cfg(feature = "async")))]
pub use stream::read_metadata as read_metadata_async;

use crate::error::Result;
use crate::metadata::ColumnChunkMetaData;
use crate::metadata::FileMetaData;
use crate::metadata::RowGroupMetaData;

/// Filters row group metadata to only those row groups,
/// for which the predicate function returns true
pub fn filter_row_groups(
    metadata: &FileMetaData,
    predicate: &dyn Fn(&RowGroupMetaData, usize) -> bool,
) -> FileMetaData {
    let mut filtered_row_groups = Vec::<RowGroupMetaData>::new();
    for (i, row_group_metadata) in metadata.row_groups.iter().enumerate() {
        if predicate(row_group_metadata, i) {
            filtered_row_groups.push(row_group_metadata.clone());
        }
    }
    let mut metadata = metadata.clone();
    metadata.row_groups = filtered_row_groups;
    metadata
}

/// Returns a new [`PageReader`] by seeking `reader` to the begining of `column_chunk`.
pub fn get_page_iterator<R: Read + Seek>(
    column_chunk: &ColumnChunkMetaData,
    mut reader: R,
    pages_filter: Option<PageFilter>,
    scratch: Vec<u8>,
    max_page_size: usize,
) -> Result<PageReader<R>> {
    let pages_filter = pages_filter.unwrap_or_else(|| Arc::new(|_, _| true));

    let (col_start, _) = column_chunk.byte_range();
    reader.seek(SeekFrom::Start(col_start))?;
    Ok(PageReader::new(
        reader,
        column_chunk,
        pages_filter,
        scratch,
        max_page_size,
    ))
}

/// Returns all [`ColumnChunkMetaData`] associated to `field_name`.
/// For non-nested types, this returns an iterator with a single column
pub fn get_field_columns<'a>(
    columns: &'a [ColumnChunkMetaData],
    field_name: &'a str,
) -> impl Iterator<Item = &'a ColumnChunkMetaData> {
    columns
        .iter()
        .filter(move |x| x.descriptor().path_in_schema[0] == field_name)
}
