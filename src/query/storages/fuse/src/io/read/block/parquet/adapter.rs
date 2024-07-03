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

//! This module implements [`RowGroup`] trait from [`parquet_rs`], does not depend on other modules in our codebase.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use parquet::arrow::arrow_reader::RowGroups;
use parquet::basic::Compression;
use parquet::column::page::PageIterator;
use parquet::column::page::PageReader;
use parquet::errors::Result as ParquetResult;
use parquet::file::metadata::ColumnChunkMetaData;
use parquet::file::serialized_reader::SerializedPageReader;
use parquet::schema::types::SchemaDescriptor;

pub struct RowGroupImplBuilder<'a> {
    num_rows: usize,
    column_chunks: HashMap<usize, Bytes>,
    column_chunk_metadatas: HashMap<usize, ColumnChunkMetaData>,
    schema_descriptor: &'a SchemaDescriptor,
    compression: Compression,
}

impl<'a> RowGroupImplBuilder<'a> {
    pub fn new(
        num_rows: usize,
        schema_descriptor: &'a SchemaDescriptor,
        compression: Compression,
    ) -> Self {
        Self {
            num_rows,
            column_chunks: HashMap::new(),
            column_chunk_metadatas: HashMap::new(),
            schema_descriptor,
            compression,
        }
    }

    pub fn add_column_chunk(&mut self, dfs_id: usize, column_chunk: Bytes) {
        let column_chunk_metadata =
            ColumnChunkMetaData::builder(self.schema_descriptor.column(dfs_id))
                .set_compression(self.compression)
                .set_data_page_offset(0)
                .set_total_compressed_size(column_chunk.len() as i64)
                .build()
                .unwrap();
        self.column_chunk_metadatas
            .insert(dfs_id, column_chunk_metadata);
        self.column_chunks.insert(dfs_id, column_chunk);
    }

    pub fn build(self) -> RowGroupImpl {
        RowGroupImpl {
            num_rows: self.num_rows,
            column_chunks: self.column_chunks,
            column_chunk_metadatas: self.column_chunk_metadatas,
        }
    }
}

// A single row group
pub struct RowGroupImpl {
    num_rows: usize,
    column_chunks: HashMap<usize, Bytes>,
    column_chunk_metadatas: HashMap<usize, ColumnChunkMetaData>,
}

impl RowGroups for RowGroupImpl {
    /// Get the number of rows in this collection
    fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// Returns a [`PageIterator`] for the column chunk with the given leaf column index
    fn column_chunks(&self, i: usize) -> ParquetResult<Box<dyn PageIterator>> {
        let column_chunk = Arc::new(self.column_chunks.get(&i).unwrap().clone());
        let column_chunk_meta = self.column_chunk_metadatas.get(&i).unwrap();
        let page_reader = Box::new(SerializedPageReader::new(
            column_chunk,
            column_chunk_meta,
            self.num_rows(),
            None,
        )?);

        Ok(Box::new(PageIteratorImpl {
            reader: Some(Ok(page_reader)),
        }))
    }
}

struct PageIteratorImpl {
    reader: Option<ParquetResult<Box<dyn PageReader>>>,
}

impl Iterator for PageIteratorImpl {
    type Item = ParquetResult<Box<dyn PageReader>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.reader.take()
    }
}

impl PageIterator for PageIteratorImpl {}
