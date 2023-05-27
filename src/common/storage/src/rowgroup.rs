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

use std::sync::Arc;

use arrow_schema::Schema;
use bytes::Buf;
use bytes::Bytes;
use parquet::arrow::array_reader::build_array_reader;
use parquet::arrow::array_reader::RowGroupCollection;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::arrow_to_parquet_schema;
use parquet::arrow::schema::parquet_to_array_schema_and_fields;
use parquet::arrow::schema::ParquetField;
use parquet::arrow::schema::ParquetFieldType;
use parquet::arrow::ProjectionMask;
use parquet::column::page::PageIterator;
use parquet::column::page::PageReader;
use parquet::errors::ParquetError;
use parquet::errors::Result;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::reader::ChunkReader;
use parquet::file::reader::Length;
use parquet::file::serialized_reader::SerializedPageReader;
use parquet::format::PageLocation;
use parquet::schema::types::ColumnDescPtr;
use parquet::schema::types::SchemaDescPtr;
use parquet::schema::types::SchemaDescriptor;

/// Implements [`PageIterator`] for a single column chunk, yielding a single [`PageReader`]
struct ColumnChunkIterator {
    schema: SchemaDescPtr,
    column_schema: ColumnDescPtr,
    reader: Option<Result<Box<dyn PageReader>>>,
}

impl Iterator for ColumnChunkIterator {
    type Item = Result<Box<dyn PageReader>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.reader.take()
    }
}

impl PageIterator for ColumnChunkIterator {
    fn schema(&mut self) -> Result<SchemaDescPtr> {
        Ok(self.schema.clone())
    }

    fn column_schema(&mut self) -> Result<ColumnDescPtr> {
        Ok(self.column_schema.clone())
    }
}

/// An in-memory column chunk
#[derive(Clone)]
pub enum ColumnChunkData {
    /// Column chunk data representing only a subset of data pages
    Sparse {
        /// Length of the full column chunk
        length: usize,
        /// Set of data pages included in this sparse chunk. Each element is a tuple
        /// of (page offset, page data)
        data: Vec<(usize, Bytes)>,
    },
    /// Full column chunk and its offset
    Dense { offset: usize, data: Bytes },
}

impl ColumnChunkData {
    fn get(&self, start: u64) -> Result<Bytes> {
        match &self {
            ColumnChunkData::Sparse { data, .. } => data
                .binary_search_by_key(&start, |(offset, _)| *offset as u64)
                .map(|idx| data[idx].1.clone())
                .map_err(|_| {
                    ParquetError::General(format!(
                        "Invalid offset in sparse column chunk data: {start}"
                    ))
                }),
            ColumnChunkData::Dense { offset, data } => {
                let start = start as usize - *offset;
                Ok(data.slice(start..))
            }
        }
    }
}

impl Length for ColumnChunkData {
    fn len(&self) -> u64 {
        match &self {
            ColumnChunkData::Sparse { length, .. } => *length as u64,
            ColumnChunkData::Dense { data, .. } => data.len() as u64,
        }
    }
}

impl ChunkReader for ColumnChunkData {
    type T = bytes::buf::Reader<Bytes>;

    fn get_read(&self, start: u64, _lenght: usize) -> Result<Self::T> {
        Ok(self.get(start)?.reader())
    }

    fn get_bytes(&self, start: u64, length: usize) -> Result<Bytes> {
        Ok(self.get(start)?.slice(..length))
    }
}

pub struct InMemoryRowGroup<'a> {
    pub schema: Schema,
    pub fields: Option<ParquetField>,
    pub metadata: &'a RowGroupMetaData,
    pub column_chunks: Vec<Option<Arc<ColumnChunkData>>>,
    pub row_count: usize,
}

impl<'a> InMemoryRowGroup<'a> {
    pub fn create(
        metadata: &'a RowGroupMetaData,
        column_chunks: Vec<Option<Arc<ColumnChunkData>>>,
        row_count: usize,
    ) -> Result<Self> {
        let (schema, fields) = parquet_to_array_schema_and_fields(
            &metadata.schema_descr_ptr(),
            ProjectionMask::all(),
            None,
        )?;

        Ok(Self {
            schema,
            fields,
            metadata,
            column_chunks,
            row_count,
        })
    }

    pub fn set_column_data(&mut self, offset: usize, data: Bytes) {
        self.column_chunks[offset] = Some(Arc::new(ColumnChunkData::Dense { offset, data }));
    }

    pub fn build_reader(
        self,
        mask: ProjectionMask,
        batch_size: usize,
    ) -> Result<ParquetRecordBatchReader> {
        let reader = ParquetRecordBatchReader::new(
            batch_size,
            build_array_reader(self.fields.as_ref(), &mask, &self)?,
            None,
        );

        Ok(reader)
    }
}

impl<'a> RowGroupCollection for InMemoryRowGroup<'a> {
    fn schema(&self) -> SchemaDescPtr {
        self.metadata.schema_descr_ptr()
    }

    fn num_rows(&self) -> usize {
        self.row_count
    }

    fn column_chunks(&self, i: usize) -> Result<Box<dyn PageIterator>> {
        match &self.column_chunks[i] {
            None => Err(ParquetError::General(format!(
                "Invalid column index {i}, column was not fetched"
            ))),
            Some(data) => {
                let column_meta = self.metadata.column(i);
                let page_reader: Box<dyn PageReader> = Box::new(SerializedPageReader::new(
                    data.clone(),
                    self.metadata.column(i),
                    self.row_count,
                    None,
                )?);

                Ok(Box::new(ColumnChunkIterator {
                    schema: self.metadata.schema_descr_ptr(),
                    column_schema: self.metadata.schema_descr_ptr().columns()[i].clone(),
                    reader: Some(Ok(page_reader)),
                }))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xrow_group() {}
}
