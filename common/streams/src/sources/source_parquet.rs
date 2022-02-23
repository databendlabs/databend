// Copyright 2021 Datafuse Labs.
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

use async_trait::async_trait;
use common_arrow::arrow::datatypes::Field;
use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow::io::parquet::read::read_metadata_async;
use common_arrow::arrow::io::parquet::read::schema::FileMetaData;
use common_arrow::arrow::io::parquet::read::RowGroupDeserializer;
use common_arrow::read_columns_many_async;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_tracing::tracing;
use common_tracing::tracing::debug_span;
use common_tracing::tracing::Instrument;
use futures::AsyncRead;
use futures::AsyncSeek;

use crate::Source;

pub struct ParquetSource<R> {
    reader: R,
    block_schema: DataSchemaRef,
    arrow_table_schema: ArrowSchema,
    projection: Vec<usize>,
    metadata: Option<FileMetaData>,
    current_row_group: usize,
}

impl<R> ParquetSource<R>
where R: AsyncRead + AsyncSeek + Unpin + Send
{
    pub fn new(reader: R, table_schema: DataSchemaRef, projection: Vec<usize>) -> Self {
        Self::with_meta(reader, table_schema, projection, None)
    }

    pub fn with_meta(
        reader: R,
        table_schema: DataSchemaRef,
        projection: Vec<usize>,
        metadata: Option<FileMetaData>,
    ) -> Self {
        let block_schema = Arc::new(table_schema.project(projection.clone()));
        ParquetSource {
            reader,
            block_schema,
            arrow_table_schema: table_schema.to_arrow(),
            projection,
            metadata,
            current_row_group: 0,
        }
    }
}

#[async_trait]
impl<R> Source for ParquetSource<R>
where R: AsyncRead + AsyncSeek + Unpin + Send
{
    #[tracing::instrument(level = "debug", skip_all)]
    async fn read(&mut self) -> Result<Option<DataBlock>> {
        let fetched_metadata;
        let metadata = match &self.metadata {
            Some(m) => m,
            None => {
                fetched_metadata = read_metadata_async(&mut self.reader)
                    .instrument(debug_span!("parquet_source_read_meta"))
                    .await
                    .map_err(|e| ErrorCode::ParquetError(e.to_string()))?;
                self.metadata = Some(fetched_metadata);
                match self.metadata.as_ref() {
                    Some(m) => m,
                    _ => unreachable!(),
                }
            }
        };

        if self.current_row_group >= metadata.row_groups.len() {
            return Ok(None);
        }

        let fields = &self.arrow_table_schema.fields;

        let row_group = &metadata.row_groups[self.current_row_group];
        let fields_to_read: Vec<&Field> = self
            .projection
            .clone()
            .into_iter()
            .map(|idx| &fields[idx])
            .collect();

        let column_chunks =
            read_columns_many_async(&mut self.reader, row_group, fields_to_read, None)
                .await
                .map_err(|e| ErrorCode::ParquetError(e.to_string()))?;

        let mut chunks =
            RowGroupDeserializer::new(column_chunks, row_group.num_rows() as usize, None);

        // expect exact one chunk
        let chunk = match chunks.next() {
            None => return Err(ErrorCode::ParquetError("fail to get a chunk")),
            Some(chunk) => chunk.map_err(|e| ErrorCode::ParquetError(e.to_string()))?,
        };

        let block = DataBlock::from_chunk(&self.block_schema, &chunk)?;
        self.current_row_group += 1;
        Ok(Some(block))
    }
}
