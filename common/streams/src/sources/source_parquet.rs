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
use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow::io::parquet::read::decompress;
use common_arrow::arrow::io::parquet::read::page_stream_to_array;
use common_arrow::arrow::io::parquet::read::read_metadata_async;
use common_arrow::arrow::io::parquet::read::schema::FileMetaData;
use common_arrow::parquet::read::get_page_stream;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_tracing::tracing;
use common_tracing::tracing::debug_span;
use common_tracing::tracing::Instrument;
use futures::AsyncRead;
use futures::AsyncSeek;
use futures::StreamExt;

use crate::Source;

#[derive(Debug, Clone)]
pub struct ParquetSourceBuilder {
    schema: DataSchemaRef,
    projection: Vec<usize>,
    metadata: Option<FileMetaData>,
}

impl ParquetSourceBuilder {
    pub fn create(schema: DataSchemaRef) -> Self {
        ParquetSourceBuilder {
            schema,
            projection: vec![],
            metadata: None,
        }
    }

    pub fn projection(&mut self, projection: Vec<usize>) -> &mut Self {
        self.projection = projection;
        self
    }

    pub fn meta_data(&mut self, meta_data: Option<FileMetaData>) -> &mut Self {
        self.metadata = meta_data;
        self
    }

    pub fn build<R>(&self, reader: R) -> Result<ParquetSource<R>>
    where R: AsyncRead + AsyncSeek + Unpin + Send {
        Ok(ParquetSource::create(self.clone(), reader))
    }
}

pub struct ParquetSource<R> {
    reader: R,
    builder: ParquetSourceBuilder,
    current_row_group: usize,
    arrow_table_schema: ArrowSchema,
}

impl<R> ParquetSource<R>
where R: AsyncRead + AsyncSeek + Unpin + Send
{
    fn create(builder: ParquetSourceBuilder, reader: R) -> Self {
        let arrow_table_schema =
            Arc::new(builder.schema.project(builder.projection.clone())).to_arrow();

        ParquetSource {
            reader,
            builder,
            arrow_table_schema,
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
        let metadata = match &self.builder.metadata {
            Some(m) => m,
            None => {
                fetched_metadata = read_metadata_async(&mut self.reader)
                    .instrument(debug_span!("parquet_source_read_meta"))
                    .await
                    .map_err(|e| ErrorCode::ParquetError(e.to_string()))?;
                self.builder.metadata = Some(fetched_metadata);
                match self.builder.metadata.as_ref() {
                    Some(m) => m,
                    _ => unreachable!(),
                }
            }
        };

        if self.current_row_group >= metadata.row_groups.len() {
            return Ok(None);
        }

        let fields = self.arrow_table_schema.fields();
        let row_grp = &metadata.row_groups[self.current_row_group];
        let cols = self
            .builder
            .projection
            .clone()
            .into_iter()
            .map(|idx| (row_grp.column(idx).clone(), idx));
        let mut data_cols = Vec::with_capacity(cols.len());
        for (col_meta, idx) in cols {
            let col_pages =
                get_page_stream(&col_meta, &mut self.reader, vec![], Arc::new(|_, _| true))
                    .instrument(debug_span!("parquet_source_get_column_page"))
                    .await
                    .map_err(|e| ErrorCode::ParquetError(e.to_string()))?;
            let pages = col_pages.map(|compressed_page| decompress(compressed_page?, &mut vec![]));
            let array = page_stream_to_array(pages, &col_meta, fields[idx].data_type.clone())
                .instrument(debug_span!("parquet_source_page_stream_to_array"))
                .await?;
            let array: Arc<dyn common_arrow::arrow::array::Array> = array.into();

            let column = match fields[idx].nullable {
                false => array.into_column(),
                true => array.into_nullable_column(),
            };
            data_cols.push(column);
        }
        self.current_row_group += 1;
        let block = DataBlock::create(self.builder.schema.clone(), data_cols);
        Ok(Some(block))
    }
}
