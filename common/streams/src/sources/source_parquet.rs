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
use common_dal::DataAccessor;
use common_datablocks::DataBlock;
use common_datavalues::prelude::DataColumn;
use common_datavalues::series::IntoSeries;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_tracing::tracing;
use common_tracing::tracing::debug_span;
use common_tracing::tracing::Instrument;
use futures::io::BufReader;
use futures::StreamExt;
use futures::TryStreamExt;

use crate::Source;

pub struct ParquetSource {
    data_accessor: Arc<dyn DataAccessor>,
    path: String,

    block_schema: DataSchemaRef,
    arrow_table_schema: ArrowSchema,
    projection: Vec<usize>,
    row_group: usize,
    row_groups: usize,
    metadata: Option<FileMetaData>,
    decompress_in_thread_pool: bool,
    stream_len: Option<u64>,
    read_buffer_size: Option<u64>,
}

impl ParquetSource {
    pub fn new(
        data_accessor: Arc<dyn DataAccessor>,
        path: String,
        table_schema: DataSchemaRef,
        projection: Vec<usize>,
    ) -> Self {
        let block_schema = Arc::new(table_schema.project(projection.clone()));
        Self {
            data_accessor,
            path,
            block_schema,
            arrow_table_schema: table_schema.to_arrow(),
            projection,
            row_group: 0,
            row_groups: 0,
            metadata: None,
            decompress_in_thread_pool: false,
            stream_len: None,
            read_buffer_size: None,
        }
    }

    pub fn with_meta(
        data_accessor: Arc<dyn DataAccessor>,
        path: String,
        table_schema: DataSchemaRef,
        projection: Vec<usize>,
        metadata: FileMetaData,
        file_len: u64,
        buffer_size: u64,
    ) -> Self {
        let block_schema = Arc::new(table_schema.project(projection.clone()));
        Self {
            data_accessor,
            path,
            block_schema,
            arrow_table_schema: table_schema.to_arrow(),
            projection,
            row_group: 0,
            row_groups: 0,
            metadata: Some(metadata),
            decompress_in_thread_pool: false,
            stream_len: Some(file_len),
            read_buffer_size: Some(buffer_size),
        }
    }

    pub fn enable_decompress_in_pool(&mut self) {
        self.decompress_in_thread_pool = true
    }
}

#[async_trait]
impl Source for ParquetSource {
    #[tracing::instrument(level = "debug", skip_all)]
    async fn read(&mut self) -> Result<Option<DataBlock>> {
        let extern_meta;
        let metadata = match &self.metadata {
            Some(m) => m,
            None => {
                let mut reader = self
                    .data_accessor
                    .get_input_stream(self.path.as_str(), None)?;
                extern_meta = read_metadata_async(&mut reader)
                    .instrument(debug_span!("parquet_source_read_meta"))
                    .await
                    .map_err(|e| ErrorCode::ParquetError(e.to_string()))?;
                &extern_meta
            }
        };

        self.row_groups = metadata.row_groups.len();
        self.row_group = 0;

        if self.row_group >= self.row_groups {
            return Ok(None);
        }
        let col_num = self.projection.len();
        let row_group = self.row_group;
        let cols = self
            .projection
            .clone()
            .into_iter()
            .map(|idx| (metadata.row_groups[row_group].column(idx).clone(), idx));

        let fields = self.arrow_table_schema.fields();
        let dedicated_decompression_thread_pool = self.decompress_in_thread_pool;
        let stream_len = self.stream_len;
        let read_buffer_size = self.read_buffer_size.unwrap_or(10 * 1024 * 1024);

        let stream = futures::stream::iter(cols).map(|(col_meta, idx)| {
            let data_accessor = self.data_accessor.clone();
            let path = self.path.clone();

            async move {
                let reader = data_accessor.get_input_stream(path.as_str(), stream_len)?;
                let mut reader = BufReader::with_capacity(read_buffer_size as usize, reader);
                // TODO cache block column
                let col_pages =
                    get_page_stream(&col_meta, &mut reader, vec![], Arc::new(|_, _| true))
                        .instrument(debug_span!("parquet_source_get_column_page"))
                        .await
                        .map_err(|e| ErrorCode::ParquetError(e.to_string()))?;
                let array = if dedicated_decompression_thread_pool {
                    let pages = col_pages.then(|compressed_page| async {
                        tokio_rayon::spawn(|| decompress(compressed_page?, &mut vec![])).await
                    });
                    page_stream_to_array(pages, &col_meta, fields[idx].data_type.clone())
                        .instrument(debug_span!("parquet_source_page_stream_to_array_rayon"))
                        .await?
                } else {
                    let pages = col_pages.map(|compressed_page| {
                        debug_span!("parquet_source_decompress_page")
                            .in_scope(|| decompress(compressed_page?, &mut vec![]))
                    });
                    page_stream_to_array(pages, &col_meta, fields[idx].data_type.clone())
                        .instrument(debug_span!("parquet_source_page_stream_to_array"))
                        .await?
                };
                let array: Arc<dyn common_arrow::arrow::array::Array> = array.into();
                Ok::<_, ErrorCode>(DataColumn::Array(array.into_series()))
            }
            .instrument(debug_span!("parquet_source_read_column").or_current())
        });

        // TODO configuration of the buffer size
        let buffer_size = 10;
        let n = std::cmp::min(buffer_size, col_num);
        let data_cols = stream.buffered(n).try_collect().await?;

        self.row_group += 1;
        let block = DataBlock::create(self.block_schema.clone(), data_cols);
        Ok(Some(block))
    }
}
