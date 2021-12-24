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
use common_tracing::tracing::debug_span;
use common_tracing::tracing::Instrument;
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
        }
    }
}

#[async_trait]
impl Source for ParquetSource {
    async fn read(&mut self) -> Result<Option<DataBlock>> {
        let metadata = match self.metadata.clone() {
            Some(m) => m,
            None => {
                let mut reader = self
                    .data_accessor
                    .get_input_stream(self.path.as_str(), None)?;
                let m = read_metadata_async(&mut reader)
                    .instrument(debug_span!("parquet_source_read_meta"))
                    .await
                    .map_err(|e| ErrorCode::ParquetError(e.to_string()))?;
                self.metadata = Some(m.clone());
                self.row_groups = m.row_groups.len();
                self.row_group = 0;
                m
            }
        };

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

        let stream = futures::stream::iter(cols).map(|(col_meta, idx)| {
            let data_accessor = self.data_accessor.clone();
            let path = self.path.clone();

            async move {
                let mut reader = data_accessor.get_input_stream(path.as_str(), None)?;
                // TODO cache block column
                let col_pages =
                    get_page_stream(&col_meta, &mut reader, vec![], Arc::new(|_, _| true))
                        .instrument(debug_span!("parquet_source_get_column_page"))
                        .await
                        .map_err(|e| ErrorCode::ParquetError(e.to_string()))?;
                let pages = col_pages.map(|compressed_page| {
                    debug_span!("parquet_source_decompress_page")
                        .in_scope(|| decompress(compressed_page?, &mut vec![]))
                });
                let array = page_stream_to_array(pages, &col_meta, fields[idx].data_type.clone())
                    .instrument(debug_span!("parquet_source_page_stream_to_array"))
                    .await?;
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
