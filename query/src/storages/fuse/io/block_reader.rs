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

use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_arrow::arrow::datatypes::Schema as ArrowSchema;
use common_arrow::arrow::io::parquet::read::decompress;
use common_arrow::arrow::io::parquet::read::page_stream_to_array;
use common_arrow::parquet::metadata::ColumnChunkMetaData;
use common_arrow::parquet::read::get_page_stream;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_tracing::tracing;
use common_tracing::tracing::debug_span;
use common_tracing::tracing::Instrument;
use futures::io::BufReader;
use futures::StreamExt;
use futures::TryStreamExt;
use opendal::readers::SeekableReader;
use opendal::Operator;

use crate::storages::fuse::io::meta_readers::BlockMetaReader;

pub struct BlockReader {
    data_accessor: Operator,
    path: String,
    block_schema: DataSchemaRef,
    table_schema: DataSchemaRef,
    arrow_table_schema: ArrowSchema,
    projection: Vec<usize>,
    file_len: u64,
    read_buffer_size: u64,
    metadata_reader: BlockMetaReader,
}

impl BlockReader {
    pub fn new(
        data_accessor: Operator,
        path: String,
        table_schema: DataSchemaRef,
        projection: Vec<usize>,
        file_len: u64,
        read_buffer_size: u64,
        reader: BlockMetaReader,
    ) -> Self {
        let block_schema = Arc::new(table_schema.project(projection.clone()));
        let arrow_table_schema = table_schema.to_arrow();
        Self {
            data_accessor,
            path,
            block_schema,
            table_schema,
            arrow_table_schema,
            projection,
            file_len,
            read_buffer_size,
            metadata_reader: reader,
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn read(&mut self) -> Result<DataBlock> {
        let block_meta = &self.metadata_reader.read(self.path.as_str()).await?;
        let metadata = block_meta.inner();

        // FUSE uses exact one "row group"
        let num_row_groups = metadata.row_groups.len();
        let row_group = if num_row_groups != 1 {
            return Err(ErrorCode::LogicalError(format!(
                "invalid parquet file, expect exact one row group insides, but got {}",
                num_row_groups
            )));
        } else {
            &metadata.row_groups[0]
        };

        let col_num = self.projection.len();
        let cols = self
            .projection
            .clone()
            .into_iter()
            .map(|idx| (row_group.column(idx).clone(), idx));

        let fields = self.table_schema.fields();
        let arrow_fields = self.arrow_table_schema.fields();
        let stream_len = self.file_len;
        let read_buffer_size = self.read_buffer_size;

        let stream = futures::stream::iter(cols).map(|(col_meta, idx)| {
            let data_accessor = self.data_accessor.clone();
            let path = self.path.clone();
            async move {
                let reader = SeekableReader::new(data_accessor, path.as_str(), stream_len);
                let reader = BufReader::with_capacity(read_buffer_size as usize, reader);
                let data_type = fields[idx].data_type();
                let arrow_type = arrow_fields[idx].data_type();
                Self::read_column(reader, &col_meta, data_type.clone(), arrow_type.clone()).await
            }
            .instrument(debug_span!("block_reader_read_column").or_current())
        });

        // TODO configuration of the buffer size
        let buffer_size = 10;
        let n = std::cmp::min(buffer_size, col_num);
        let data_cols = stream.buffered(n).try_collect().await?;

        let block = DataBlock::create(self.block_schema.clone(), data_cols);
        Ok(block)
    }

    async fn read_column(
        mut reader: BufReader<SeekableReader>,
        column_chunk_meta: &ColumnChunkMetaData,
        data_type: DataTypePtr,
        arrow_type: ArrowType,
    ) -> Result<ColumnRef> {
        let col_pages = get_page_stream(
            column_chunk_meta,
            &mut reader,
            vec![],
            Arc::new(|_, _| true),
        )
        .instrument(debug_span!("block_reader_get_column_page"))
        .await
        .map_err(|e| ErrorCode::ParquetError(e.to_string()))?;
        let pages = col_pages.map(|compressed_page| {
            debug_span!("block_reader_decompress_page")
                .in_scope(|| decompress(compressed_page?, &mut vec![]))
        });
        let array = page_stream_to_array(pages, column_chunk_meta, arrow_type)
            .instrument(debug_span!("block_reader_page_stream_to_array"))
            .await?;
        let array: Arc<dyn common_arrow::arrow::array::Array> = array.into();

        match data_type.is_nullable() {
            true => Ok(array.into_nullable_column()),
            false => Ok(array.into_column()),
        }
    }
}
