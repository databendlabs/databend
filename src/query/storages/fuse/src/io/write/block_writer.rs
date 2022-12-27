//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::collections::HashMap;

use backon::ExponentialBackoff;
use backon::Retryable;
use common_arrow::arrow::chunk::Chunk as ArrowChunk;
use common_arrow::native::write::PaWriter;
use common_arrow::parquet::compression::CompressionOptions;
use common_exception::Result;
use common_expression::serialize_to_parquet;
use common_expression::serialize_to_parquet_with_compression;
use common_expression::Chunk;
use common_expression::FunctionContext;
use common_expression::TableSchemaRef;
use common_storages_table_meta::meta::BlockMeta;
use common_storages_table_meta::meta::ClusterStatistics;
use common_storages_table_meta::meta::ColumnId;
use common_storages_table_meta::meta::ColumnMeta;
use common_storages_table_meta::meta::Location;
use common_storages_table_meta::meta::StatisticsOfColumns;
use opendal::Operator;
use tracing::warn;
use uuid::Uuid;

use crate::fuse_table::FuseStorageFormat;
use crate::index::ChunkFilter;
use crate::io::TableMetaLocationGenerator;
use crate::operations::util;

const DEFAULT_BLOOM_INDEX_WRITE_BUFFER_SIZE: usize = 300 * 1024;
const DEFAULT_BLOCK_WRITE_BUFFER_SIZE: usize = 100 * 1024 * 1024;

pub struct BlockWriter<'a> {
    schema: &'a TableSchemaRef,
    location_generator: &'a TableMetaLocationGenerator,
    data_accessor: &'a Operator,
}

impl<'a> BlockWriter<'a> {
    pub fn new(
        schema: &'a TableSchemaRef,
        data_accessor: &'a Operator,
        location_generator: &'a TableMetaLocationGenerator,
    ) -> Self {
        Self {
            schema,
            location_generator,
            data_accessor,
        }
    }

    pub async fn write(
        &self,
        storage_format: FuseStorageFormat,
        chunk: Chunk,
        col_stats: StatisticsOfColumns,
        cluster_stats: Option<ClusterStatistics>,
    ) -> Result<BlockMeta> {
        let (location, chunk_id) = self.location_generator.gen_block_location();

        let data_accessor = &self.data_accessor;
        let row_count = chunk.num_rows() as u64;
        let block_size = chunk.memory_size() as u64;
        let (bloom_filter_index_size, bloom_filter_index_location) = self
            .build_chunk_index(data_accessor, &chunk, chunk_id)
            .await?;

        let mut buf = Vec::with_capacity(DEFAULT_BLOCK_WRITE_BUFFER_SIZE);
        let (file_size, col_metas) = write_block(storage_format, self.schema, chunk, &mut buf)?;

        write_data(&buf, data_accessor, &location.0).await?;
        let block_meta = BlockMeta::new(
            row_count,
            block_size,
            file_size,
            col_stats,
            col_metas,
            cluster_stats,
            location,
            Some(bloom_filter_index_location),
            bloom_filter_index_size,
        );
        Ok(block_meta)
    }

    pub async fn build_chunk_index(
        &self,
        data_accessor: &Operator,
        chunk: &Chunk,
        chunk_id: Uuid,
    ) -> Result<(u64, Location)> {
        let bloom_index =
            ChunkFilter::try_create(FunctionContext::default(), self.schema.clone(), &[chunk])?;

        let index_chunk = bloom_index.filter_chunk;
        let location = self
            .location_generator
            .block_bloom_index_location(&chunk_id);
        let mut data = Vec::with_capacity(DEFAULT_BLOOM_INDEX_WRITE_BUFFER_SIZE);
        let index_block_schema = &bloom_index.filter_schema;
        let (size, _) = serialize_to_parquet_with_compression(
            vec![index_chunk],
            index_block_schema,
            &mut data,
            CompressionOptions::Uncompressed,
        )?;
        write_data(&data, data_accessor, &location.0).await?;
        Ok((size, location))
    }
}

pub fn write_block(
    storage_format: FuseStorageFormat,
    schema: &TableSchemaRef,
    chunk: Chunk,
    buf: &mut Vec<u8>,
) -> Result<(u64, HashMap<ColumnId, ColumnMeta>)> {
    match storage_format {
        FuseStorageFormat::Parquet => {
            let result = serialize_to_parquet(vec![chunk], schema, buf)?;
            let meta = util::column_metas(&result.1)?;
            Ok((result.0, meta))
        }
        FuseStorageFormat::Native => {
            let arrow_schema = schema.to_arrow();
            let mut writer = PaWriter::new(
                buf,
                arrow_schema,
                common_arrow::native::write::WriteOptions {
                    compression: common_arrow::native::Compression::LZ4,
                    max_page_size: Some(8192),
                },
            );

            let batch = ArrowChunk::try_from(chunk)?;

            writer.start()?;
            writer.write(&batch)?;
            writer.finish()?;

            let metas = writer
                .metas
                .iter()
                .enumerate()
                .map(|(idx, meta)| {
                    (
                        idx as ColumnId,
                        ColumnMeta::new(meta.offset, meta.length, meta.num_values),
                    )
                })
                .collect();
            Ok((writer.total_size() as u64, metas))
        }
    }
}

pub async fn write_data(data: &[u8], data_accessor: &Operator, location: &str) -> Result<()> {
    let object = data_accessor.object(location);

    { || object.write(data) }
        .retry(ExponentialBackoff::default().with_jitter())
        .when(|err| err.is_temporary())
        .notify(|err, dur| {
            warn!(
                "fuse table block writer write_data retry after {}s for error {:?}",
                dur.as_secs(),
                err
            )
        })
        .await?;

    Ok(())
}
