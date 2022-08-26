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

use std::sync::Arc;

use common_arrow::parquet::compression::CompressionOptions;
use common_arrow::parquet::metadata::ThriftFileMetaData;
use common_catalog::table_context::TableContext;
use common_datablocks::serialize_data_blocks;
use common_datablocks::serialize_data_blocks_with_compression;
use common_datablocks::DataBlock;
use common_exception::Result;
use common_fuse_meta::meta::BlockMeta;
use common_fuse_meta::meta::ClusterStatistics;
use common_fuse_meta::meta::Location;
use opendal::Operator;
use tracing::warn;
use uuid::Uuid;

use crate::index::BloomFilterIndexer;
use crate::io::retry;
use crate::io::retry::Retryable;
use crate::io::TableMetaLocationGenerator;
use crate::operations::util;
use crate::statistics::gen_columns_statistics;

const DEFAULT_BLOOM_INDEX_WRITE_BUFFER_SIZE: usize = 300 * 1024;
const DEFAULT_BLOCK_WRITE_BUFFER_SIZE: usize = 100 * 1024 * 1024;

pub struct BlockWriter<'a> {
    ctx: &'a Arc<dyn TableContext>,
    location_generator: &'a TableMetaLocationGenerator,
    data_accessor: &'a Operator,
}

impl<'a> BlockWriter<'a> {
    pub fn new(
        ctx: &'a Arc<dyn TableContext>,
        data_accessor: &'a Operator,
        location_generator: &'a TableMetaLocationGenerator,
    ) -> Self {
        Self {
            ctx,
            location_generator,
            data_accessor,
        }
    }

    pub async fn write_with_location(
        &self,
        block: DataBlock,
        block_id: Uuid,
        location: Location,
        cluster_stats: Option<ClusterStatistics>,
        chunk_size: usize,
    ) -> Result<BlockMeta> {
        let data_accessor = &self.data_accessor;
        let row_count = block.num_rows() as u64;
        let block_size = block.memory_size() as u64;
        let col_stats = gen_columns_statistics(&block)?;
        let (bloom_filter_index_size, bloom_filter_index_location) = self
            .build_block_index(data_accessor, &block, block_id)
            .await?;
        let (file_size, file_meta_data) =
            write_block(block, data_accessor, &location.0, chunk_size).await?;
        let col_metas = util::column_metas(&file_meta_data)?;
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

    pub async fn write(
        &self,
        block: DataBlock,
        cluster_stats: Option<ClusterStatistics>,
        chunk_size: usize,
    ) -> Result<BlockMeta> {
        let (location, block_id) = self.location_generator.gen_block_location();
        self.write_with_location(block, block_id, location, cluster_stats, chunk_size)
            .await
    }

    pub async fn build_block_index(
        &self,
        data_accessor: &Operator,
        block: &DataBlock,
        block_id: Uuid,
    ) -> Result<(u64, Location)> {
        let bloom_index = BloomFilterIndexer::try_create(self.ctx.clone(), &[block])?;
        let index_block = bloom_index.bloom_block;
        let location = self
            .location_generator
            .block_bloom_index_location(&block_id);
        let mut data = Vec::with_capacity(DEFAULT_BLOOM_INDEX_WRITE_BUFFER_SIZE);
        let index_block_schema = &bloom_index.bloom_schema;
        let (size, _) = serialize_data_blocks_with_compression(
            vec![index_block],
            &index_block_schema,
            &mut data,
            1024 * 1024, // TODO
            CompressionOptions::Uncompressed,
        )?;
        write_data(&data, data_accessor, &location.0).await?;
        Ok((size, location))
    }
}

pub async fn write_block(
    block: DataBlock,
    data_accessor: &Operator,
    location: &str,
    chunk_size: usize,
) -> Result<(u64, ThriftFileMetaData)> {
    let mut buf = Vec::with_capacity(DEFAULT_BLOCK_WRITE_BUFFER_SIZE);
    // let blocks = DataBlock::split_block_by_size(&block, chunk_size)?;
    let schema = block.schema().clone();
    let result = serialize_data_blocks(vec![block], &schema, chunk_size, &mut buf)?;
    write_data(&buf, data_accessor, location).await?;
    Ok(result)
}

pub async fn write_data(data: &[u8], data_accessor: &Operator, location: &str) -> Result<()> {
    let op = || async {
        data_accessor
            .object(location)
            .write(data)
            .await
            .map_err(retry::from_io_error)
    };

    let notify = |e: std::io::Error, duration| {
        warn!(
            "transient error encountered while write block, location {}, at duration {:?} : {}",
            location, duration, e,
        )
    };

    op.retry_with_notify(notify).await?;

    Ok(())
}
