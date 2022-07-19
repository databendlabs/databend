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
//

use common_arrow::parquet::metadata::ThriftFileMetaData;
use common_datablocks::serialize_data_blocks;
use common_datablocks::DataBlock;
use common_exception::Result;
use common_fuse_meta::meta::BlockMeta;
use common_fuse_meta::meta::Versioned;
use common_tracing::tracing::warn;
use opendal::Operator;

use crate::storages::fuse::io::retry;
use crate::storages::fuse::io::retry::Retryable;
use crate::storages::fuse::io::TableMetaLocationGenerator;
use crate::storages::fuse::operations::util;
use crate::storages::fuse::statistics::accumulator;

pub struct BlockWriter<'a> {
    location_generator: &'a TableMetaLocationGenerator,
    data_accessor: &'a Operator,
}

impl<'a> BlockWriter<'a> {
    pub fn new(
        data_accessor: &'a Operator,
        location_generator: &'a TableMetaLocationGenerator,
    ) -> Self {
        Self {
            location_generator,
            data_accessor,
        }
    }
    pub async fn write(&self, block: DataBlock) -> Result<BlockMeta> {
        let location = self.location_generator.gen_block_location();
        let data_accessor = &self.data_accessor;
        let row_count = block.num_rows() as u64;
        let block_size = block.memory_size() as u64;
        let col_stats = accumulator::columns_statistics(&block)?;
        let (file_size, file_meta_data) = write_block(block, data_accessor, &location).await?;
        let col_metas = util::column_metas(&file_meta_data)?;
        let cluster_stats = None; // TODO confirm this with zhyass
        let location = (location, DataBlock::VERSION);
        let block_meta = BlockMeta::new(
            row_count,
            block_size,
            file_size,
            col_stats,
            col_metas,
            cluster_stats,
            location,
        );
        Ok(block_meta)
    }
}

pub async fn write_block(
    block: DataBlock,
    data_accessor: &Operator,
    location: &str,
) -> Result<(u64, ThriftFileMetaData)> {
    // we need a configuration of block size threshold here
    let mut buf = Vec::with_capacity(100 * 1024 * 1024);
    let schema = block.schema().clone();
    let result = serialize_data_blocks(vec![block], &schema, &mut buf)?;
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
