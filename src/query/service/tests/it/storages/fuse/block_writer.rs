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

use common_datablocks::DataBlock;
use common_exception::Result;
use common_storages_common::blocks_to_parquet;
use common_storages_fuse::io::write_block;
use common_storages_fuse::io::write_data;
use common_storages_fuse::io::TableMetaLocationGenerator;
use common_storages_fuse::io::WriteSettings;
use common_storages_fuse::FuseStorageFormat;
use common_storages_index::BlockFilter;
use common_storages_table_meta::meta::BlockMeta;
use common_storages_table_meta::meta::ClusterStatistics;
use common_storages_table_meta::meta::Compression;
use common_storages_table_meta::meta::Location;
use common_storages_table_meta::meta::StatisticsOfColumns;
use common_storages_table_meta::table::TableCompression;
use opendal::Operator;
use uuid::Uuid;

const DEFAULT_BLOOM_INDEX_WRITE_BUFFER_SIZE: usize = 300 * 1024;
const DEFAULT_BLOCK_WRITE_BUFFER_SIZE: usize = 100 * 1024 * 1024;

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

    pub async fn write(
        &self,
        storage_format: FuseStorageFormat,
        block: DataBlock,
        col_stats: StatisticsOfColumns,
        cluster_stats: Option<ClusterStatistics>,
    ) -> Result<BlockMeta> {
        let (location, block_id) = self.location_generator.gen_block_location();

        let data_accessor = &self.data_accessor;
        let row_count = block.num_rows() as u64;
        let block_size = block.memory_size() as u64;
        let (bloom_filter_index_size, bloom_filter_index_location) = self
            .build_block_index(data_accessor, &block, block_id)
            .await?;

        let write_settings = WriteSettings {
            storage_format,
            ..Default::default()
        };

        let mut buf = Vec::with_capacity(DEFAULT_BLOCK_WRITE_BUFFER_SIZE);
        let (file_size, col_metas) = write_block(&write_settings, block, &mut buf)?;

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
            Compression::Lz4Raw,
        );
        Ok(block_meta)
    }

    pub async fn build_block_index(
        &self,
        data_accessor: &Operator,
        block: &DataBlock,
        block_id: Uuid,
    ) -> Result<(u64, Location)> {
        let bloom_index = BlockFilter::try_create(&[block])?;
        let index_block = bloom_index.filter_block;
        let location = self
            .location_generator
            .block_bloom_index_location(&block_id);
        let mut data = Vec::with_capacity(DEFAULT_BLOOM_INDEX_WRITE_BUFFER_SIZE);
        let index_block_schema = &bloom_index.filter_schema;
        let (size, _) = blocks_to_parquet(
            index_block_schema,
            vec![index_block],
            &mut data,
            TableCompression::None,
        )?;
        write_data(&data, data_accessor, &location.0).await?;
        Ok((size, location))
    }
}
