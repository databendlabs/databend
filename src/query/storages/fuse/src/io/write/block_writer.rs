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

use std::collections::HashMap;
use std::sync::Arc;

use common_arrow::arrow::chunk::Chunk as ArrowChunk;
use common_arrow::native::write::NativeWriter;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::ColumnId;
use common_expression::DataBlock;
use common_expression::TableSchemaRef;
use common_io::constants::DEFAULT_BLOCK_BUFFER_SIZE;
use opendal::Operator;
use storages_common_blocks::blocks_to_parquet;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::ClusterStatistics;
use storages_common_table_meta::meta::ColumnMeta;

use crate::fuse_table::FuseStorageFormat;
use crate::io::write::WriteSettings;
use crate::io::TableMetaLocationGenerator;
use crate::operations::util;
use crate::operations::BloomIndexState;
use crate::statistics::gen_columns_statistics;
use crate::statistics::ClusterStatsGenerator;

// TODO rename this, it is serialization, or pass in a writer(if not rename)
pub fn serialize_block(
    write_settings: &WriteSettings,
    schema: &TableSchemaRef,
    block: DataBlock,
    buf: &mut Vec<u8>,
) -> Result<(u64, HashMap<ColumnId, ColumnMeta>)> {
    match write_settings.storage_format {
        FuseStorageFormat::Parquet => {
            let result =
                blocks_to_parquet(schema, vec![block], buf, write_settings.table_compression)?;
            let meta = util::column_parquet_metas(&result.1, schema)?;
            Ok((result.0, meta))
        }
        FuseStorageFormat::Native => {
            let arrow_schema = schema.to_arrow();
            let mut writer = NativeWriter::new(
                buf,
                arrow_schema,
                common_arrow::native::write::WriteOptions {
                    compression: write_settings.table_compression.into(),
                    max_page_size: Some(write_settings.max_page_size),
                },
            );

            let batch = ArrowChunk::try_from(block)?;

            writer.start()?;
            writer.write(&batch)?;
            writer.finish()?;

            let leaf_column_ids = schema.to_leaf_column_ids();
            let mut metas = HashMap::with_capacity(writer.metas.len());
            for (idx, meta) in writer.metas.iter().enumerate() {
                // use column id as key instead of index
                let column_id = leaf_column_ids.get(idx).unwrap();
                metas.insert(*column_id, ColumnMeta::Native(meta.clone()));
            }

            Ok((writer.total_size() as u64, metas))
        }
    }
}

/// Take ownership here to avoid extra copy.
#[async_backtrace::framed]
pub async fn write_data(data: Vec<u8>, data_accessor: &Operator, location: &str) -> Result<()> {
    data_accessor.write(location, data).await?;

    Ok(())
}

pub struct BlockSerialization {
    pub block_raw_data: Vec<u8>,
    pub size: u64, // TODO redundancy
    pub block_meta: BlockMeta,
    pub bloom_index_state: Option<BloomIndexState>,
}

#[derive(Clone)]
pub struct BlockBuilder {
    pub ctx: Arc<dyn TableContext>,
    pub meta_locations: TableMetaLocationGenerator,
    pub source_schema: TableSchemaRef,
    pub write_settings: WriteSettings,
    pub cluster_stats_gen: ClusterStatsGenerator,
}

impl BlockBuilder {
    pub fn build<F>(&self, data_block: DataBlock, f: F) -> Result<BlockSerialization>
    where F: Fn(DataBlock, &ClusterStatsGenerator) -> Result<(Option<ClusterStatistics>, DataBlock)>
    {
        let (cluster_stats, data_block) = f(data_block, &self.cluster_stats_gen)?;
        let (block_location, block_id) = self.meta_locations.gen_block_location();

        let bloom_index_location = self.meta_locations.block_bloom_index_location(&block_id);
        let bloom_index_state = BloomIndexState::try_create(
            self.ctx.clone(),
            self.source_schema.clone(),
            &data_block,
            bloom_index_location,
        )?;
        let column_distinct_count = bloom_index_state
            .as_ref()
            .map(|i| i.column_distinct_count.clone());

        let row_count = data_block.num_rows() as u64;
        let block_size = data_block.memory_size() as u64;
        let col_stats =
            gen_columns_statistics(&data_block, column_distinct_count, &self.source_schema)?;

        let mut buffer = Vec::with_capacity(DEFAULT_BLOCK_BUFFER_SIZE);
        let (file_size, col_metas) = serialize_block(
            &self.write_settings,
            &self.source_schema,
            data_block,
            &mut buffer,
        )?;

        let block_meta = BlockMeta {
            row_count,
            block_size,
            file_size,
            col_stats,
            col_metas,
            cluster_stats,
            location: block_location,
            bloom_filter_index_location: bloom_index_state.as_ref().map(|v| v.location.clone()),
            bloom_filter_index_size: bloom_index_state
                .as_ref()
                .map(|v| v.size)
                .unwrap_or_default(),
            compression: self.write_settings.table_compression.try_into()?,
        };

        let serialized = BlockSerialization {
            block_raw_data: buffer,
            size: file_size,
            block_meta,
            bloom_index_state,
        };
        Ok(serialized)
    }
}
