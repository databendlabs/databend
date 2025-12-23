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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::time::Instant;

use chrono::Utc;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::FieldIndex;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::local_block_meta_serde;
use databend_common_io::constants::DEFAULT_BLOCK_BUFFER_SIZE;
use databend_common_metrics::storage::metrics_inc_block_index_write_milliseconds;
use databend_common_metrics::storage::metrics_inc_block_index_write_nums;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_write_bytes;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_write_milliseconds;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_write_nums;
use databend_common_metrics::storage::metrics_inc_block_vector_index_write_bytes;
use databend_common_metrics::storage::metrics_inc_block_vector_index_write_milliseconds;
use databend_common_metrics::storage::metrics_inc_block_vector_index_write_nums;
use databend_common_metrics::storage::metrics_inc_block_virtual_column_write_bytes;
use databend_common_metrics::storage::metrics_inc_block_virtual_column_write_milliseconds;
use databend_common_metrics::storage::metrics_inc_block_virtual_column_write_nums;
use databend_common_metrics::storage::metrics_inc_block_write_milliseconds;
use databend_common_metrics::storage::metrics_inc_block_write_nums;
use databend_common_native::write::NativeWriter;
use databend_storages_common_blocks::blocks_to_parquet_with_stats;
use databend_storages_common_index::NgramArgs;
use databend_storages_common_table_meta::meta::BlockHLLState;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::ExtendedBlockMeta;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::encode_column_hll;
use databend_storages_common_table_meta::table::TableCompression;
use opendal::Operator;

use crate::FuseStorageFormat;
use crate::io::BloomIndexState;
use crate::io::TableMetaLocationGenerator;
use crate::io::build_column_hlls;
use crate::io::write::InvertedIndexBuilder;
use crate::io::write::InvertedIndexState;
use crate::io::write::VectorIndexBuilder;
use crate::io::write::VectorIndexState;
use crate::io::write::WriteSettings;
use crate::io::write::virtual_column_builder::VirtualColumnBuilder;
use crate::io::write::virtual_column_builder::VirtualColumnState;
use crate::operations::column_parquet_metas;
use crate::statistics::ClusterStatsGenerator;
use crate::statistics::gen_columns_statistics;

pub fn serialize_block(
    write_settings: &WriteSettings,
    schema: &TableSchemaRef,
    block: DataBlock,
    buf: &mut Vec<u8>,
) -> Result<HashMap<ColumnId, ColumnMeta>> {
    serialize_block_with_column_stats(write_settings, schema, None, block, buf)
}

pub fn serialize_block_with_column_stats(
    write_settings: &WriteSettings,
    schema: &TableSchemaRef,
    column_stats: Option<&StatisticsOfColumns>,
    block: DataBlock,
    buf: &mut Vec<u8>,
) -> Result<HashMap<ColumnId, ColumnMeta>> {
    let schema = Arc::new(schema.remove_virtual_computed_fields());
    match write_settings.storage_format {
        FuseStorageFormat::Parquet => {
            let parquet_options = write_settings.parquet_options();
            let result = blocks_to_parquet_with_stats(
                &schema,
                vec![block],
                buf,
                &parquet_options,
                column_stats,
            )?;
            let meta = column_parquet_metas(&result, &schema)?;
            Ok(meta)
        }
        FuseStorageFormat::Native => {
            let leaf_column_ids = schema.to_leaf_column_ids();

            let mut default_compress_ratio = Some(2.10f64);
            if matches!(write_settings.table_compression, TableCompression::Zstd) {
                default_compress_ratio = Some(3.72f64);
            }

            let mut writer = NativeWriter::new(
                buf,
                schema.as_ref().clone(),
                databend_common_native::write::WriteOptions {
                    default_compression: write_settings.table_compression.into(),
                    max_page_size: Some(write_settings.max_page_size),
                    default_compress_ratio,
                    forbidden_compressions: vec![],
                },
            )?;

            let block = block.consume_convert_to_full();
            let batch: Vec<Column> = block
                .take_columns()
                .into_iter()
                .map(|x| x.into_column().unwrap())
                .collect();

            writer.start()?;
            writer.write(&batch)?;
            writer.finish()?;

            let mut metas = HashMap::with_capacity(writer.metas.len());
            for (idx, meta) in writer.metas.iter().enumerate() {
                // use column id as key instead of index
                let column_id = leaf_column_ids.get(idx).unwrap();
                metas.insert(*column_id, ColumnMeta::Native(meta.clone()));
            }

            Ok(metas)
        }
    }
}

/// Take ownership here to avoid extra copy.
#[async_backtrace::framed]
pub async fn write_data(data: Vec<u8>, data_accessor: &Operator, location: &str) -> Result<()> {
    data_accessor.write(location, data).await?;

    Ok(())
}

#[derive(Debug)]
pub struct BlockSerialization {
    pub block_raw_data: Vec<u8>,
    pub block_meta: BlockMeta,
    pub bloom_index_state: Option<BloomIndexState>,
    pub inverted_index_states: Vec<InvertedIndexState>,
    pub virtual_column_state: Option<VirtualColumnState>,
    pub vector_index_state: Option<VectorIndexState>,
    pub column_hlls: Option<BlockHLLState>,
}

local_block_meta_serde!(BlockSerialization);

#[typetag::serde(name = "block_serialization_meta")]
impl BlockMetaInfo for BlockSerialization {}

#[derive(Clone)]
pub struct BlockBuilder {
    pub ctx: Arc<dyn TableContext>,
    pub meta_locations: TableMetaLocationGenerator,
    pub source_schema: TableSchemaRef,
    pub write_settings: WriteSettings,
    pub cluster_stats_gen: ClusterStatsGenerator,
    pub bloom_columns_map: BTreeMap<FieldIndex, TableField>,
    pub ndv_columns_map: BTreeMap<FieldIndex, TableField>,
    pub ngram_args: Vec<NgramArgs>,
    pub inverted_index_builders: Vec<InvertedIndexBuilder>,
    pub virtual_column_builder: Option<VirtualColumnBuilder>,
    pub vector_index_builder: Option<VectorIndexBuilder>,
    pub table_meta_timestamps: TableMetaTimestamps,
    /// Indicates whether column_hlls should be serialized into RawBlockHLL
    /// - true: Output as BlockHLLState::Serialized(RawBlockHLL)
    /// - false: Output as BlockHLLState::Deserialized(BlockHLL)
    pub serialize_hll: bool,
}

impl BlockBuilder {
    pub fn build<F>(&self, data_block: DataBlock, f: F) -> Result<BlockSerialization>
    where F: Fn(DataBlock, &ClusterStatsGenerator) -> Result<(Option<ClusterStatistics>, DataBlock)>
    {
        let (cluster_stats, data_block) = f(data_block, &self.cluster_stats_gen)?;
        let (block_location, block_id) = self
            .meta_locations
            .gen_block_location(self.table_meta_timestamps);

        let bloom_index_location = self.meta_locations.block_bloom_index_location(&block_id);
        let bloom_index_state = BloomIndexState::from_data_block(
            self.ctx.clone(),
            &data_block,
            bloom_index_location,
            self.bloom_columns_map.clone(),
            &self.ngram_args,
        )?;
        let mut column_distinct_count = bloom_index_state
            .as_ref()
            .map(|i| i.column_distinct_count.clone())
            .unwrap_or_default();

        let column_hlls = build_column_hlls(&data_block, &self.ndv_columns_map)?;
        if let Some(hlls) = &column_hlls {
            for (key, val) in hlls {
                if let Entry::Vacant(entry) = column_distinct_count.entry(*key) {
                    entry.insert(val.count());
                }
            }
        }

        let mut inverted_index_states = Vec::with_capacity(self.inverted_index_builders.len());
        for inverted_index_builder in &self.inverted_index_builders {
            let inverted_index_state = InvertedIndexState::from_data_block(
                &self.source_schema,
                &data_block,
                &block_location,
                inverted_index_builder,
            )?;
            inverted_index_states.push(inverted_index_state);
        }
        let vector_index_state = if let Some(ref vector_index_builder) = self.vector_index_builder {
            let vector_index_location = self.meta_locations.block_vector_index_location();
            let mut vector_index_builder = vector_index_builder.clone();
            vector_index_builder.add_block(&data_block)?;
            let vector_index_state = vector_index_builder.finalize(&vector_index_location)?;
            Some(vector_index_state)
        } else {
            None
        };

        let virtual_column_state =
            if let Some(ref virtual_column_builder) = self.virtual_column_builder {
                let mut virtual_column_builder = virtual_column_builder.clone();
                virtual_column_builder.add_block(&data_block)?;
                let virtual_column_state =
                    virtual_column_builder.finalize(&self.write_settings, &block_location)?;
                Some(virtual_column_state)
            } else {
                None
            };

        let row_count = data_block.num_rows() as u64;
        let col_stats = gen_columns_statistics(
            &data_block,
            Some(column_distinct_count),
            &self.source_schema,
        )?;

        let mut buffer = Vec::with_capacity(DEFAULT_BLOCK_BUFFER_SIZE);
        let block_size = data_block.estimate_block_size() as u64;
        let col_metas = serialize_block_with_column_stats(
            &self.write_settings,
            &self.source_schema,
            Some(&col_stats),
            data_block,
            &mut buffer,
        )?;
        let file_size = buffer.len() as u64;
        let inverted_index_size = if !inverted_index_states.is_empty() {
            let size = inverted_index_states.iter().map(|v| v.size).sum();
            Some(size)
        } else {
            None
        };
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
            ngram_filter_index_size: bloom_index_state
                .as_ref()
                .map(|v| v.ngram_size)
                .unwrap_or_default(),
            vector_index_size: vector_index_state.as_ref().map(|v| v.size),
            vector_index_location: vector_index_state.as_ref().map(|v| v.location.clone()),
            compression: self.write_settings.table_compression.into(),
            inverted_index_size,
            virtual_block_meta: None,
            create_on: Some(Utc::now()),
        };

        let column_hlls = column_hlls
            .map(|hlls| {
                if self.serialize_hll {
                    encode_column_hll(&hlls).map(BlockHLLState::Serialized)
                } else {
                    Ok(BlockHLLState::Deserialized(hlls))
                }
            })
            .transpose()?;
        let serialized = BlockSerialization {
            block_raw_data: buffer,
            block_meta,
            bloom_index_state,
            inverted_index_states,
            virtual_column_state,
            vector_index_state,
            column_hlls,
        };
        Ok(serialized)
    }
}

pub struct BlockWriter;

impl BlockWriter {
    pub async fn write_down(
        dal: &Operator,
        serialized: BlockSerialization,
    ) -> Result<ExtendedBlockMeta> {
        let block_meta = serialized.block_meta;
        let column_hlls = serialized.column_hlls;
        let block_location = block_meta.location.0.clone();

        let extended_block_meta =
            if let Some(virtual_column_state) = &serialized.virtual_column_state {
                ExtendedBlockMeta {
                    block_meta,
                    draft_virtual_block_meta: Some(
                        virtual_column_state.draft_virtual_block_meta.clone(),
                    ),
                    column_hlls,
                }
            } else {
                ExtendedBlockMeta {
                    block_meta,
                    draft_virtual_block_meta: None,
                    column_hlls,
                }
            };

        Self::write_down_data_block(dal, serialized.block_raw_data, &block_location).await?;
        Self::write_down_bloom_index_state(dal, serialized.bloom_index_state).await?;
        Self::write_down_vector_index_state(dal, serialized.vector_index_state).await?;
        Self::write_down_inverted_index_state(dal, serialized.inverted_index_states).await?;
        Self::write_down_virtual_column_state(dal, serialized.virtual_column_state).await?;

        Ok(extended_block_meta)
    }

    pub async fn write_down_data_block(
        dal: &Operator,
        raw_block_data: Vec<u8>,
        block_location: &str,
    ) -> Result<()> {
        let start = Instant::now();
        let size = raw_block_data.len();

        write_data(raw_block_data, dal, block_location).await?;

        metrics_inc_block_write_nums(1);
        metrics_inc_block_write_nums(size as u64);
        metrics_inc_block_write_milliseconds(start.elapsed().as_millis() as u64);

        Ok(())
    }

    pub async fn write_down_bloom_index_state(
        dal: &Operator,
        bloom_index_state: Option<BloomIndexState>,
    ) -> Result<()> {
        if let Some(index_state) = bloom_index_state {
            let start = Instant::now();

            let location = &index_state.location.0;
            write_data(index_state.data, dal, location).await?;

            metrics_inc_block_index_write_nums(1);
            metrics_inc_block_index_write_nums(index_state.size);
            metrics_inc_block_index_write_milliseconds(start.elapsed().as_millis() as u64);
        }
        Ok(())
    }

    pub async fn write_down_vector_index_state(
        dal: &Operator,
        vector_index_state: Option<VectorIndexState>,
    ) -> Result<()> {
        if let Some(vector_index_state) = vector_index_state {
            let start = Instant::now();

            let location = &vector_index_state.location.0;
            let index_size = vector_index_state.size;
            write_data(vector_index_state.data, dal, location).await?;

            metrics_inc_block_vector_index_write_nums(1);
            metrics_inc_block_vector_index_write_bytes(index_size);
            metrics_inc_block_vector_index_write_milliseconds(start.elapsed().as_millis() as u64);
        }
        Ok(())
    }

    pub async fn write_down_inverted_index_state(
        dal: &Operator,
        inverted_index_states: Vec<InvertedIndexState>,
    ) -> Result<()> {
        for inverted_index_state in inverted_index_states {
            let start = Instant::now();

            let location = &inverted_index_state.location.0;
            let index_size = inverted_index_state.size;
            write_data(inverted_index_state.data, dal, location).await?;
            metrics_inc_block_inverted_index_write_nums(1);
            metrics_inc_block_inverted_index_write_bytes(index_size);
            metrics_inc_block_inverted_index_write_milliseconds(start.elapsed().as_millis() as u64);
        }
        Ok(())
    }

    pub async fn write_down_virtual_column_state(
        dal: &Operator,
        virtual_column_state: Option<VirtualColumnState>,
    ) -> Result<()> {
        if let Some(virtual_column_state) = virtual_column_state {
            if virtual_column_state
                .draft_virtual_block_meta
                .virtual_column_size
                == 0
            {
                return Ok(());
            }
            let start = Instant::now();

            let index_size = virtual_column_state
                .draft_virtual_block_meta
                .virtual_column_size;
            let location = &virtual_column_state
                .draft_virtual_block_meta
                .virtual_location
                .0;
            write_data(virtual_column_state.data, dal, location).await?;
            metrics_inc_block_virtual_column_write_nums(1);
            metrics_inc_block_virtual_column_write_bytes(index_size);
            metrics_inc_block_virtual_column_write_milliseconds(start.elapsed().as_millis() as u64);
        }
        Ok(())
    }
}
