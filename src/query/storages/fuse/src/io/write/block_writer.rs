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
use std::sync::Arc;
use std::time::Instant;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::FieldIndex;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::local_block_meta_serde;
use databend_common_metrics::storage::metrics_inc_block_index_write_bytes;
use databend_common_metrics::storage::metrics_inc_block_index_write_milliseconds;
use databend_common_metrics::storage::metrics_inc_block_index_write_nums;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_write_bytes;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_write_milliseconds;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_write_nums;
use databend_common_metrics::storage::metrics_inc_block_spatial_index_write_bytes;
use databend_common_metrics::storage::metrics_inc_block_spatial_index_write_milliseconds;
use databend_common_metrics::storage::metrics_inc_block_spatial_index_write_nums;
use databend_common_metrics::storage::metrics_inc_block_vector_index_write_bytes;
use databend_common_metrics::storage::metrics_inc_block_vector_index_write_milliseconds;
use databend_common_metrics::storage::metrics_inc_block_vector_index_write_nums;
use databend_common_metrics::storage::metrics_inc_block_virtual_column_write_bytes;
use databend_common_metrics::storage::metrics_inc_block_virtual_column_write_milliseconds;
use databend_common_metrics::storage::metrics_inc_block_virtual_column_write_nums;
use databend_common_metrics::storage::metrics_inc_block_write_bytes;
use databend_common_metrics::storage::metrics_inc_block_write_milliseconds;
use databend_common_metrics::storage::metrics_inc_block_write_nums;
use databend_storages_common_blocks::SerializedParquet;
use databend_storages_common_blocks::build_parquet_writer_properties;
use databend_storages_common_index::NgramArgs;
use databend_storages_common_table_meta::meta::BlockHLLState;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::BlockTopN;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::ExtendedBlockMeta;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use opendal::Buffer;
use opendal::Operator;

use super::FuseBlockWriteOptions;
use super::FuseBlockWriter;
use super::parquet_block_writer::ParquetBlockWriter;
use crate::FuseStorageFormat;
use crate::io::TableMetaLocationGenerator;
use crate::io::granule_index::GranuleIndexSpec;
use crate::io::granule_index::materialize_cluster_key_columns;
use crate::io::write::GranuleIndexState;
use crate::io::write::InvertedIndexBuilder;
use crate::io::write::SpatialIndexBuilder;
use crate::io::write::SpatialIndexState;
use crate::io::write::VectorIndexBuilder;
use crate::io::write::VectorIndexState;
use crate::io::write::WriteSettings;
use crate::io::write::virtual_column_builder::VirtualColumnBuilder;
use crate::io::write::virtual_column_builder::VirtualColumnState;
use crate::operations::column_parquet_metas;
use crate::statistics::ClusterStatsGenerator;

pub fn serialize_block(
    write_settings: &WriteSettings,
    schema: &TableSchemaRef,
    block: DataBlock,
) -> Result<(HashMap<ColumnId, ColumnMeta>, Buffer)> {
    serialize_block_with_column_stats(write_settings, schema, None, block)
}

pub fn serialize_block_with_column_stats(
    write_settings: &WriteSettings,
    schema: &TableSchemaRef,
    column_stats: Option<&StatisticsOfColumns>,
    block: DataBlock,
) -> Result<(HashMap<ColumnId, ColumnMeta>, Buffer)> {
    let schema = Arc::new(schema.remove_virtual_computed_fields());
    match write_settings.storage_format {
        FuseStorageFormat::Parquet => {
            // Plain write: `granule_rows = None`, no page-boundary forcing, no granule index.
            let props = Arc::new(build_parquet_writer_properties(
                write_settings.table_compression,
                write_settings.enable_parquet_dictionary,
                column_stats,
                None,
                block.num_rows(),
                &schema,
                write_settings.data_page_rows,
                write_settings.data_page_bytes,
            ));
            let mut writer = ParquetBlockWriter::new(props, schema.clone(), None);
            writer.write(block)?;
            let SerializedParquet {
                payload, metadata, ..
            } = writer.finish_plain()?;
            let meta = column_parquet_metas(&metadata, &schema)?;
            Ok((meta, Buffer::from(payload)))
        }
        FuseStorageFormat::Unsupported => Err(crate::unsupported_storage_format_error()),
    }
}

/// Take ownership here to avoid extra copy. Accepts anything convertible to an opendal
/// `Buffer`, including a multi-chunk `Buffer` that is written out without consolidation.
#[async_backtrace::framed]
pub async fn write_data(
    data: impl Into<Buffer>,
    data_accessor: &Operator,
    location: &str,
) -> Result<()> {
    data_accessor.write(location, data).await?;

    Ok(())
}

#[derive(Debug)]
#[doc(hidden)]
pub struct PendingBlockSerialization {
    pub(crate) block_raw_data: Buffer,
    pub(crate) block_meta: BlockMeta,
    pub(crate) block_indexes: crate::io::write::block_index::PendingBlockIndexOutput,
    pub(crate) virtual_column_state: Option<VirtualColumnState>,
    pub(crate) granule_index_state: Option<GranuleIndexState>,
    pub(crate) granule_index_payloads: Vec<crate::io::granule_index::PendingGranuleIndexPayload>,
    pub(crate) column_hlls: Option<BlockHLLState>,
    pub(crate) column_top_n: Option<BlockTopN>,
}

impl PendingBlockSerialization {
    async fn write_down(self, dal: &Operator) -> Result<ExtendedBlockMeta> {
        let block_location = self.block_meta.location.0.clone();
        BlockWriter::write_down_data_block(dal, self.block_raw_data, &block_location).await?;
        BlockWriter::write_down_indexes(dal, self.block_indexes).await?;
        BlockWriter::write_down_granule_index_state(dal, self.granule_index_state).await?;
        for payload in self.granule_index_payloads {
            write_data(payload.data, dal, &payload.location.0).await?;
        }
        let draft_virtual_block_meta = if let Some(state) = self.virtual_column_state {
            let meta = state.draft_virtual_block_meta.clone();
            BlockWriter::write_down_virtual_column_state(dal, Some(state)).await?;
            Some(meta)
        } else {
            None
        };

        Ok(ExtendedBlockMeta {
            block_meta: self.block_meta,
            draft_virtual_block_meta,
            column_hlls: self.column_hlls,
            column_top_n: self.column_top_n,
        })
    }
}

#[derive(Debug)]
pub enum BlockSerialization {
    #[doc(hidden)]
    Pending(PendingBlockSerialization),
    Written(ExtendedBlockMeta),
}

local_block_meta_serde!(BlockSerialization);

#[typetag::serde(name = "block_serialization_meta")]
impl BlockMetaInfo for BlockSerialization {}

#[derive(Clone)]
pub struct BlockBuilder {
    pub ctx: Arc<dyn TableContext>,
    pub operator: Operator,
    pub meta_locations: TableMetaLocationGenerator,
    pub source_schema: TableSchemaRef,
    pub write_settings: WriteSettings,
    pub cluster_stats_gen: ClusterStatsGenerator,
    pub bloom_columns_map: BTreeMap<FieldIndex, TableField>,
    pub ndv_columns_map: BTreeMap<FieldIndex, TableField>,
    pub top_n: Option<(BTreeMap<FieldIndex, TableField>, usize)>,
    pub ngram_args: Vec<NgramArgs>,
    /// One spec per declared granule-level index; empty makes the granule-level write path a no-op.
    pub granule_index_specs: Vec<Arc<dyn GranuleIndexSpec>>,
    pub inverted_index_builders: Vec<InvertedIndexBuilder>,
    pub virtual_column_builder: Option<VirtualColumnBuilder>,
    pub vector_index_builder: Option<VectorIndexBuilder>,
    pub spatial_index_builder: Option<SpatialIndexBuilder>,
    pub table_meta_timestamps: TableMetaTimestamps,
    /// Indicates whether column_hlls should be serialized into RawBlockHLL.
    pub serialize_hll: bool,
}

impl BlockBuilder {
    pub fn build<F>(&self, data_block: DataBlock, f: F) -> Result<BlockSerialization>
    where F: Fn(
            DataBlock,
            &ClusterStatsGenerator,
        ) -> Result<(Option<ClusterStatistics>, DataBlock, Option<Vec<usize>>)> {
        let (cluster_stats, data_block, granule_cluster_key_offsets) =
            f(data_block, &self.cluster_stats_gen)?;
        let granule_cluster_columns = if self.write_settings.index_granularity.is_some() {
            materialize_cluster_key_columns(
                &data_block,
                &self.cluster_stats_gen,
                granule_cluster_key_offsets,
            )?
        } else {
            None
        };
        let options = FuseBlockWriteOptions::from_block_builder_parts(
            self.ctx.clone(),
            self.meta_locations.clone(),
            self.source_schema.clone(),
            self.write_settings.clone(),
            self.cluster_stats_gen.block_thresholds(),
            self.bloom_columns_map.clone(),
            self.ndv_columns_map.clone(),
            self.top_n.clone(),
            self.ngram_args.clone(),
            self.inverted_index_builders.clone(),
            self.virtual_column_builder.clone(),
            self.vector_index_builder.clone(),
            self.spatial_index_builder.clone(),
            self.granule_index_specs.clone(),
            granule_cluster_columns,
            self.table_meta_timestamps,
            self.serialize_hll,
            cluster_stats,
        )?;
        let mut writer = FuseBlockWriter::create(options)?;
        writer.write(data_block)?;
        writer.finish()
    }
}

pub struct BlockWriter;

impl BlockWriter {
    pub async fn write_down(
        dal: &Operator,
        pending: PendingBlockSerialization,
    ) -> Result<ExtendedBlockMeta> {
        pending.write_down(dal).await
    }

    pub async fn write_down_data_block(
        dal: &Operator,
        raw_block_data: Buffer,
        block_location: &str,
    ) -> Result<()> {
        let start = Instant::now();
        let size = raw_block_data.len();

        write_data(raw_block_data, dal, block_location).await?;

        metrics_inc_block_write_nums(1);
        metrics_inc_block_write_bytes(size as u64);
        metrics_inc_block_write_milliseconds(start.elapsed().as_millis() as u64);

        Ok(())
    }

    pub async fn write_down_indexes(
        dal: &Operator,
        output: crate::io::write::block_index::PendingBlockIndexOutput,
    ) -> Result<()> {
        if let Some(index) = output.bloom {
            let start = Instant::now();
            let size = index.file.write(dal).await?;
            metrics_inc_block_index_write_nums(1);
            metrics_inc_block_index_write_bytes(size);
            metrics_inc_block_index_write_milliseconds(start.elapsed().as_millis() as u64);
        }
        for index in output.inverted {
            let start = Instant::now();
            let size = index.file.write(dal).await?;
            metrics_inc_block_inverted_index_write_nums(1);
            metrics_inc_block_inverted_index_write_bytes(size);
            metrics_inc_block_inverted_index_write_milliseconds(start.elapsed().as_millis() as u64);
        }
        if let Some(index) = output.vector
            && let Some(file) = index.file
        {
            let start = Instant::now();
            let size = file.write(dal).await?;
            metrics_inc_block_vector_index_write_nums(1);
            metrics_inc_block_vector_index_write_bytes(size);
            metrics_inc_block_vector_index_write_milliseconds(start.elapsed().as_millis() as u64);
        }
        if let Some(index) = output.spatial
            && let Some(file) = index.file
        {
            let start = Instant::now();
            let size = file.write(dal).await?;
            metrics_inc_block_spatial_index_write_nums(1);
            metrics_inc_block_spatial_index_write_bytes(size);
            metrics_inc_block_spatial_index_write_milliseconds(start.elapsed().as_millis() as u64);
        }
        Ok(())
    }

    pub async fn write_down_bloom_index_state(
        dal: &Operator,
        state: Option<crate::io::BloomIndexState>,
    ) -> Result<()> {
        let Some(state) = state else {
            return Ok(());
        };
        let start = Instant::now();
        let size = state.data.len() as u64;
        write_data(state.data, dal, &state.location.0).await?;
        metrics_inc_block_index_write_nums(1);
        metrics_inc_block_index_write_bytes(size);
        metrics_inc_block_index_write_milliseconds(start.elapsed().as_millis() as u64);
        Ok(())
    }

    pub async fn write_down_vector_index_state(
        dal: &Operator,
        state: Option<VectorIndexState>,
    ) -> Result<()> {
        let Some(state) = state else {
            return Ok(());
        };
        let start = Instant::now();
        let size = state.data.len() as u64;
        write_data(state.data, dal, &state.location.0).await?;
        metrics_inc_block_vector_index_write_nums(1);
        metrics_inc_block_vector_index_write_bytes(size);
        metrics_inc_block_vector_index_write_milliseconds(start.elapsed().as_millis() as u64);
        Ok(())
    }

    pub async fn write_down_spatial_index_state(
        dal: &Operator,
        state: Option<SpatialIndexState>,
    ) -> Result<()> {
        let Some(state) = state else {
            return Ok(());
        };
        let start = Instant::now();
        let size = state.data.len() as u64;
        write_data(state.data, dal, &state.location.0).await?;
        metrics_inc_block_spatial_index_write_nums(1);
        metrics_inc_block_spatial_index_write_bytes(size);
        metrics_inc_block_spatial_index_write_milliseconds(start.elapsed().as_millis() as u64);
        Ok(())
    }

    pub async fn write_down_granule_index_state(
        dal: &Operator,
        granule_index_state: Option<GranuleIndexState>,
    ) -> Result<()> {
        let Some(state) = granule_index_state else {
            return Ok(());
        };
        if let Some(mins) = state.mins {
            write_data(mins.data, dal, &mins.layout.location.0).await?;
        }
        write_data(state.offsets.data, dal, &state.offsets.layout.location.0).await
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
