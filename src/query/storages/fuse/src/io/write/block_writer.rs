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
use std::collections::HashSet;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::time::Instant;

use chrono::Utc;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::FieldIndex;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::local_block_meta_serde;
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
use databend_common_metrics::storage::metrics_inc_block_write_milliseconds;
use databend_common_metrics::storage::metrics_inc_block_write_nums;
use databend_storages_common_blocks::SerializedParquet;
use databend_storages_common_blocks::blocks_to_parquet_with_stats;
use databend_storages_common_index::NgramArgs;
use databend_storages_common_table_meta::meta::BlockHLL;
use databend_storages_common_table_meta::meta::BlockHLLState;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::BlockTopN;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::ColumnGroupBloomMeta;
use databend_storages_common_table_meta::meta::ColumnGroupFileMeta;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::ExtendedBlockMeta;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::encode_column_hll;
use opendal::Buffer;
use opendal::Operator;

use crate::FuseStorageFormat;
use crate::io::BlockStatsBuilder;
use crate::io::BloomIndexState;
use crate::io::TableMetaLocationGenerator;
use crate::io::build_column_hlls;
use crate::io::write::InvertedIndexBuilder;
use crate::io::write::InvertedIndexState;
use crate::io::write::SpatialIndexBuilder;
use crate::io::write::SpatialIndexState;
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
            let SerializedParquet { payload, metadata } = blocks_to_parquet_with_stats(
                &schema,
                vec![block],
                write_settings.table_compression,
                write_settings.enable_parquet_dictionary,
                None,
                column_stats,
                write_settings.data_page_rows,
                write_settings.data_page_bytes,
            )?;
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
pub struct BlockSerialization {
    pub block_raw_data: Buffer,
    pub block_meta: BlockMeta,
    pub bloom_index_state: Option<BloomIndexState>,
    pub inverted_index_states: Vec<InvertedIndexState>,
    pub virtual_column_state: Option<VirtualColumnState>,
    pub vector_index_state: Option<VectorIndexState>,
    pub spatial_index_state: Option<SpatialIndexState>,
    pub column_hlls: Option<BlockHLLState>,
    pub column_top_n: Option<BlockTopN>,
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
    pub top_n: Option<(BTreeMap<FieldIndex, TableField>, usize)>,
    pub ngram_args: Vec<NgramArgs>,
    pub inverted_index_builders: Vec<InvertedIndexBuilder>,
    pub virtual_column_builder: Option<VirtualColumnBuilder>,
    pub vector_index_builder: Option<VectorIndexBuilder>,
    pub spatial_index_builder: Option<SpatialIndexBuilder>,
    pub table_meta_timestamps: TableMetaTimestamps,
    /// Indicates whether column_hlls should be serialized into RawBlockHLL
    /// - true: Output as BlockHLLState::Serialized(RawBlockHLL)
    /// - false: Output as BlockHLLState::Deserialized(BlockHLL)
    pub serialize_hll: bool,
}

struct ColumnGroupUpdate {
    active_column_ids: Vec<ColumnId>,
    location: Location,
    file_size: u64,
    uncompressed_size: u64,
    column_metas: HashMap<ColumnId, ColumnMeta>,
    column_stats: StatisticsOfColumns,
    bloom: Option<ColumnGroupBloomMeta>,
}

fn merge_column_group_metadata(
    origin: &BlockMeta,
    current_column_ids: &HashSet<ColumnId>,
    update: ColumnGroupUpdate,
) -> BlockMeta {
    let updated_column_ids = update
        .active_column_ids
        .iter()
        .copied()
        .collect::<HashSet<_>>();
    let mut column_groups = origin.physical_column_groups().into_owned();
    if origin.column_groups.is_empty() {
        let legacy_bloom_is_ordinary_and_paired = origin.ngram_filter_index_size.is_none()
            && origin
                .bloom_filter_index_location
                .as_ref()
                .is_none_or(|location| {
                    let expected =
                        TableMetaLocationGenerator::gen_bloom_index_location_with_version(
                            &origin.location.0,
                            location.1,
                        );
                    expected == location.0
                });
        // A legacy Bloom may have an unpaired path after Ngram refresh, or a paired path but
        // contain Ngram filters when the index existed at INSERT time. Neither file can be
        // represented by ColumnGroupBloomMeta after the index is dropped, so adopt only an
        // ordinary paired file and otherwise fail open.
        if legacy_bloom_is_ordinary_and_paired {
            column_groups[0].bloom =
                origin
                    .bloom_filter_index_location
                    .as_ref()
                    .map(|location| ColumnGroupBloomMeta {
                        format_version: location.1,
                        file_size: origin.bloom_filter_index_size,
                    });
        }
    }
    for group in &mut column_groups {
        group.active_column_ids.retain(|column_id| {
            current_column_ids.contains(column_id) && !updated_column_ids.contains(column_id)
        });
    }
    column_groups.retain(|group| !group.active_column_ids.is_empty());
    column_groups.push(ColumnGroupFileMeta {
        active_column_ids: update.active_column_ids,
        location: update.location.clone(),
        format_version: update.location.1,
        file_size: update.file_size,
        uncompressed_size: update.uncompressed_size,
        leaf_column_metas: update.column_metas.clone(),
        bloom: update.bloom,
    });

    let mut block_meta = origin.clone();
    block_meta.location = update.location;
    block_meta.file_size = column_groups.iter().map(|group| group.file_size).sum();
    block_meta.block_size = column_groups
        .iter()
        .map(|group| group.uncompressed_size)
        .sum();
    block_meta.column_groups = column_groups;
    block_meta
        .col_metas
        .retain(|column_id, _| current_column_ids.contains(column_id));
    block_meta.col_metas.extend(update.column_metas);
    block_meta
        .col_stats
        .retain(|column_id, _| current_column_ids.contains(column_id));
    block_meta.col_stats.extend(update.column_stats);
    block_meta.bloom_filter_index_location = None;
    block_meta.bloom_filter_index_size = block_meta
        .column_groups
        .iter()
        .filter_map(|group| group.bloom.as_ref())
        .map(|bloom| bloom.file_size)
        .sum();
    block_meta.ngram_filter_index_size = None;
    block_meta
}

impl BlockBuilder {
    fn add_hll_distinct_counts(
        column_distinct_count: &mut HashMap<ColumnId, usize>,
        column_hlls: &Option<BlockHLL>,
    ) {
        if let Some(hlls) = column_hlls {
            for (column_id, hll) in hlls {
                column_distinct_count
                    .entry(*column_id)
                    .or_insert_with(|| hll.count());
            }
        }
    }

    fn finalize_column_hlls(&self, column_hlls: Option<BlockHLL>) -> Result<Option<BlockHLLState>> {
        column_hlls
            .map(|hlls| {
                if self.serialize_hll {
                    encode_column_hll(&hlls).map(BlockHLLState::Serialized)
                } else {
                    Ok(BlockHLLState::Deserialized(hlls))
                }
            })
            .transpose()
    }

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
            self.write_settings.bloom_index_type,
            self.bloom_columns_map.clone(),
            &self.ngram_args,
        )?;
        let mut column_distinct_count = bloom_index_state
            .as_ref()
            .map(|i| i.column_distinct_count.clone())
            .unwrap_or_default();

        let top_n = self
            .top_n
            .as_ref()
            .map(|(top_n_columns_map, top_n_size)| (top_n_columns_map, *top_n_size));
        let mut block_stats_builder = BlockStatsBuilder::new(&self.ndv_columns_map, top_n, None)?;
        block_stats_builder.add_block(&data_block)?;
        let block_stats = block_stats_builder.finalize_with_top_n()?;
        let (column_hlls, column_top_n) = if let Some(stats) = block_stats {
            (
                (!stats.hll.is_empty()).then_some(stats.hll),
                (!stats.top_n.is_empty()).then_some(stats.top_n),
            )
        } else {
            (None, None)
        };
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
        let (vector_index_state, vector_stats) = if let Some(ref vector_index_builder) =
            self.vector_index_builder
        {
            let vector_index_location = self.meta_locations.block_vector_index_location();
            let mut vector_index_builder = vector_index_builder.clone();
            vector_index_builder.add_block(&data_block)?;
            let vector_index_state = vector_index_builder.finalize_block(&vector_index_location)?;
            (
                vector_index_state.index_state,
                vector_index_state.vector_stats,
            )
        } else {
            (None, None)
        };

        let (spatial_index_state, spatial_stats) =
            if let Some(ref spatial_index_builder) = self.spatial_index_builder {
                let spatial_index_location = self.meta_locations.block_spatial_index_location();
                let mut spatial_index_builder = spatial_index_builder.clone();
                spatial_index_builder.add_block(&data_block)?;
                let spatial_result = spatial_index_builder.finalize(&spatial_index_location)?;
                (spatial_result.index_state, spatial_result.spatial_stats)
            } else {
                (None, None)
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
            &self.write_settings.col_stats_truncate_lens,
        )?;

        let block_size = data_block.estimate_block_size(data_block.num_columns()) as u64;
        let (col_metas, buffer) = serialize_block_with_column_stats(
            &self.write_settings,
            &self.source_schema,
            Some(&col_stats),
            data_block,
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
            column_groups: vec![],
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
            spatial_index_size: spatial_index_state.as_ref().map(|v| v.size),
            spatial_index_location: spatial_index_state.as_ref().map(|v| v.location.clone()),
            spatial_stats,
            vector_stats,
            compression: self.write_settings.table_compression.into(),
            inverted_index_size,
            virtual_block_meta: None,
            create_on: Some(Utc::now()),
        };

        let column_hlls = self.finalize_column_hlls(column_hlls)?;
        let serialized = BlockSerialization {
            block_raw_data: buffer,
            block_meta,
            bloom_index_state,
            inverted_index_states,
            virtual_column_state,
            vector_index_state,
            spatial_index_state,
            column_hlls,
            column_top_n,
        };
        Ok(serialized)
    }

    /// Serialize only the fields changed by an UPDATE and merge their physical
    /// metadata back into the original logical block.
    pub fn build_column_group(
        &self,
        data_block: DataBlock,
        origin: &BlockMeta,
        updated_field_indices: &[FieldIndex],
    ) -> Result<BlockSerialization> {
        if data_block.num_rows() as u64 != origin.row_count {
            return Err(ErrorCode::Internal(
                "column-group update changed the block row count",
            ));
        }

        let mut updated_field_indices = updated_field_indices.to_vec();
        updated_field_indices.sort_unstable();
        updated_field_indices.dedup();
        if updated_field_indices.is_empty() {
            return Err(ErrorCode::Internal(
                "column-group update has no updated fields",
            ));
        }
        if updated_field_indices
            .iter()
            .any(|index| *index >= self.source_schema.fields().len())
        {
            return Err(ErrorCode::Internal(
                "column-group update field is outside the table schema",
            ));
        }

        let updated_schema = Arc::new(self.source_schema.project(&updated_field_indices));
        if data_block.num_columns() != updated_schema.fields().len() {
            return Err(ErrorCode::Internal(
                "column-group update block does not match updated fields",
            ));
        }
        let updated_block = data_block;
        let updated_column_ids = updated_schema.to_leaf_column_ids();

        let (data_location, block_id) = self
            .meta_locations
            .gen_block_location(self.table_meta_timestamps);

        let updated_bloom_columns_map = updated_field_indices
            .iter()
            .enumerate()
            .filter_map(|(updated_index, source_index)| {
                self.bloom_columns_map
                    .get(source_index)
                    .cloned()
                    .map(|field| (updated_index, field))
            })
            .collect::<BTreeMap<_, _>>();
        let rebuild_bloom_index = !updated_bloom_columns_map.is_empty();
        let bloom_index_state = if rebuild_bloom_index {
            let location = self.meta_locations.block_bloom_index_location(&block_id);
            BloomIndexState::from_data_block(
                self.ctx.clone(),
                &updated_block,
                location,
                self.write_settings.bloom_index_type,
                updated_bloom_columns_map,
                &[],
            )?
        } else {
            None
        };

        let mut column_distinct_count = bloom_index_state
            .as_ref()
            .map(|index| index.column_distinct_count.clone())
            .unwrap_or_default();
        let updated_ndv_columns_map = updated_field_indices
            .iter()
            .enumerate()
            .filter_map(|(updated_index, source_index)| {
                self.ndv_columns_map
                    .get(source_index)
                    .cloned()
                    .map(|field| (updated_index, field))
            })
            .collect::<BTreeMap<_, _>>();
        let column_hlls = build_column_hlls(&updated_block, &updated_ndv_columns_map)?;
        Self::add_hll_distinct_counts(&mut column_distinct_count, &column_hlls);

        let invalidate_virtual_columns = updated_field_indices.iter().any(|index| {
            self.source_schema
                .field(*index)
                .data_type()
                .remove_nullable()
                == TableDataType::Variant
        });
        let virtual_column_state = None;

        let updated_col_stats = gen_columns_statistics(
            &updated_block,
            Some(column_distinct_count),
            &updated_schema,
            &self.write_settings.col_stats_truncate_lens,
        )?;
        let uncompressed_size =
            updated_block.estimate_block_size(updated_block.num_columns()) as u64;
        let mut write_settings = self.write_settings.clone();
        write_settings.table_compression = origin.compression.into();
        let (updated_col_metas, buffer) = serialize_block_with_column_stats(
            &write_settings,
            &updated_schema,
            Some(&updated_col_stats),
            updated_block,
        )?;
        let file_size = buffer.len() as u64;

        let current_column_ids = self.source_schema.to_leaf_column_id_set();
        let mut block_meta =
            merge_column_group_metadata(origin, &current_column_ids, ColumnGroupUpdate {
                active_column_ids: updated_column_ids,
                location: data_location,
                file_size,
                uncompressed_size,
                column_metas: updated_col_metas,
                column_stats: updated_col_stats,
                bloom: bloom_index_state
                    .as_ref()
                    .map(|state| ColumnGroupBloomMeta {
                        format_version: state.location.1,
                        file_size: state.size,
                    }),
            });
        block_meta.create_on = Some(Utc::now());
        if invalidate_virtual_columns {
            block_meta.virtual_block_meta = None;
        }

        let column_hlls = self.finalize_column_hlls(column_hlls)?;

        Ok(BlockSerialization {
            block_raw_data: buffer,
            block_meta,
            bloom_index_state,
            inverted_index_states: vec![],
            virtual_column_state,
            vector_index_state: None,
            spatial_index_state: None,
            column_hlls,
            column_top_n: None,
        })
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
        let column_top_n = serialized.column_top_n;
        let block_location = block_meta.location.0.clone();

        let extended_block_meta =
            if let Some(virtual_column_state) = &serialized.virtual_column_state {
                ExtendedBlockMeta {
                    block_meta,
                    draft_virtual_block_meta: Some(
                        virtual_column_state.draft_virtual_block_meta.clone(),
                    ),
                    column_hlls,
                    column_top_n,
                }
            } else {
                ExtendedBlockMeta {
                    block_meta,
                    draft_virtual_block_meta: None,
                    column_hlls,
                    column_top_n,
                }
            };

        Self::write_down_data_block(dal, serialized.block_raw_data, &block_location).await?;
        Self::write_down_bloom_index_state(dal, serialized.bloom_index_state).await?;
        Self::write_down_vector_index_state(dal, serialized.vector_index_state).await?;
        Self::write_down_spatial_index_state(dal, serialized.spatial_index_state).await?;
        Self::write_down_inverted_index_state(dal, serialized.inverted_index_states).await?;
        Self::write_down_virtual_column_state(dal, serialized.virtual_column_state).await?;

        Ok(extended_block_meta)
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

    pub async fn write_down_spatial_index_state(
        dal: &Operator,
        spatial_index_state: Option<SpatialIndexState>,
    ) -> Result<()> {
        if let Some(spatial_index_state) = spatial_index_state {
            let start = Instant::now();

            let location = &spatial_index_state.location.0;
            let index_size = spatial_index_state.size;
            write_data(spatial_index_state.data, dal, location).await?;

            metrics_inc_block_spatial_index_write_nums(1);
            metrics_inc_block_spatial_index_write_bytes(index_size);
            metrics_inc_block_spatial_index_write_milliseconds(start.elapsed().as_millis() as u64);
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
