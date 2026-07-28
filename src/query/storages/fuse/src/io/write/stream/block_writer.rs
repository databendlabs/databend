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

use chrono::Utc;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::ComputedExpr;
use databend_common_expression::DataBlock;
use databend_common_expression::FieldIndex;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::types::DataType;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_storages_common_blocks::NdvProvider;
use databend_storages_common_blocks::build_parquet_writer_properties;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_index::Index;
use databend_storages_common_index::NgramArgs;
use databend_storages_common_index::RangeIndex;
use databend_storages_common_table_meta::meta::BlockHLLState;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::encode_column_hll;
use opendal::Buffer;
use opendal::Operator;

use super::super::parquet_block_writer::GranuleWriteSettings;
use super::super::parquet_block_writer::ParquetBlockOutput;
use super::super::parquet_block_writer::ParquetBlockWriter;
use crate::FuseStorageFormat;
use crate::FuseTable;
use crate::io::BlockSerialization;
use crate::io::FuseLowLevelBlockWriteOptions;
use crate::io::InvertedIndexBuilder;
use crate::io::PendingBlockSerialization;
use crate::io::SpatialIndexBuilder;
use crate::io::TableMetaLocationGenerator;
use crate::io::VectorIndexBuilder;
use crate::io::VirtualColumnBuilder;
use crate::io::WriteSettings;
use crate::io::create_inverted_index_builders;
use crate::io::granule_index::GranuleIndexSpec;
use crate::io::granule_index::build_granule_index_specs;
use crate::io::write::BlockColumnSketchesBuilder;
use crate::io::write::BloomIndexWriteSpec;
use crate::io::write::GranuleIndexState;
use crate::io::write::block_index::BlockIndexSpec;
use crate::io::write::block_index::BlockIndexWriteContext;
use crate::io::write::block_index::BlockIndexWriter;
use crate::io::write::block_index::PendingBlockIndexOutput;
use crate::io::write::block_index::PendingIndexFile;
use crate::io::write::stream::ColumnStatisticsState;
use crate::io::write::stream::cluster_statistics::ClusterStatisticsBuilder;
use crate::io::write::stream::cluster_statistics::ClusterStatisticsState;

struct ColumnsNdvInfo {
    cols_ndv: HashMap<ColumnId, usize>,
}

impl NdvProvider for ColumnsNdvInfo {
    fn column_ndv(&self, column_id: &ColumnId) -> Option<u64> {
        self.cols_ndv.get(column_id).map(|v| *v as u64)
    }
}

/// Standard FUSE block writer driven by complete `DataBlock` inputs. It retains serialized
/// payloads in memory and returns a pending block serialization for asynchronous upload.
pub struct FuseBlockWriter {
    properties: Arc<FuseBlockWriteOptions>,
    /// The block's location, fixed at construction (not at `finish`): granule-index builders stream
    /// their payload files to storage as granules seal, so payload paths — derived from this — must
    /// exist before the first block is written.
    block_location: Location,
    /// `None` until the first block arrives: props depend on first-block NDV, so creation is
    /// deferred to `write`.
    block_writer: Option<ParquetBlockWriter>,
    block_index_writers: Vec<Box<dyn BlockIndexWriter>>,
    virtual_column_builder: Option<VirtualColumnBuilder>,
    column_sketches_builder: BlockColumnSketchesBuilder,

    cluster_stats_state: ClusterStatisticsState,
    column_stats_state: ColumnStatisticsState,

    row_count: usize,
    block_size: usize,
}

impl FuseBlockWriter {
    pub fn create(properties: Arc<FuseBlockWriteOptions>) -> Result<Self> {
        // Reject unsupported formats up front so the deferred `write` can assume Parquet.
        if matches!(
            properties.write_settings.storage_format,
            FuseStorageFormat::Unsupported
        ) {
            return Err(crate::unsupported_storage_format_error());
        }

        let (block_location, block_id) = properties
            .meta_locations
            .gen_block_location(properties.table_meta_timestamps);

        let func_ctx = properties.ctx.get_function_context()?;
        let index_context = BlockIndexWriteContext {
            func_ctx,
            physical_schema: properties.source_schema.clone(),
            block_location: block_location.clone(),
            write_settings: properties.write_settings.clone(),
        };
        let bloom_location = properties
            .meta_locations
            .block_bloom_index_location(&block_id);
        let mut block_index_writers = vec![
            BloomIndexWriteSpec::new(
                properties.bloom_columns_map.clone(),
                properties.ngram_args.clone(),
                bloom_location,
            )
            .new_writer(index_context.clone())?,
        ];
        for spec in &properties.inverted_index_builders {
            block_index_writers.push(spec.new_writer(index_context.clone())?);
        }
        if let Some(builder) = properties.vector_index_builder.clone() {
            let spec = builder.into_write_spec(
                properties.meta_locations.block_vector_index_location(),
                properties.source_schema.num_fields(),
            );
            block_index_writers.push(spec.new_writer(index_context.clone())?);
        }
        if let Some(builder) = properties.spatial_index_builder.clone() {
            let spec = builder.into_write_spec(
                properties.meta_locations.block_spatial_index_location(),
                properties.source_schema.num_fields(),
            );
            block_index_writers.push(spec.new_writer(index_context)?);
        }

        let virtual_column_builder = properties.virtual_column_builder.clone();
        let top_n = properties
            .top_n
            .as_ref()
            .map(|(columns, size)| (columns, *size));
        let column_sketches_builder =
            BlockColumnSketchesBuilder::new(&properties.ndv_columns_map, top_n, None)?;
        let cluster_stats_state =
            ClusterStatisticsState::new(properties.cluster_stats_builder.clone());
        let column_stats_state = ColumnStatisticsState::new(
            &properties.stats_columns,
            &properties.distinct_columns,
            &properties.write_settings.col_stats_truncate_lens,
        );
        Ok(FuseBlockWriter {
            properties,
            block_location,
            block_writer: None,
            block_index_writers,
            virtual_column_builder,
            column_sketches_builder,
            row_count: 0,
            block_size: 0,
            column_stats_state,
            cluster_stats_state,
        })
    }

    pub fn is_empty(&self) -> bool {
        self.row_count == 0
    }

    pub fn need_flush(&self) -> bool {
        let file_size = match &self.block_writer {
            Some(writer) => writer.compressed_size(),
            None => 0,
        };
        self.row_count >= self.properties.block_thresholds.min_rows_per_block
            || self.block_size >= self.properties.block_thresholds.min_bytes_per_block * 2
            || (file_size >= self.properties.block_thresholds.min_compressed_per_block
                && self.block_size >= self.properties.block_thresholds.min_bytes_per_block)
    }

    pub fn write(&mut self, block: DataBlock) -> Result<()> {
        if block.is_empty() {
            return Ok(());
        }

        let block = self.cluster_stats_state.add_block(block)?;
        self.column_stats_state
            .add_block(&self.properties.source_schema, &block)?;
        for writer in self.block_index_writers.iter_mut() {
            writer.write(&block)?;
        }
        self.column_sketches_builder.add_block(&block)?;
        if let Some(ref mut virtual_column_builder) = self.virtual_column_builder {
            virtual_column_builder.add_block(&block)?;
        }
        self.row_count += block.num_rows();
        self.block_size += block.estimate_block_size(block.num_columns());

        if self.block_writer.is_none() {
            let mut cols_ndv = self.column_stats_state.peek_cols_ndv();
            cols_ndv.extend(self.column_sketches_builder.peek_cols_ndv());
            let ndv_info = ColumnsNdvInfo { cols_ndv };
            self.block_writer = Some(self.new_block_writer(block.num_rows(), ndv_info)?);
        }

        // ParquetBlockWriter tracks the residual granule fill across calls, so just forward the block.
        let Some(writer) = self.block_writer.as_mut() else {
            return Err(ErrorCode::Internal(
                "stream block writer was not initialized",
            ));
        };
        writer.write(block)?;
        Ok(())
    }

    /// Build the parquet writer for the first block, configured from its NDV snapshot.
    fn new_block_writer(
        &self,
        num_rows: usize,
        cols_ndv_info: ColumnsNdvInfo,
    ) -> Result<ParquetBlockWriter> {
        let write_settings = &self.properties.write_settings;
        let data_page_rows = write_settings
            .index_granularity
            .or(write_settings.data_page_rows);
        let enable_parquet_dictionary = write_settings.parquet_dictionary_enabled();
        let props = Arc::new(build_parquet_writer_properties(
            write_settings.table_compression,
            enable_parquet_dictionary,
            Some(cols_ndv_info),
            None,
            num_rows,
            self.properties.source_schema.as_ref(),
            data_page_rows,
            write_settings.data_page_bytes,
        ));
        let granule = if let Some(rows) = write_settings.index_granularity {
            let func_ctx = self.properties.ctx.get_function_context()?;
            let writers = self
                .properties
                .granule_index_specs
                .iter()
                .map(|spec| {
                    spec.new_writer(
                        func_ctx.clone(),
                        &self.properties.source_schema,
                        &self.block_location.0,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let mins = self
                .properties
                .granule_cluster_keys
                .as_ref()
                .map(|columns| {
                    (
                        columns.clone(),
                        TableMetaLocationGenerator::gen_granule_mins_location_from_block_location(
                            &self.block_location.0,
                        ),
                    )
                });
            Some(GranuleWriteSettings::new(
                rows,
                writers,
                mins,
                TableMetaLocationGenerator::gen_granule_offsets_location_from_block_location(
                    &self.block_location.0,
                ),
            ))
        } else {
            None
        };
        Ok(ParquetBlockWriter::new(
            props,
            self.properties.source_schema.clone(),
            granule,
        ))
    }

    pub fn finish(mut self) -> Result<BlockSerialization> {
        let block_location = self.block_location.clone();

        let mut block_indexes = PendingBlockIndexOutput::default();
        for writer in self.block_index_writers {
            block_indexes.merge(writer.finish()?)?;
        }
        let mut column_distinct_count = block_indexes
            .bloom
            .as_ref()
            .map(|index| index.column_distinct_count.clone())
            .unwrap_or_default();
        let column_sketches = self.column_sketches_builder.finalize_sketches()?;
        let (column_hlls, column_top_n) = if let Some(sketches) = column_sketches {
            (
                (!sketches.hll.is_empty()).then_some(sketches.hll),
                (!sketches.top_n.is_empty()).then_some(sketches.top_n),
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
        let col_stats = self.column_stats_state.finalize(column_distinct_count)?;

        let virtual_column_state =
            if let Some(ref mut virtual_column_builder) = self.virtual_column_builder {
                let virtual_column_state = virtual_column_builder
                    .finalize(&self.properties.write_settings, &block_location)?;
                Some(virtual_column_state)
            } else {
                None
            };
        let (vector_index_size, vector_index_location, vector_stats) = match &block_indexes.vector {
            Some(index) => (
                index.file.as_ref().map(PendingIndexFile::size),
                index.file.as_ref().map(|file| file.location.clone()),
                index.statistics.clone(),
            ),
            None => (None, None, None),
        };
        let (spatial_index_size, spatial_index_location, spatial_stats) =
            match &block_indexes.spatial {
                Some(index) => (
                    index.file.as_ref().map(PendingIndexFile::size),
                    index.file.as_ref().map(|file| file.location.clone()),
                    index.statistics.clone(),
                ),
                None => (None, None, None),
            };

        let ParquetBlockOutput {
            data: block_raw_data,
            col_metas,
            granule_index,
            granule_payloads,
        } = match self.block_writer.take() {
            Some(writer) => writer.finish()?,
            // Empty builder: no block was ever written.
            None => ParquetBlockOutput {
                data: Buffer::new(),
                col_metas: HashMap::new(),
                granule_index: None,
                granule_payloads: Vec::new(),
            },
        };
        let file_size = block_raw_data.len();
        let inverted_index_size = block_indexes
            .inverted
            .iter()
            .map(|index| index.file.size())
            .sum::<u64>();
        let inverted_index_size = (inverted_index_size > 0).then_some(inverted_index_size);
        let perfect = self.properties.block_thresholds.check_perfect_block(
            self.row_count,
            self.block_size,
            file_size,
        );
        let cluster_stats = match &self.properties.cluster_stats_override {
            Some(stats) => Some(stats.clone()),
            None => self.cluster_stats_state.finalize(perfect)?,
        };
        let (bloom_filter_index_location, bloom_filter_index_size, ngram_filter_index_size) =
            match &block_indexes.bloom {
                Some(index) => (
                    Some(index.file.location.clone()),
                    index.file.size(),
                    index.ngram_size,
                ),
                None => (None, 0, None),
            };
        let granule_index_layout = granule_index.as_ref().map(GranuleIndexState::layout);
        let block_meta = BlockMeta {
            row_count: self.row_count as u64,
            block_size: self.block_size as u64,
            file_size: file_size as u64,
            col_stats,
            col_metas,
            cluster_stats,
            location: block_location,
            bloom_filter_index_location,
            bloom_filter_index_size,
            compression: self.properties.write_settings.table_compression.into(),
            inverted_index_size,
            vector_index_size,
            vector_index_location,
            spatial_index_size,
            spatial_index_location,
            spatial_stats,
            granule_index: granule_index_layout,
            vector_stats,
            create_on: Some(Utc::now()),
            ngram_filter_index_size,
            virtual_block_meta: None,
        };
        let serialized = BlockSerialization::Pending(PendingBlockSerialization {
            block_raw_data,
            block_meta,
            block_indexes,
            virtual_column_state,
            granule_index_state: granule_index,
            granule_index_payloads: granule_payloads,
            column_hlls: match column_hlls {
                Some(hlls) if self.properties.serialize_hll => {
                    Some(BlockHLLState::Serialized(encode_column_hll(&hlls)?))
                }
                Some(hlls) => Some(BlockHLLState::Deserialized(hlls)),
                None => None,
            },
            column_top_n,
        });
        Ok(serialized)
    }
}

fn sample_granule_cluster_keys(columns: Vec<Column>, granule_rows: usize) -> Result<Vec<Column>> {
    if columns.is_empty() {
        return Err(ErrorCode::Internal(
            "granule cluster keys require at least one column",
        ));
    }
    if granule_rows == 0 {
        return Err(ErrorCode::Internal(
            "granule cluster key rows must be greater than zero",
        ));
    }

    let rows = columns[0].len();
    if columns.iter().any(|column| column.len() != rows) {
        return Err(ErrorCode::Internal(
            "granule cluster key columns have different row counts",
        ));
    }

    let indices = (0..rows)
        .step_by(granule_rows)
        .map(|row| row as u64)
        .collect::<Vec<_>>();
    let sampled = DataBlock::new_from_columns(columns).take(indices.as_slice())?;
    Ok(sampled
        .columns()
        .iter()
        .map(|entry| entry.to_column().wrap_nullable(None))
        .collect())
}

pub struct FuseBlockWriteOptions {
    pub(crate) ctx: Arc<dyn TableContext>,
    pub(crate) write_settings: WriteSettings,
    pub(crate) block_thresholds: BlockThresholds,

    meta_locations: TableMetaLocationGenerator,
    source_schema: TableSchemaRef,

    cluster_stats_builder: Arc<ClusterStatisticsBuilder>,
    stats_columns: Vec<(ColumnId, DataType)>,
    distinct_columns: Vec<(ColumnId, DataType)>,
    bloom_columns_map: BTreeMap<FieldIndex, TableField>,
    ndv_columns_map: BTreeMap<FieldIndex, TableField>,
    top_n: Option<(BTreeMap<FieldIndex, TableField>, usize)>,
    ngram_args: Vec<NgramArgs>,
    inverted_index_builders: Vec<InvertedIndexBuilder>,
    virtual_column_builder: Option<VirtualColumnBuilder>,
    table_meta_timestamps: TableMetaTimestamps,
    vector_index_builder: Option<VectorIndexBuilder>,
    spatial_index_builder: Option<SpatialIndexBuilder>,
    granule_index_specs: Vec<Arc<dyn GranuleIndexSpec>>,
    granule_cluster_keys: Option<Vec<Column>>,
    serialize_hll: bool,
    cluster_stats_override: Option<ClusterStatistics>,
}

impl FuseBlockWriteOptions {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        table: &FuseTable,
        kind: MutationKind,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<Arc<Self>> {
        let schema = table.schema();
        // remove virtual computed fields.
        let mut fields = schema
            .fields()
            .iter()
            .filter(|f| !matches!(f.computed_expr(), Some(ComputedExpr::Virtual(_))))
            .cloned()
            .collect::<Vec<_>>();
        if !matches!(kind, MutationKind::Insert | MutationKind::Replace) {
            // add stream fields.
            for stream_column in table.stream_columns().iter() {
                fields.push(stream_column.table_field());
            }
        }

        let source_schema = Arc::new(TableSchema {
            fields,
            ..schema.as_ref().clone()
        });

        let write_settings = table.get_write_settings();

        let bloom_columns_map = table
            .bloom_index_cols
            .bloom_index_fields(source_schema.clone(), BloomIndex::supported_type)?;
        let ngram_args =
            FuseTable::create_ngram_index_args(&table.table_info.meta.indexes, &schema, true)?;
        let ndv_columns_map = table
            .approx_distinct_cols
            .distinct_column_fields(source_schema.clone(), RangeIndex::supported_table_type)?;
        let top_n = if matches!(kind, MutationKind::Insert) {
            table.append_top_n_columns(source_schema.clone())?
        } else {
            None
        };
        let bloom_ndv_columns = bloom_columns_map
            .values()
            .chain(ndv_columns_map.values())
            .map(|v| v.column_id())
            .collect::<HashSet<_>>();

        let inverted_index_builders = create_inverted_index_builders(&table.table_info.meta);
        let granule_index_specs = build_granule_index_specs(
            &table.table_info.meta.indexes,
            &table.table_info.meta.schema,
            table.bloom_index_type(),
        )?;

        let virtual_column_builder = if table.enable_virtual_column() {
            VirtualColumnBuilder::try_create(source_schema.clone()).ok()
        } else {
            None
        };

        let cluster_stats_builder =
            ClusterStatisticsBuilder::try_create(table, ctx.clone(), &source_schema)?;

        let mut stats_columns = vec![];
        let mut distinct_columns = vec![];
        let leaf_fields = source_schema.leaf_fields();
        for field in leaf_fields.iter() {
            let column_id = field.column_id();
            let data_type = DataType::from(field.data_type());
            if RangeIndex::supported_type(&data_type) {
                stats_columns.push((column_id, data_type.clone()));
                if !bloom_ndv_columns.contains(&column_id) {
                    distinct_columns.push((column_id, data_type));
                }
            }
        }
        let vector_index_builder = VectorIndexBuilder::try_create(
            &table.table_info.meta.indexes,
            source_schema.clone(),
            true,
        );
        let spatial_index_builder = SpatialIndexBuilder::try_create(
            &table.table_info.meta.indexes,
            source_schema.clone(),
            true,
        );
        Ok(Arc::new(FuseBlockWriteOptions {
            ctx,
            meta_locations: table.meta_location_generator().clone(),
            block_thresholds: table.get_block_thresholds(),
            source_schema,
            write_settings,
            cluster_stats_builder,
            virtual_column_builder,
            stats_columns,
            distinct_columns,
            bloom_columns_map,
            top_n,
            ngram_args,
            inverted_index_builders,
            table_meta_timestamps,
            vector_index_builder,
            spatial_index_builder,
            ndv_columns_map,
            granule_index_specs,
            granule_cluster_keys: None,
            serialize_hll: false,
            cluster_stats_override: None,
        }))
    }

    pub fn source_schema(&self) -> &TableSchemaRef {
        &self.source_schema
    }

    /// Conservative retained-state estimate used by vertical admission before IO starts.
    pub fn retained_index_bytes(&self, output_rows: usize) -> usize {
        // The data writer opens the main Parquet output and every block-index
        // output together. Bloom is always configured; inverted, vector, and
        // spatial writers are optional. Granule Bloom indexes retain one output
        // per bound physical column. Every OpenDAL blocking output retains a
        // current 4 MiB chunk, its bounded channel, and one worker-owned chunk.
        let granule_blocking_writers = if self.write_settings.index_granularity.is_some() {
            self.granule_index_specs
                .iter()
                .map(|spec| spec.low_level_blocking_writers(self.source_schema.as_ref()))
                .sum::<usize>()
        } else {
            0
        };
        let active_blocking_writers = 2usize
            .saturating_add(self.inverted_index_builders.len())
            .saturating_add(self.vector_index_builder.is_some() as usize)
            .saturating_add(self.spatial_index_builder.is_some() as usize)
            .saturating_add(granule_blocking_writers);
        let writer_buffers = databend_storages_common_io::blocking_write_retained_bytes(
            databend_storages_common_io::BLOCKING_WRITE_MAX_CHUNKS,
        )
        .saturating_mul(active_blocking_writers);

        // Index builders and granule mark state grow with output rows even when
        // they do not own a concurrent upload buffer.
        let retained_builders = 1usize
            .saturating_add(self.inverted_index_builders.len())
            .saturating_add(self.vector_index_builder.is_some() as usize)
            .saturating_add(self.spatial_index_builder.is_some() as usize)
            .saturating_add(self.granule_index_specs.len())
            .saturating_mul(output_rows.saturating_mul(16).saturating_add(64 * 1024));
        writer_buffers.saturating_add(retained_builders)
    }

    pub fn create_low_level_options(
        &self,
        operator: Operator,
        cluster_key_types: Vec<DataType>,
        cluster_key_id: u32,
        level: i32,
        output_rows: usize,
    ) -> Result<FuseLowLevelBlockWriteOptions> {
        let (block_location, block_id) = self.meta_locations.gen_unique_block_location();
        let dictionary = self.write_settings.parquet_dictionary_enabled();
        let properties = Arc::new(build_parquet_writer_properties(
            self.write_settings.table_compression,
            dictionary,
            None::<ColumnsNdvInfo>,
            None,
            output_rows,
            self.source_schema.as_ref(),
            self.write_settings
                .index_granularity
                .or(self.write_settings.data_page_rows),
            self.write_settings.data_page_bytes,
        ));
        let mut options = FuseLowLevelBlockWriteOptions::new(
            self.ctx.get_function_context()?,
            operator.clone(),
            self.source_schema.clone(),
            self.write_settings.clone(),
            properties,
            block_location.clone(),
        );
        options.set_statistics(
            self.stats_columns.clone(),
            self.distinct_columns.clone(),
            self.serialize_hll,
        );
        options.set_bloom_indexes(
            self.meta_locations.block_bloom_index_location(&block_id),
            self.bloom_columns_map.clone(),
            self.ngram_args.clone(),
        );
        options.set_ndv_columns(self.ndv_columns_map.clone());
        if let Some((columns, size)) = &self.top_n {
            options.set_top_n_columns(columns.clone(), *size);
        }
        options.set_inverted_indexes(self.inverted_index_builders.clone());
        options.set_virtual_columns(self.virtual_column_builder.clone());
        if let Some(builder) = self.vector_index_builder.clone() {
            options.set_vector_index(self.meta_locations.block_vector_index_location(), builder);
        }
        if let Some(builder) = self.spatial_index_builder.clone() {
            options.set_spatial_index(self.meta_locations.block_spatial_index_location(), builder);
        }
        if let Some(rows) = self.write_settings.index_granularity {
            let func_ctx = self.ctx.get_function_context()?;
            let writers = self
                .granule_index_specs
                .iter()
                .map(|spec| {
                    spec.new_low_level_writer(
                        func_ctx.clone(),
                        self.source_schema.as_ref(),
                        &block_location.0,
                        operator.clone(),
                        rows,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            options.set_granule_indexes(
                Some(
                    TableMetaLocationGenerator::gen_granule_mins_location_from_block_location(
                        &block_location.0,
                    ),
                ),
                TableMetaLocationGenerator::gen_granule_offsets_location_from_block_location(
                    &block_location.0,
                ),
                writers,
            );
        }
        options.set_cluster_keys(cluster_key_id, cluster_key_types, level, None);
        Ok(options)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_block_builder_parts(
        ctx: Arc<dyn TableContext>,
        meta_locations: TableMetaLocationGenerator,
        source_schema: TableSchemaRef,
        write_settings: WriteSettings,
        block_thresholds: BlockThresholds,
        bloom_columns_map: BTreeMap<FieldIndex, TableField>,
        ndv_columns_map: BTreeMap<FieldIndex, TableField>,
        top_n: Option<(BTreeMap<FieldIndex, TableField>, usize)>,
        ngram_args: Vec<NgramArgs>,
        inverted_index_builders: Vec<InvertedIndexBuilder>,
        virtual_column_builder: Option<VirtualColumnBuilder>,
        vector_index_builder: Option<VectorIndexBuilder>,
        spatial_index_builder: Option<SpatialIndexBuilder>,
        granule_index_specs: Vec<Arc<dyn GranuleIndexSpec>>,
        granule_cluster_columns: Option<Vec<databend_common_expression::Column>>,
        table_meta_timestamps: TableMetaTimestamps,
        serialize_hll: bool,
        cluster_stats_override: Option<ClusterStatistics>,
    ) -> Result<Arc<Self>> {
        let bloom_ndv_columns = bloom_columns_map
            .values()
            .chain(ndv_columns_map.values())
            .map(TableField::column_id)
            .collect::<HashSet<_>>();
        let mut stats_columns = Vec::new();
        let mut distinct_columns = Vec::new();
        for field in source_schema.leaf_fields() {
            let column_id = field.column_id();
            let data_type = DataType::from(field.data_type());
            if RangeIndex::supported_type(&data_type) {
                stats_columns.push((column_id, data_type.clone()));
                if !bloom_ndv_columns.contains(&column_id) {
                    distinct_columns.push((column_id, data_type));
                }
            }
        }
        let granule_rows = write_settings.index_granularity;
        Ok(Arc::new(Self {
            ctx,
            write_settings,
            block_thresholds,
            meta_locations,
            source_schema,
            cluster_stats_builder: Arc::new(ClusterStatisticsBuilder::default()),
            stats_columns,
            distinct_columns,
            bloom_columns_map,
            ndv_columns_map,
            top_n,
            ngram_args,
            inverted_index_builders,
            virtual_column_builder,
            table_meta_timestamps,
            vector_index_builder,
            spatial_index_builder,
            granule_index_specs,
            granule_cluster_keys: match granule_cluster_columns {
                Some(columns) => Some(sample_granule_cluster_keys(
                    columns,
                    granule_rows.expect("granule cluster keys require granules"),
                )?),
                None => None,
            },
            serialize_hll,
            cluster_stats_override,
        }))
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::FromData;
    use databend_common_expression::types::number::Int32Type;
    use databend_common_expression::types::string::StringType;

    use super::*;

    #[test]
    fn test_sample_granule_cluster_keys_takes_all_keys_at_each_boundary() {
        let sampled = sample_granule_cluster_keys(
            vec![
                Int32Type::from_data(vec![1, 2, 3, 4, 5]),
                StringType::from_data(vec!["one", "two", "three", "four", "five"]),
            ],
            2,
        )
        .unwrap();

        assert_eq!(sampled, vec![
            Int32Type::from_opt_data(vec![Some(1), Some(3), Some(5)]),
            StringType::from_opt_data(vec![Some("one"), Some("three"), Some("five")]),
        ]);
    }
}
