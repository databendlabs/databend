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

use std::collections::hash_map::Entry;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::mem;
use std::sync::Arc;

use chrono::Utc;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockThresholds;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::ComputedExpr;
use databend_common_expression::DataBlock;
use databend_common_expression::FieldIndex;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_io::constants::DEFAULT_BLOCK_BUFFER_SIZE;
use databend_common_meta_app::schema::TableIndex;
use databend_common_native::write::NativeWriter;
use databend_common_native::write::WriteOptions;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_storages_common_blocks::parquet_writer_properties_builder;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_index::BloomIndexBuilder;
use databend_storages_common_index::Index;
use databend_storages_common_index::NgramArgs;
use databend_storages_common_index::RangeIndex;
use databend_storages_common_table_meta::meta::BlockHLLState;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::table::TableCompression;
use parquet::arrow::ArrowWriter;

use crate::io::create_inverted_index_builders;
use crate::io::write::stream::cluster_statistics::ClusterStatisticsBuilder;
use crate::io::write::stream::cluster_statistics::ClusterStatisticsState;
use crate::io::write::stream::ColumnStatisticsState;
use crate::io::write::BlockStatsBuilder;
use crate::io::write::InvertedIndexState;
use crate::io::BlockSerialization;
use crate::io::BloomIndexState;
use crate::io::InvertedIndexBuilder;
use crate::io::InvertedIndexWriter;
use crate::io::TableMetaLocationGenerator;
use crate::io::VectorIndexBuilder;
use crate::io::VirtualColumnBuilder;
use crate::io::WriteSettings;
use crate::operations::column_parquet_metas;
use crate::FuseStorageFormat;
use crate::FuseTable;

pub enum BlockWriterImpl {
    Arrow(ArrowWriter<Vec<u8>>),
    // Native format doesnot support stream write.
    Native(NativeWriter<Vec<u8>>),
}

pub trait BlockWriter {
    fn start(&mut self) -> Result<()>;

    fn write(&mut self, block: DataBlock, schema: &TableSchema) -> Result<()>;

    fn finish(&mut self, schema: &TableSchemaRef) -> Result<HashMap<ColumnId, ColumnMeta>>;

    fn compressed_size(&self) -> usize;

    fn inner_mut(&mut self) -> &mut Vec<u8>;
}

impl BlockWriter for BlockWriterImpl {
    fn start(&mut self) -> Result<()> {
        match self {
            BlockWriterImpl::Arrow(_) => Ok(()),
            BlockWriterImpl::Native(writer) => Ok(writer.start()?),
        }
    }

    fn write(&mut self, block: DataBlock, schema: &TableSchema) -> Result<()> {
        match self {
            BlockWriterImpl::Arrow(writer) => {
                let batch = block.to_record_batch(schema)?;
                writer.write(&batch)?;
            }
            BlockWriterImpl::Native(writer) => {
                let block = block.consume_convert_to_full();
                let batch: Vec<Column> = block
                    .take_columns()
                    .into_iter()
                    .map(|x| x.into_column().unwrap())
                    .collect();
                writer.write(&batch)?;
            }
        }
        Ok(())
    }

    fn finish(&mut self, schema: &TableSchemaRef) -> Result<HashMap<ColumnId, ColumnMeta>> {
        match self {
            BlockWriterImpl::Arrow(writer) => {
                let file_meta = writer.finish()?;
                column_parquet_metas(&file_meta, schema)
            }
            BlockWriterImpl::Native(writer) => {
                writer.finish()?;
                let mut metas = HashMap::with_capacity(writer.metas.len());
                let leaf_column_ids = schema.to_leaf_column_ids();
                for (idx, meta) in writer.metas.iter().enumerate() {
                    // use column id as key instead of index
                    let column_id = leaf_column_ids.get(idx).unwrap();
                    metas.insert(*column_id, ColumnMeta::Native(meta.clone()));
                }
                Ok(metas)
            }
        }
    }

    fn inner_mut(&mut self) -> &mut Vec<u8> {
        match self {
            BlockWriterImpl::Arrow(writer) => writer.inner_mut(),
            BlockWriterImpl::Native(writer) => writer.inner_mut(),
        }
    }

    fn compressed_size(&self) -> usize {
        match self {
            BlockWriterImpl::Arrow(writer) => writer.in_progress_size(),
            BlockWriterImpl::Native(writer) => writer.total_size(),
        }
    }
}

pub struct StreamBlockBuilder {
    properties: Arc<StreamBlockProperties>,
    block_writer: BlockWriterImpl,
    inverted_index_writers: Vec<InvertedIndexWriter>,
    bloom_index_builder: BloomIndexBuilder,
    virtual_column_builder: Option<VirtualColumnBuilder>,
    vector_index_builder: Option<VectorIndexBuilder>,
    block_stats_builder: BlockStatsBuilder,

    cluster_stats_state: ClusterStatisticsState,
    column_stats_state: ColumnStatisticsState,

    row_count: usize,
    block_size: usize,
}

impl StreamBlockBuilder {
    pub fn try_new_with_config(properties: Arc<StreamBlockProperties>) -> Result<Self> {
        let buffer = Vec::with_capacity(DEFAULT_BLOCK_BUFFER_SIZE);
        let block_writer = match properties.write_settings.storage_format {
            FuseStorageFormat::Parquet => {
                let write_settings = &properties.write_settings;
                // TODO NDV for heuristic rule
                let props = parquet_writer_properties_builder(
                    write_settings.table_compression,
                    write_settings.enable_parquet_dictionary,
                    None,
                )
                .build();

                let arrow_schema = Arc::new(properties.source_schema.as_ref().into());
                let writer = ArrowWriter::try_new(buffer, arrow_schema, Some(props))?;
                BlockWriterImpl::Arrow(writer)
            }
            FuseStorageFormat::Native => {
                let mut default_compress_ratio = Some(2.10f64);
                if matches!(
                    properties.write_settings.table_compression,
                    TableCompression::Zstd
                ) {
                    default_compress_ratio = Some(3.72f64);
                }

                let writer = NativeWriter::new(
                    buffer,
                    properties.source_schema.as_ref().clone(),
                    WriteOptions {
                        default_compression: properties.write_settings.table_compression.into(),
                        max_page_size: Some(properties.write_settings.max_page_size),
                        default_compress_ratio,
                        forbidden_compressions: vec![],
                    },
                )?;
                BlockWriterImpl::Native(writer)
            }
        };

        let inverted_index_writers = properties
            .inverted_index_builders
            .iter()
            .map(|builder| {
                InvertedIndexWriter::try_create(Arc::new(builder.schema.clone()), &builder.options)
            })
            .collect::<Result<Vec<_>>>()?;

        let bloom_index_builder = BloomIndexBuilder::create(
            properties.ctx.get_function_context()?,
            properties.bloom_columns_map.clone(),
            &properties.ngram_args,
        )?;

        let virtual_column_builder = properties.virtual_column_builder.clone();
        let vector_index_builder = VectorIndexBuilder::try_create(
            &properties.table_indexes,
            properties.source_schema.clone(),
            true,
        );
        let block_stats_builder = BlockStatsBuilder::new(&properties.ndv_columns_map);
        let cluster_stats_state =
            ClusterStatisticsState::new(properties.cluster_stats_builder.clone());
        let column_stats_state =
            ColumnStatisticsState::new(&properties.stats_columns, &properties.distinct_columns);

        Ok(StreamBlockBuilder {
            properties,
            block_writer,
            inverted_index_writers,
            bloom_index_builder,
            virtual_column_builder,
            vector_index_builder,
            block_stats_builder,
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
        let file_size = self.block_writer.compressed_size();
        self.row_count >= self.properties.block_thresholds.min_rows_per_block
            || self.block_size >= self.properties.block_thresholds.min_bytes_per_block * 2
            || (file_size >= self.properties.block_thresholds.min_compressed_per_block
                && self.block_size >= self.properties.block_thresholds.min_bytes_per_block)
    }

    pub fn write(&mut self, block: DataBlock) -> Result<()> {
        if block.is_empty() {
            return Ok(());
        }

        if self.row_count == 0 {
            self.block_writer.start()?;
        }

        let block = self.cluster_stats_state.add_block(block)?;
        self.column_stats_state
            .add_block(&self.properties.source_schema, &block)?;
        self.bloom_index_builder.add_block(&block)?;
        self.block_stats_builder.add_block(&block)?;
        for writer in self.inverted_index_writers.iter_mut() {
            writer.add_block(&self.properties.source_schema, &block)?;
        }
        if let Some(ref mut virtual_column_builder) = self.virtual_column_builder {
            virtual_column_builder.add_block(&block)?;
        }
        if let Some(ref mut vector_index_builder) = self.vector_index_builder {
            vector_index_builder.add_block(&block)?;
        }
        self.row_count += block.num_rows();
        self.block_size += block.estimate_block_size();
        self.block_writer
            .write(block, &self.properties.source_schema)?;
        Ok(())
    }

    pub fn finish(mut self) -> Result<BlockSerialization> {
        let (block_location, block_id) = self
            .properties
            .meta_locations
            .gen_block_location(self.properties.table_meta_timestamps);

        let bloom_index_location = self
            .properties
            .meta_locations
            .block_bloom_index_location(&block_id);
        let bloom_index = self.bloom_index_builder.finalize()?;
        let bloom_index_state = if let Some(bloom_index) = bloom_index {
            Some(BloomIndexState::from_bloom_index(
                &bloom_index,
                bloom_index_location,
            )?)
        } else {
            None
        };
        let mut column_distinct_count = bloom_index_state
            .as_ref()
            .map(|i| i.column_distinct_count.clone())
            .unwrap_or_default();
        let column_hlls = self.block_stats_builder.finalize()?;
        if let Some(hlls) = &column_hlls {
            for (key, val) in hlls {
                if let Entry::Vacant(entry) = column_distinct_count.entry(*key) {
                    entry.insert(val.count());
                }
            }
        }
        let col_stats = self.column_stats_state.finalize(column_distinct_count)?;

        let mut inverted_index_states = Vec::with_capacity(self.inverted_index_writers.len());
        for (i, inverted_index_writer) in std::mem::take(&mut self.inverted_index_writers)
            .into_iter()
            .enumerate()
        {
            let inverted_index_location = self.properties.inverted_index_builders[i]
                .gen_inverted_index_location(&block_location);
            let data = inverted_index_writer.finalize()?;
            let inverted_index_state =
                InvertedIndexState::try_create(data, inverted_index_location)?;
            inverted_index_states.push(inverted_index_state);
        }
        let virtual_column_state =
            if let Some(ref mut virtual_column_builder) = self.virtual_column_builder {
                let virtual_column_state = virtual_column_builder
                    .finalize(&self.properties.write_settings, &block_location)?;
                Some(virtual_column_state)
            } else {
                None
            };
        let vector_index_state =
            if let Some(ref mut vector_index_builder) = self.vector_index_builder {
                let vector_index_location =
                    self.properties.meta_locations.block_vector_index_location();
                let vector_index_state = vector_index_builder.finalize(&vector_index_location)?;
                Some(vector_index_state)
            } else {
                None
            };
        let vector_index_size = vector_index_state.as_ref().map(|v| v.size);
        let vector_index_location = vector_index_state.as_ref().map(|v| v.location.clone());

        let col_metas = self.block_writer.finish(&self.properties.source_schema)?;
        let block_raw_data = mem::take(self.block_writer.inner_mut());

        let file_size = block_raw_data.len();
        let inverted_index_size = inverted_index_states
            .iter()
            .map(|v| v.size)
            .reduce(|a, b| a + b);
        let perfect = self.properties.block_thresholds.check_perfect_block(
            self.row_count,
            self.block_size,
            file_size,
        );
        let cluster_stats = self.cluster_stats_state.finalize(perfect)?;
        let block_meta = BlockMeta {
            row_count: self.row_count as u64,
            block_size: self.block_size as u64,
            file_size: file_size as u64,
            col_stats,
            col_metas,
            cluster_stats,
            location: block_location,
            bloom_filter_index_location: bloom_index_state.as_ref().map(|v| v.location.clone()),
            bloom_filter_index_size: bloom_index_state
                .as_ref()
                .map(|v| v.size)
                .unwrap_or_default(),
            compression: self.properties.write_settings.table_compression.into(),
            inverted_index_size,
            vector_index_size,
            vector_index_location,
            create_on: Some(Utc::now()),
            ngram_filter_index_size: bloom_index_state
                .as_ref()
                .map(|v| v.ngram_size)
                .unwrap_or_default(),
            virtual_block_meta: None,
        };
        let serialized = BlockSerialization {
            block_raw_data,
            block_meta,
            bloom_index_state,
            inverted_index_states,
            virtual_column_state,
            vector_index_state,
            column_hlls: column_hlls.map(BlockHLLState::Deserialized),
        };
        Ok(serialized)
    }
}

pub struct StreamBlockProperties {
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
    ngram_args: Vec<NgramArgs>,
    inverted_index_builders: Vec<InvertedIndexBuilder>,
    virtual_column_builder: Option<VirtualColumnBuilder>,
    table_meta_timestamps: TableMetaTimestamps,
    table_indexes: BTreeMap<String, TableIndex>,
}

impl StreamBlockProperties {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        table: &FuseTable,
        kind: MutationKind,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<Arc<Self>> {
        // remove virtual computed fields.
        let mut fields = table
            .schema()
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
            ..table.schema().as_ref().clone()
        });

        let write_settings = table.get_write_settings();

        let bloom_columns_map = table
            .bloom_index_cols
            .bloom_index_fields(source_schema.clone(), BloomIndex::supported_type)?;
        let ngram_args = FuseTable::create_ngram_index_args(
            &table.table_info.meta,
            &table.table_info.meta.schema,
            true,
        )?;
        let ndv_columns_map = table
            .approx_distinct_cols
            .distinct_column_fields(source_schema.clone(), RangeIndex::supported_table_type)?;
        let bloom_ndv_columns = bloom_columns_map
            .values()
            .chain(ndv_columns_map.values())
            .map(|v| v.column_id())
            .collect::<HashSet<_>>();

        let inverted_index_builders = create_inverted_index_builders(&table.table_info.meta);
        let virtual_column_builder = if table.support_virtual_columns() {
            VirtualColumnBuilder::try_create(ctx.clone(), source_schema.clone()).ok()
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
        let table_indexes = table.table_info.meta.indexes.clone();
        Ok(Arc::new(StreamBlockProperties {
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
            ngram_args,
            inverted_index_builders,
            table_meta_timestamps,
            table_indexes,
            ndv_columns_map,
        }))
    }
}
