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
use std::mem;
use std::sync::Arc;

use chrono::Utc;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::FieldIndex;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_io::constants::DEFAULT_BLOCK_BUFFER_SIZE;
use databend_common_native::write::NativeWriter;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::table::TableCompression;
use parquet::arrow::ArrowWriter;
use parquet::basic::Encoding;
use parquet::file::properties::EnabledStatistics;
use parquet::file::properties::WriterProperties;

use crate::io::write::stream::cluster_statistics::ClusterStatisticsBuilder;
use crate::io::write::stream::cluster_statistics::ClusterStatisticsState;
use crate::io::write::stream::column_statistics::ColumnStatisticsState;
use crate::io::write::InvertedIndexState;
use crate::io::BlockSerialization;
use crate::io::BloomIndexBuilder;
use crate::io::BloomIndexState;
use crate::io::InvertedIndexBuilder;
use crate::io::InvertedIndexWriter;
use crate::io::TableMetaLocationGenerator;
use crate::io::WriteSettings;
use crate::operations::column_parquet_metas;
use crate::FuseStorageFormat;

pub enum BlockWriterImpl {
    Arrow(ArrowWriter<Vec<u8>>),
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
                    .map(|x| x.value.into_column().unwrap())
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
                let props = WriterProperties::builder()
                    .set_compression(properties.write_settings.table_compression.into())
                    // use `usize::MAX` to effectively limit the number of row groups to 1
                    .set_max_row_group_size(usize::MAX)
                    .set_encoding(Encoding::PLAIN)
                    .set_dictionary_enabled(false)
                    .set_statistics_enabled(EnabledStatistics::None)
                    .set_bloom_filter_enabled(false)
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
                    databend_common_native::write::WriteOptions {
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
        );

        let cluster_stats_state =
            ClusterStatisticsState::new(properties.cluster_stats_builder.clone());
        let column_stats_state =
            ColumnStatisticsState::new(&properties.stats_columns, &properties.distinct_columns);

        Ok(StreamBlockBuilder {
            properties,
            block_writer,
            inverted_index_writers,
            bloom_index_builder,
            row_count: 0,
            block_size: 0,
            column_stats_state,
            cluster_stats_state,
        })
    }

    pub fn write(&mut self, block: DataBlock, schema: &TableSchemaRef) -> Result<()> {
        if block.is_empty() {
            return Ok(());
        }

        if self.row_count == 0 {
            self.block_writer.start()?;
        }

        let block = self.cluster_stats_state.add_block(block)?;
        self.column_stats_state.add_block(schema, &block)?;
        self.bloom_index_builder.add_block(&block)?;
        for writer in self.inverted_index_writers.iter_mut() {
            writer.add_block(schema, &block)?;
        }

        self.row_count += block.num_rows();
        self.block_size += block.estimate_block_size();
        self.block_writer.write(block, schema)?;
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
        let column_distinct_count = bloom_index_state
            .as_ref()
            .map(|i| i.column_distinct_count.clone())
            .unwrap_or_default();
        let col_stats = self.column_stats_state.finalize(column_distinct_count)?;
        let cluster_stats = self.cluster_stats_state.finalize()?;

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

        let col_metas = self.block_writer.finish(&self.properties.source_schema)?;
        let block_raw_data = mem::take(self.block_writer.inner_mut());

        let file_size = block_raw_data.len() as u64;
        let inverted_index_size = inverted_index_states
            .iter()
            .map(|v| v.size)
            .reduce(|a, b| a + b);
        let block_meta = BlockMeta {
            row_count: self.row_count as u64,
            block_size: self.block_size as u64,
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
            compression: self.properties.write_settings.table_compression.into(),
            inverted_index_size,
            create_on: Some(Utc::now()),
        };
        let serialized = BlockSerialization {
            block_raw_data,
            size: file_size,
            block_meta,
            bloom_index_state,
            inverted_index_states,
        };
        Ok(serialized)
    }
}

pub struct StreamBlockProperties {
    ctx: Arc<dyn TableContext>,
    meta_locations: TableMetaLocationGenerator,
    source_schema: TableSchemaRef,
    write_settings: WriteSettings,

    cluster_stats_builder: Arc<ClusterStatisticsBuilder>,
    stats_columns: Vec<ColumnId>,
    distinct_columns: Vec<ColumnId>,
    bloom_columns_map: BTreeMap<FieldIndex, TableField>,
    inverted_index_builders: Vec<InvertedIndexBuilder>,
    table_meta_timestamps: TableMetaTimestamps,
}
