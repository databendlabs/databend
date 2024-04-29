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

use chrono::Utc;
use databend_common_arrow::arrow::chunk::Chunk as ArrowChunk;
use databend_common_arrow::native::write::NativeWriter;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::FieldIndex;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_io::constants::DEFAULT_BLOCK_BUFFER_SIZE;
use databend_common_io::constants::DEFAULT_BLOCK_INDEX_BUFFER_SIZE;
use databend_storages_common_blocks::blocks_to_parquet;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::table::TableCompression;
use opendal::Operator;

use crate::io::write::WriteSettings;
use crate::io::InvertedIndexWriter;
use crate::io::TableMetaLocationGenerator;
use crate::operations::column_parquet_metas;
use crate::statistics::gen_columns_statistics;
use crate::statistics::ClusterStatsGenerator;
use crate::FuseStorageFormat;

// TODO rename this, it is serialization, or pass in a writer(if not rename)
pub fn serialize_block(
    write_settings: &WriteSettings,
    schema: &TableSchemaRef,
    block: DataBlock,
    buf: &mut Vec<u8>,
) -> Result<HashMap<ColumnId, ColumnMeta>> {
    let schema = Arc::new(schema.remove_virtual_computed_fields());
    match write_settings.storage_format {
        FuseStorageFormat::Parquet => {
            let result =
                blocks_to_parquet(&schema, vec![block], buf, write_settings.table_compression)?;
            let meta = column_parquet_metas(&result, &schema)?;
            Ok(meta)
        }
        FuseStorageFormat::Native => {
            let arrow_schema = schema.as_ref().into();
            let leaf_column_ids = schema.to_leaf_column_ids();

            let mut default_compress_ratio = Some(2.10f64);
            if matches!(write_settings.table_compression, TableCompression::Zstd) {
                default_compress_ratio = Some(3.72f64);
            }

            let mut writer = NativeWriter::new(
                buf,
                arrow_schema,
                databend_common_arrow::native::write::WriteOptions {
                    default_compression: write_settings.table_compression.into(),
                    max_page_size: Some(write_settings.max_page_size),
                    default_compress_ratio,
                    forbidden_compressions: vec![],
                },
            );

            let batch = ArrowChunk::try_from(block)?;

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

pub struct BloomIndexState {
    pub(crate) data: Vec<u8>,
    pub(crate) size: u64,
    pub(crate) location: Location,
    pub(crate) column_distinct_count: HashMap<FieldIndex, usize>,
}

impl BloomIndexState {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        block: &DataBlock,
        location: Location,
        bloom_columns_map: BTreeMap<FieldIndex, TableField>,
    ) -> Result<Option<Self>> {
        // write index
        let maybe_bloom_index = BloomIndex::try_create(
            ctx.get_function_context()?,
            location.1,
            &[block],
            bloom_columns_map,
        )?;
        if let Some(bloom_index) = maybe_bloom_index {
            let index_block = bloom_index.serialize_to_data_block()?;
            let filter_schema = bloom_index.filter_schema;
            let column_distinct_count = bloom_index.column_distinct_count;
            let mut data = Vec::with_capacity(DEFAULT_BLOCK_INDEX_BUFFER_SIZE);
            let index_block_schema = &filter_schema;
            let _ = blocks_to_parquet(
                index_block_schema,
                vec![index_block],
                &mut data,
                TableCompression::None,
            )?;
            let data_size = data.len() as u64;
            Ok(Some(Self {
                data,
                size: data_size,
                location,
                column_distinct_count,
            }))
        } else {
            Ok(None)
        }
    }
}

#[derive(Clone)]
pub struct InvertedIndexBuilder {
    pub name: String,
    pub version: String,
    pub schema: DataSchema,
    pub options: BTreeMap<String, String>,
}

pub struct InvertedIndexState {
    pub(crate) data: Vec<u8>,
    pub(crate) size: u64,
    pub(crate) location: Location,
}

impl InvertedIndexState {
    pub fn try_create(
        source_schema: &TableSchemaRef,
        block: &DataBlock,
        block_location: &Location,
        inverted_index_builder: &InvertedIndexBuilder,
    ) -> Result<Self> {
        let mut writer = InvertedIndexWriter::try_create(
            Arc::new(inverted_index_builder.schema.clone()),
            &inverted_index_builder.options,
        )?;
        writer.add_block(source_schema, block)?;
        let data = writer.finalize()?;
        let size = data.len() as u64;

        let inverted_index_location =
            TableMetaLocationGenerator::gen_inverted_index_location_from_block_location(
                &block_location.0,
                &inverted_index_builder.name,
                &inverted_index_builder.version,
            );

        Ok(Self {
            data,
            size,
            location: (inverted_index_location, 0),
        })
    }
}

pub struct BlockSerialization {
    pub block_raw_data: Vec<u8>,
    pub size: u64, // TODO redundancy
    pub block_meta: BlockMeta,
    pub bloom_index_state: Option<BloomIndexState>,
    pub inverted_index_states: Vec<InvertedIndexState>,
}

#[derive(Clone)]
pub struct BlockBuilder {
    pub ctx: Arc<dyn TableContext>,
    pub meta_locations: TableMetaLocationGenerator,
    pub source_schema: TableSchemaRef,
    pub write_settings: WriteSettings,
    pub cluster_stats_gen: ClusterStatsGenerator,
    pub bloom_columns_map: BTreeMap<FieldIndex, TableField>,
    pub inverted_index_builders: Vec<InvertedIndexBuilder>,
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
            &data_block,
            bloom_index_location,
            self.bloom_columns_map.clone(),
        )?;
        let column_distinct_count = bloom_index_state
            .as_ref()
            .map(|i| i.column_distinct_count.clone());

        let mut inverted_index_states = Vec::with_capacity(self.inverted_index_builders.len());
        for inverted_index_builder in &self.inverted_index_builders {
            let inverted_index_state = InvertedIndexState::try_create(
                &self.source_schema,
                &data_block,
                &block_location,
                inverted_index_builder,
            )?;
            inverted_index_states.push(inverted_index_state);
        }

        let row_count = data_block.num_rows() as u64;
        let block_size = data_block.memory_size() as u64;
        let col_stats =
            gen_columns_statistics(&data_block, column_distinct_count, &self.source_schema)?;

        let mut buffer = Vec::with_capacity(DEFAULT_BLOCK_BUFFER_SIZE);
        let col_metas = serialize_block(
            &self.write_settings,
            &self.source_schema,
            data_block,
            &mut buffer,
        )?;
        let file_size = buffer.len() as u64;
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
            compression: self.write_settings.table_compression.into(),
            create_on: Some(Utc::now()),
        };

        let serialized = BlockSerialization {
            block_raw_data: buffer,
            size: file_size,
            block_meta,
            bloom_index_state,
            inverted_index_states,
        };
        Ok(serialized)
    }
}
