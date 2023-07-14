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
use common_arrow::arrow::chunk::Chunk as ArrowChunk;
use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_arrow::native::write::NativeWriter;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::ColumnId;
use common_expression::DataBlock;
use common_expression::FieldIndex;
use common_expression::TableField;
use common_expression::TableSchemaRef;
use common_io::constants::DEFAULT_BLOCK_BUFFER_SIZE;
use common_io::constants::DEFAULT_BLOCK_INDEX_BUFFER_SIZE;
use opendal::Operator;
use storages_common_blocks::blocks_to_parquet;
use storages_common_index::BloomIndex;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::ClusterStatistics;
use storages_common_table_meta::meta::ColumnMeta;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::StatisticsOfColumns;
use storages_common_table_meta::table::TableCompression;

use crate::fuse_table::FuseStorageFormat;
use crate::io::write::WriteSettings;
use crate::io::TableMetaLocationGenerator;
use crate::operations::util;
use crate::statistics::gen_columns_statistics;
use crate::statistics::ClusterStatsGenerator;

// TODO rename this, it is serialization, or pass in a writer(if not rename)
pub fn serialize_block(
    write_settings: &WriteSettings,
    schema: &TableSchemaRef,
    block: DataBlock,
    stats: Option<&StatisticsOfColumns>,
    buf: &mut Vec<u8>,
) -> Result<(u64, HashMap<ColumnId, ColumnMeta>)> {
    let schema = Arc::new(schema.remove_virtual_computed_fields());
    match write_settings.storage_format {
        FuseStorageFormat::Parquet => {
            let result =
                blocks_to_parquet(&schema, vec![block], buf, write_settings.table_compression)?;
            let meta = util::column_parquet_metas(&result.1, &schema)?;
            Ok((result.0, meta))
        }
        FuseStorageFormat::Native => {
            let arrow_schema = schema.to_arrow();
            let leaf_column_ids = schema.to_leaf_column_ids();
            let column_compressions =
                auto_column_compression(&block, &schema, &leaf_column_ids, stats);

            let mut writer = NativeWriter::new(
                buf,
                arrow_schema,
                common_arrow::native::write::WriteOptions {
                    default_compression: write_settings.table_compression.into(),
                    max_page_size: Some(write_settings.max_page_size),
                    column_compressions,
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

            Ok((writer.total_size() as u64, metas))
        }
    }
}

/// Native format support DictionaryEncoding
fn auto_column_compression(
    block: &DataBlock,
    table_schema: &TableSchemaRef,
    leaf_column_ids: &[u32],
    stats: Option<&StatisticsOfColumns>,
) -> HashMap<usize, common_arrow::native::Compression> {
    let mut column_compressions = HashMap::new();
    let num_rows = block.num_rows();
    let dict_compressor = common_arrow::native::Compression::Dict.create_compressor();
    let leaf_fields = table_schema.leaf_fields();

    for (idx, column_id) in leaf_column_ids.iter().enumerate() {
        let field = leaf_fields.get(idx).unwrap();
        let field_type = field.data_type();
        let arrow_type = ArrowType::from(field_type);

        if !matches!(arrow_type, ArrowType::Binary | ArrowType::LargeBinary) {
            continue;
        }
        let distinct = stats
            .and_then(|stats| stats.get(column_id))
            .and_then(|s| s.distinct_of_values)
            .unwrap_or(num_rows as u64);

        if num_rows > 0 && distinct > 0 {
            let ratio = distinct as f64 / num_rows as f64;
            if ratio <= 0.01 {
                println!("using dict encoding for {:?} ration {ratio}", field.name());
                column_compressions.insert(idx, common_arrow::native::Compression::Dict);
            }
        }
    }
    column_compressions
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
            let (size, _) = blocks_to_parquet(
                index_block_schema,
                vec![index_block],
                &mut data,
                TableCompression::None,
            )?;
            Ok(Some(Self {
                data,
                size,
                location,
                column_distinct_count,
            }))
        } else {
            Ok(None)
        }
    }
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
    pub bloom_columns_map: BTreeMap<FieldIndex, TableField>,
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

        let row_count = data_block.num_rows() as u64;
        let block_size = data_block.memory_size() as u64;
        let col_stats =
            gen_columns_statistics(&data_block, column_distinct_count, &self.source_schema)?;

        let mut buffer = Vec::with_capacity(DEFAULT_BLOCK_BUFFER_SIZE);
        let (file_size, col_metas) = serialize_block(
            &self.write_settings,
            &self.source_schema,
            data_block,
            Some(&col_stats),
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
            create_on: Some(Utc::now()),
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
