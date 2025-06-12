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
use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_io::constants::DEFAULT_BLOCK_INDEX_BUFFER_SIZE;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_app::schema::TableIndex;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_meta_app::schema::TableMeta;
use databend_storages_common_blocks::blocks_to_parquet;
use databend_storages_common_index::DistanceType;
use databend_storages_common_index::HNSWIndex;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::table::TableCompression;

#[derive(Debug, Clone)]
pub struct VectorIndexState {
    pub location: Location,
    pub size: u64,
    pub data: Vec<u8>,
}

#[derive(Clone)]
pub struct VectorIndexBuilder {
    // vector field offset and ColumnId
    vector_indexes: Vec<(TableIndex, Vec<usize>)>,
}

impl VectorIndexBuilder {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        table_meta: &TableMeta,
        schema: TableSchemaRef,
    ) -> Option<VectorIndexBuilder> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(ctx.get_license_key(), Feature::VectorIndex)
            .ok()?;

        let mut vector_indexes = Vec::with_capacity(table_meta.indexes.len());
        for index in table_meta.indexes.values() {
            if !matches!(index.index_type, TableIndexType::Vector) {
                continue;
            }
            if !index.sync_creation {
                continue;
            }
            let mut offsets = Vec::with_capacity(index.column_ids.len());
            for column_id in &index.column_ids {
                for (offset, field) in schema.fields.iter().enumerate() {
                    if field.column_id() == *column_id {
                        offsets.push(offset);
                        break;
                    }
                }
            }
            // ignore invalid index
            if offsets.len() != index.column_ids.len() {
                continue;
            }
            vector_indexes.push((index.clone(), offsets));
        }
        if !vector_indexes.is_empty() {
            Some(VectorIndexBuilder { vector_indexes })
        } else {
            None
        }
    }

    pub fn add_block(&self, block: &DataBlock, location: Location) -> Result<VectorIndexState> {
        let mut index_fields = Vec::new();
        let mut index_columns = Vec::new();
        let mut metadata = BTreeMap::new();

        for (vector_index, offsets) in &self.vector_indexes {
            let m = match vector_index.options.get("m") {
                Some(value) => value.parse::<usize>()?,
                None => 16,
            };
            let ef_construct = match vector_index.options.get("ef_construct") {
                Some(value) => value.parse::<usize>()?,
                None => 64,
            };

            for (column_id, offset) in vector_index.column_ids.iter().zip(offsets.iter()) {
                let block_entry = block.get_by_offset(*offset);
                let column = block_entry.to_column();
                // Generate encoded vectors for each of the three different DistanceTypes,
                // so that the indexes can be used by the various vector functions.
                let distance_types = vec![DistanceType::Dot, DistanceType::L1, DistanceType::L2];
                for distance_type in distance_types {
                    let (mut hnsw_index_fields, mut hnsw_index_columns) = HNSWIndex::build(
                        m,
                        ef_construct,
                        *column_id,
                        column.clone(),
                        distance_type,
                    )?;
                    index_fields.append(&mut hnsw_index_fields);
                    index_columns.append(&mut hnsw_index_columns);
                }
            }

            metadata.insert(vector_index.name.clone(), vector_index.version.clone());
        }

        let index_schema = TableSchemaRefExt::create(index_fields);
        let index_block = DataBlock::new(index_columns, 1);

        let mut data = Vec::with_capacity(DEFAULT_BLOCK_INDEX_BUFFER_SIZE);
        let _ = blocks_to_parquet(
            index_schema.as_ref(),
            vec![index_block],
            &mut data,
            TableCompression::Zstd,
        )?;

        let size = data.len() as u64;
        let state = VectorIndexState {
            location,
            size,
            data,
        };

        Ok(state)
    }
}
