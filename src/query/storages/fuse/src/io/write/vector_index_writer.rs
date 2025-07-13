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
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_io::constants::DEFAULT_BLOCK_INDEX_BUFFER_SIZE;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_app::schema::TableIndex;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_metrics::storage::metrics_inc_block_vector_index_generate_milliseconds;
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

#[derive(Debug, Clone)]
struct VectorIndexParam {
    index_name: String,
    index_version: String,
    m: usize,
    ef_construct: usize,
    distances: Vec<DistanceType>,
}

#[derive(Clone)]
pub struct VectorIndexBuilder {
    // Parameters for each vector index
    index_params: Vec<VectorIndexParam>,
    field_offsets: Vec<Vec<(usize, ColumnId)>>,
    field_offsets_set: HashSet<usize>,

    // Collected vector columns
    columns: BTreeMap<usize, Vec<Column>>,
}

impl VectorIndexBuilder {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        table_indexes: &BTreeMap<String, TableIndex>,
        schema: TableSchemaRef,
    ) -> Option<VectorIndexBuilder> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(ctx.get_license_key(), Feature::VectorIndex)
            .ok()?;

        let mut index_params = Vec::with_capacity(table_indexes.len());
        let mut field_offsets = Vec::with_capacity(table_indexes.len());
        let mut field_offsets_set = HashSet::new();

        for index in table_indexes.values() {
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
                        offsets.push((offset, *column_id));
                        break;
                    }
                }
            }
            // ignore invalid index
            if offsets.len() != index.column_ids.len() {
                continue;
            }
            for (offset, _) in &offsets {
                field_offsets_set.insert(*offset);
            }
            field_offsets.push(offsets);

            // Parse index parameters
            let m = match index.options.get("m") {
                Some(value) => value.parse::<usize>().unwrap_or(16),
                None => 16,
            };

            let ef_construct = match index.options.get("ef_construct") {
                Some(value) => value.parse::<usize>().unwrap_or(64),
                None => 64,
            };

            let mut distances = Vec::new();
            match index.options.get("distance") {
                Some(value) => {
                    let distance_types: Vec<&str> = value.split(',').collect();
                    for distance_type in distance_types {
                        let distance = match distance_type {
                            "cosine" => DistanceType::Dot,
                            "l1" => DistanceType::L1,
                            "l2" => DistanceType::L2,
                            _ => continue,
                        };
                        distances.push(distance);
                    }
                }
                None => continue,
            };
            if distances.is_empty() {
                continue;
            }
            let index_param = VectorIndexParam {
                index_name: index.name.clone(),
                index_version: index.version.clone(),
                m,
                ef_construct,
                distances,
            };
            index_params.push(index_param);
        }

        let mut columns = BTreeMap::new();
        for offset in &field_offsets_set {
            columns.insert(*offset, vec![]);
        }

        if !field_offsets.is_empty() {
            Some(VectorIndexBuilder {
                index_params,
                field_offsets,
                field_offsets_set,
                columns,
            })
        } else {
            None
        }
    }

    pub fn add_block(&mut self, block: &DataBlock) -> Result<()> {
        for offset in &self.field_offsets_set {
            let block_entry = block.get_by_offset(*offset);
            let column = block_entry.to_column();

            if let Some(columns) = self.columns.get_mut(offset) {
                columns.push(column);
            } else {
                return Err(ErrorCode::Internal("Can't find vector column"));
            }
        }
        Ok(())
    }

    #[async_backtrace::framed]
    pub fn finalize(&mut self, location: &Location) -> Result<VectorIndexState> {
        let start = Instant::now();

        let mut columns = BTreeMap::new();
        for offset in &self.field_offsets_set {
            columns.insert(*offset, vec![]);
        }
        std::mem::swap(&mut self.columns, &mut columns);

        let mut concated_columns = BTreeMap::new();
        for (offset, columns) in columns.into_iter() {
            let concated_column = Column::concat_columns(columns.into_iter())?;
            concated_columns.insert(offset, concated_column);
        }

        let mut index_fields = Vec::new();
        let mut index_columns = Vec::new();
        let mut metadata = BTreeMap::new();

        for (field_offsets, index_param) in self.field_offsets.iter().zip(&self.index_params) {
            for (offset, column_id) in field_offsets {
                let Some(column) = concated_columns.get(offset) else {
                    return Err(ErrorCode::Internal("Can't find vector column"));
                };
                for distance in &index_param.distances {
                    let (mut hnsw_index_fields, mut hnsw_index_columns) = HNSWIndex::build(
                        index_param.m,
                        index_param.ef_construct,
                        *column_id,
                        column.clone(),
                        *distance,
                    )?;
                    index_fields.append(&mut hnsw_index_fields);
                    index_columns.append(&mut hnsw_index_columns);
                }
            }
            metadata.insert(
                index_param.index_name.clone(),
                index_param.index_version.clone(),
            );
        }

        let index_schema = TableSchemaRefExt::create(index_fields);
        let index_block = DataBlock::new(index_columns, 1);

        let mut data = Vec::with_capacity(DEFAULT_BLOCK_INDEX_BUFFER_SIZE);
        let _ = blocks_to_parquet(
            index_schema.as_ref(),
            vec![index_block],
            &mut data,
            // Zstd has the best compression ratio
            TableCompression::Zstd,
        )?;

        let size = data.len() as u64;
        let state = VectorIndexState {
            location: location.clone(),
            size,
            data,
        };

        // Perf.
        {
            metrics_inc_block_vector_index_generate_milliseconds(start.elapsed().as_millis() as u64);
        }

        Ok(state)
    }
}
