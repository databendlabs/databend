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
use std::time::Instant;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::DataType;
use databend_common_io::constants::DEFAULT_BLOCK_INDEX_BUFFER_SIZE;
use databend_common_meta_app::schema::TableIndex;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_metrics::storage::metrics_inc_block_spatial_index_generate_milliseconds;
use databend_storages_common_blocks::blocks_to_parquet;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
use databend_storages_common_table_meta::table::TableCompression;
use geo::algorithm::bounding_rect::BoundingRect;
use geo_index::rtree::RTreeBuilder;
use geo_index::rtree::sort::HilbertSort;
use geozero::ToGeo;
use geozero::wkb::Ewkb;
use log::debug;
use log::info;
use opendal::Operator;
use parquet::file::metadata::KeyValue;

use crate::io::read::load_spatial_index_files;

#[derive(Debug, Clone)]
pub struct SpatialIndexState {
    pub location: Location,
    pub size: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
struct SpatialIndexParam {
    index_name: String,
    index_version: String,
}

#[derive(Clone)]
pub struct SpatialIndexBuilder {
    // Parameters for each spatial index
    index_params: Vec<SpatialIndexParam>,
    field_offsets: Vec<Vec<(usize, ColumnId)>>,
    field_offsets_set: HashSet<usize>,

    // Collected spatial index columns
    columns: BTreeMap<usize, Vec<Column>>,
}

impl SpatialIndexBuilder {
    pub fn try_create(
        table_indexes: &BTreeMap<String, TableIndex>,
        schema: TableSchemaRef,
        is_sync: bool,
    ) -> Option<SpatialIndexBuilder> {
        let mut index_params = Vec::with_capacity(table_indexes.len());
        let mut field_offsets = Vec::with_capacity(table_indexes.len());
        let mut field_offsets_set = HashSet::new();

        for index in table_indexes.values() {
            if !matches!(index.index_type, TableIndexType::Spatial) {
                continue;
            }
            if is_sync && !index.sync_creation {
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
                debug!(
                    "Ignoring invalid spatial index: {}, missing columns",
                    index.name
                );
                continue;
            }
            for (offset, _) in &offsets {
                field_offsets_set.insert(*offset);
            }
            field_offsets.push(offsets);

            let index_param = SpatialIndexParam {
                index_name: index.name.clone(),
                index_version: index.version.clone(),
            };
            index_params.push(index_param);
        }

        let mut columns = BTreeMap::new();
        for offset in &field_offsets_set {
            columns.insert(*offset, vec![]);
        }

        if !field_offsets.is_empty() {
            Some(SpatialIndexBuilder {
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
                return Err(ErrorCode::Internal("Can't find spatial index column"));
            }
        }
        Ok(())
    }

    pub fn finalize(&mut self, location: &Location) -> Result<SpatialIndexState> {
        let start = Instant::now();
        info!(
            "Start build spatial R-Tree index for location: {}",
            location.0
        );

        let result = self.build_spatial_index()?;
        let SpatialIndexResult {
            index_fields,
            index_columns,
            metadata,
        } = result;

        let index_schema = TableSchemaRefExt::create(index_fields);
        let index_block = DataBlock::new(index_columns, 1);

        let mut data = Vec::with_capacity(DEFAULT_BLOCK_INDEX_BUFFER_SIZE);
        let _ = blocks_to_parquet(
            index_schema.as_ref(),
            vec![index_block],
            &mut data,
            // Zstd has the best compression ratio
            TableCompression::Zstd,
            // No dictionary page for spatial index
            false,
            Some(metadata),
        )?;

        let size = data.len() as u64;
        let state = SpatialIndexState {
            location: location.clone(),
            size,
            data,
        };

        // Perf.
        let elapsed_ms = start.elapsed().as_millis() as u64;
        {
            metrics_inc_block_spatial_index_generate_milliseconds(elapsed_ms);
        }
        info!(
            "Finish build spatial index: location={}, size={} bytes in {} ms",
            location.0, size, elapsed_ms
        );

        Ok(state)
    }

    #[async_backtrace::framed]
    pub async fn finalize_with_existing(
        &mut self,
        operator: Operator,
        settings: &ReadSettings,
        location: &Location,
        existing_location: Option<&Location>,
        existing_column_metas: Option<Vec<(String, SingleColumnMeta)>>,
        existing_index_meta: Option<BTreeMap<String, String>>,
    ) -> Result<SpatialIndexState> {
        // If there's no existing spatial index, just use the regular finalize method
        if existing_location.is_none() || existing_column_metas.is_none() {
            return self.finalize(location);
        }

        // Process new spatial index data
        let start = Instant::now();
        info!(
            "Start build merged spatial index for location: {}",
            location.0
        );

        let existing_location = existing_location.unwrap();
        let existing_column_metas = existing_column_metas.unwrap();

        let existing_column_names = existing_column_metas
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();

        let existing_columns = load_spatial_index_files(
            operator,
            settings,
            &existing_column_names,
            &existing_location.0,
        )
        .await?;

        info!(
            "Read existing spatial index at location={} in {} ms",
            existing_location.0,
            start.elapsed().as_millis() as u64
        );

        let result = self.build_spatial_index()?;
        let SpatialIndexResult {
            mut index_fields,
            mut index_columns,
            mut metadata,
        } = result;

        for (name, _) in existing_column_metas.into_iter() {
            let existing_field = TableField::new(&name, TableDataType::Binary);
            index_fields.push(existing_field);
        }
        for existing_column in existing_columns.into_iter() {
            index_columns.push(BlockEntry::Column(existing_column));
        }

        if let Some(existing_index_meta) = existing_index_meta {
            for (key, value) in &existing_index_meta {
                let version_meta = KeyValue {
                    key: key.clone(),
                    value: Some(value.clone()),
                };
                metadata.push(version_meta);
            }
        }

        // Create merged index
        let index_schema = TableSchemaRefExt::create(index_fields);
        let index_block = DataBlock::new(index_columns, 1);

        // Serialize to parquet
        let mut data = Vec::with_capacity(DEFAULT_BLOCK_INDEX_BUFFER_SIZE);
        let _ = blocks_to_parquet(
            index_schema.as_ref(),
            vec![index_block],
            &mut data,
            // Zstd has the best compression ratio
            TableCompression::Zstd,
            // No dictionary page for spatial index
            false,
            Some(metadata),
        )?;

        let size = data.len() as u64;
        let state = SpatialIndexState {
            location: location.clone(),
            size,
            data,
        };

        // Perf.
        let elapsed_ms = start.elapsed().as_millis() as u64;
        {
            metrics_inc_block_spatial_index_generate_milliseconds(elapsed_ms);
        }
        info!(
            "Finish build merged spatial index: location={}, size={} bytes in {} ms",
            location.0, size, elapsed_ms
        );

        Ok(state)
    }

    fn build_spatial_index(&mut self) -> Result<SpatialIndexResult> {
        let mut columns = BTreeMap::new();
        for offset in &self.field_offsets_set {
            columns.insert(*offset, vec![]);
        }
        std::mem::swap(&mut self.columns, &mut columns);

        let mut concated_columns = BTreeMap::new();
        for (offset, columns) in columns.into_iter() {
            let concated_column = if columns.len() == 1 {
                columns[0].clone()
            } else {
                Column::concat_columns(columns.into_iter())?
            };
            concated_columns.insert(offset, concated_column);
        }

        let mut index_fields = Vec::new();
        let mut index_columns = Vec::new();
        let mut metadata = Vec::with_capacity(self.index_params.len());

        for (field_offsets, index_param) in self.field_offsets.iter().zip(&self.index_params) {
            debug!("Building Spatial index for {}", index_param.index_name);
            for (offset, column_id) in field_offsets {
                let Some(column) = concated_columns.get(offset) else {
                    return Err(ErrorCode::Internal("Can't find spatial index column"));
                };

                let mut builder = RTreeBuilder::<f64>::new(column.len() as u32);
                for value in column.iter() {
                    let ewkb = match value {
                        ScalarRef::Geometry(v) => v,
                        ScalarRef::Geography(v) => v.0,
                        _ => {
                            builder.add(0.0f64, 0.0f64, 0.0f64, 0.0f64);
                            continue;
                        }
                    };
                    let Ok(geo) = Ewkb(ewkb).to_geo() else {
                        return Err(ErrorCode::Internal("Invalid geo ewkb value"));
                    };

                    let Some(rec) = geo.bounding_rect() else {
                        builder.add(0.0f64, 0.0f64, 0.0f64, 0.0f64);
                        continue;
                    };

                    let min = rec.min();
                    let max = rec.max();
                    builder.add(min.x, min.y, max.x, max.y);
                }

                let tree = builder.finish::<HilbertSort>();
                let buffer = tree.into_inner();

                index_fields.push(TableField::new(
                    &format!("{}", column_id),
                    TableDataType::Binary,
                ));

                index_columns.push(BlockEntry::new_const_column(
                    DataType::Binary,
                    Scalar::Binary(buffer),
                    1,
                ));
            }
            let version_meta = KeyValue {
                key: index_param.index_name.clone(),
                value: Some(index_param.index_version.clone()),
            };
            metadata.push(version_meta);
        }

        let result = SpatialIndexResult {
            index_fields,
            index_columns,
            metadata,
        };
        Ok(result)
    }
}

struct SpatialIndexResult {
    index_fields: Vec<TableField>,
    index_columns: Vec<BlockEntry>,
    metadata: Vec<KeyValue>,
}
