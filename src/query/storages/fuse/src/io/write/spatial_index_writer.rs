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
use databend_common_expression::types::geometry::extract_geo_and_srid;
use databend_common_io::constants::DEFAULT_BLOCK_INDEX_BUFFER_SIZE;
use databend_common_meta_app::schema::TableIndex;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_metrics::storage::metrics_inc_block_spatial_index_generate_milliseconds;
use databend_storages_common_blocks::blocks_to_parquet;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::StatisticsOfSpatialColumns;
use databend_storages_common_table_meta::table::TableCompression;
use geo::algorithm::bounding_rect::BoundingRect;
use geo_index::rtree::RTreeBuilder;
use geo_index::rtree::sort::HilbertSort;
use log::debug;
use log::info;
use parquet::file::metadata::KeyValue;

use crate::statistics::SpatialStatsBuilder;

#[derive(Debug, Clone)]
pub struct SpatialIndexState {
    pub location: Location,
    pub size: u64,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct SpatialIndexBuildResult {
    pub index_state: Option<SpatialIndexState>,
    pub spatial_stats: Option<StatisticsOfSpatialColumns>,
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
    columns: HashMap<usize, Vec<Column>>,

    stats_only_offsets: Vec<(usize, ColumnId)>,
    spatial_stats: HashMap<ColumnId, SpatialStatsBuilder>,
}

impl SpatialIndexBuilder {
    pub fn try_create(
        table_indexes: &BTreeMap<String, TableIndex>,
        schema: TableSchemaRef,
        is_sync: bool,
    ) -> Option<SpatialIndexBuilder> {
        let mut spatial_columns = HashSet::new();
        let mut spatial_offsets = Vec::new();
        for (offset, field) in schema.fields.iter().enumerate() {
            let data_type = field.data_type().remove_nullable();
            if matches!(
                data_type,
                TableDataType::Geometry | TableDataType::Geography
            ) {
                spatial_columns.insert(field.column_id());
                spatial_offsets.push((offset, field.column_id()));
            }
        }
        if spatial_columns.is_empty() {
            return None;
        }

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

        let mut columns = HashMap::new();
        for offset in &field_offsets_set {
            columns.insert(*offset, vec![]);
        }

        let indexed_columns = field_offsets
            .iter()
            .flatten()
            .map(|(_, column_id)| *column_id)
            .collect::<HashSet<_>>();
        let stats_only_offsets = spatial_offsets
            .into_iter()
            .filter(|(_, column_id)| !indexed_columns.contains(column_id))
            .collect::<Vec<_>>();

        Some(SpatialIndexBuilder {
            index_params,
            field_offsets,
            field_offsets_set,
            columns,
            stats_only_offsets,
            spatial_stats: HashMap::new(),
        })
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
        for (offset, column_id) in &self.stats_only_offsets {
            let block_entry = block.get_by_offset(*offset);
            let spatial_stat = self.spatial_stats.entry(*column_id).or_default();
            if spatial_stat.is_srid_mixed() {
                continue;
            }
            match block_entry {
                BlockEntry::Const(scalar, _, _) => {
                    spatial_stat.update_value(scalar.as_ref())?;
                }
                BlockEntry::Column(col) => {
                    for value in col.iter() {
                        spatial_stat.update_value(value)?;
                        if spatial_stat.is_srid_mixed() {
                            break;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn finalize(&mut self, location: &Location) -> Result<SpatialIndexBuildResult> {
        let mut index_state = None;
        if !self.field_offsets.is_empty() {
            let start = Instant::now();
            info!(
                "Start build spatial R-Tree index for location: {}",
                location.0
            );

            if let Some(result) = self.build_spatial_index()? {
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
                index_state = Some(SpatialIndexState {
                    location: location.clone(),
                    size,
                    data,
                });

                // Perf.
                let elapsed_ms = start.elapsed().as_millis() as u64;
                {
                    metrics_inc_block_spatial_index_generate_milliseconds(elapsed_ms);
                }
                info!(
                    "Finish build spatial index: location={}, size={} bytes in {} ms",
                    location.0, size, elapsed_ms
                );
            }
        }

        let spatial_stats = self.finalize_spatial_stats();
        Ok(SpatialIndexBuildResult {
            index_state,
            spatial_stats,
        })
    }

    fn build_spatial_index(&mut self) -> Result<Option<SpatialIndexResult>> {
        let mut columns = HashMap::new();
        for offset in &self.field_offsets_set {
            columns.insert(*offset, vec![]);
        }
        std::mem::swap(&mut self.columns, &mut columns);

        let mut concated_columns = HashMap::new();
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

                let spatial_stat = self.spatial_stats.entry(*column_id).or_default();

                let mut rects = Vec::with_capacity(column.len());
                for value in column.iter() {
                    let Some((geo, srid)) = extract_geo_and_srid(value)? else {
                        let _ = spatial_stat.update_value(ScalarRef::Null);
                        continue;
                    };
                    let rect = geo.bounding_rect();
                    spatial_stat.update_rect_with_srid(rect, srid);
                    if spatial_stat.is_srid_mixed() {
                        break;
                    }
                    if let Some(rect) = rect {
                        rects.push(rect)
                    }
                }
                // Don't build index if the column SRID is mixed or all rects are empty.
                if !spatial_stat.is_valid() {
                    continue;
                }
                let mut builder = RTreeBuilder::<f64>::new(rects.len() as u32);
                for rect in rects {
                    let min = rect.min();
                    let max = rect.max();
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

        if index_fields.is_empty() {
            return Ok(None);
        }
        let result = SpatialIndexResult {
            index_fields,
            index_columns,
            metadata,
        };
        Ok(Some(result))
    }

    fn finalize_spatial_stats(&mut self) -> Option<StatisticsOfSpatialColumns> {
        if self.spatial_stats.is_empty() {
            return None;
        }
        let mut statistics = HashMap::new();
        for (column_id, spatial_stat) in std::mem::take(&mut self.spatial_stats) {
            let spatial_stat = spatial_stat.finalize();
            statistics.insert(column_id, spatial_stat);
        }
        (!statistics.is_empty()).then_some(statistics)
    }
}

struct SpatialIndexResult {
    index_fields: Vec<TableField>,
    index_columns: Vec<BlockEntry>,
    metadata: Vec<KeyValue>,
}
