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
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_io::constants::DEFAULT_BLOCK_INDEX_BUFFER_SIZE;
use databend_common_io::ewkb_to_geo;
use databend_common_meta_app::schema::TableIndex;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_metrics::storage::metrics_inc_block_spatial_index_generate_milliseconds;
use databend_storages_common_blocks::ParquetWriteOptions;
use databend_storages_common_blocks::blocks_to_parquet;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::table::TableCompression;
use geo::algorithm::bounding_rect::BoundingRect;
use geo_index::rtree::RTreeBuilder;
use geo_index::rtree::sort::HilbertSort;
use geozero::ToGeo;
use geozero::wkb::Ewkb;
use log::debug;
use log::info;
use parquet::file::metadata::KeyValue;
use roaring::RoaringBitmap;

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

    pub fn finalize(&mut self, location: &Location) -> Result<Option<SpatialIndexState>> {
        let start = Instant::now();
        info!(
            "Start build spatial R-Tree index for location: {}",
            location.0
        );

        let Some(result) = self.build_spatial_index()? else {
            return Ok(None);
        };
        let SpatialIndexResult {
            index_fields,
            index_columns,
            metadata,
        } = result;

        let index_schema = TableSchemaRefExt::create(index_fields);
        let index_block = DataBlock::new(index_columns, 1);

        let mut data = Vec::with_capacity(DEFAULT_BLOCK_INDEX_BUFFER_SIZE);
        let options = ParquetWriteOptions::builder(TableCompression::Zstd)
            // No dictionary page for spatial index.
            .enable_dictionary(false)
            .metadata(Some(metadata))
            .build();
        let _ = blocks_to_parquet(
            index_schema.as_ref(),
            vec![index_block],
            &mut data,
            &options,
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

        Ok(Some(state))
    }

    fn build_spatial_index(&mut self) -> Result<Option<SpatialIndexResult>> {
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

                let mut column_srid = None;
                let mut srid_mixed = false;
                let mut builder = RTreeBuilder::<f64>::new(column.len() as u32);
                // Track rows that cannot be indexed (null, empty, or invalid geometry).
                let mut invalid_rows_rb = RoaringBitmap::new();
                for (row_idx, value) in column.iter().enumerate() {
                    let (geo, srid) = match value {
                        ScalarRef::Geometry(v) => {
                            let (geo, srid) = ewkb_to_geo(&mut Ewkb(v))?;
                            (geo, srid.unwrap_or(0))
                        }
                        ScalarRef::Geography(v) => {
                            let geo = Ewkb(v.0).to_geo().map_err(|e| {
                                ErrorCode::Internal(format!("Invalid geo ewkb value: {e}"))
                            })?;
                            (geo, 4326)
                        }
                        _ => {
                            invalid_rows_rb.insert(row_idx as u32);
                            builder.add(0.0, 0.0, 0.0, 0.0);
                            continue;
                        }
                    };
                    if let Some(prev) = column_srid {
                        if prev != srid {
                            debug!("Mixed SRID {} and {}", prev, srid);
                            srid_mixed = true;
                            break;
                        }
                    } else {
                        column_srid = Some(srid);
                    }
                    if let Some(rec) = geo.bounding_rect() {
                        let min = rec.min();
                        let max = rec.max();
                        builder.add(min.x, min.y, max.x, max.y);
                    } else {
                        invalid_rows_rb.insert(row_idx as u32);
                        builder.add(0.0, 0.0, 0.0, 0.0);
                    }
                }
                // Don't build index if the column SRID is mixed,
                if srid_mixed {
                    continue;
                }
                let Some(srid) = column_srid else {
                    continue;
                };
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

                index_fields.push(TableField::new(
                    &format!("{}_srid", column_id),
                    TableDataType::Number(NumberDataType::Int32),
                ));
                index_columns.push(BlockEntry::new_const_column(
                    DataType::Number(NumberDataType::Int32),
                    Scalar::Number(NumberScalar::Int32(srid)),
                    1,
                ));
                // Rows not indexed by the RTree (null, empty geometry, or invalid data).
                if !invalid_rows_rb.is_empty() {
                    let mut invalid_rows_buffer = vec![];
                    invalid_rows_rb
                        .serialize_into(&mut invalid_rows_buffer)
                        .unwrap();

                    index_fields.push(TableField::new(
                        &format!("{}_invalid_rows", column_id),
                        TableDataType::Binary,
                    ));
                    index_columns.push(BlockEntry::new_const_column(
                        DataType::Binary,
                        Scalar::Binary(invalid_rows_buffer),
                        1,
                    ));
                }
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
}

struct SpatialIndexResult {
    index_fields: Vec<TableField>,
    index_columns: Vec<BlockEntry>,
    metadata: Vec<KeyValue>,
}
