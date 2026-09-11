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
use databend_common_expression::types::geometry::extract_bbox_and_srid;
use databend_common_meta_app::schema::TableIndex;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_metrics::storage::metrics_inc_block_spatial_index_generate_milliseconds;
use databend_storages_common_blocks::block_to_parquet_with_writer;
use databend_storages_common_blocks::blocks_to_parquet;
use databend_storages_common_io::OpenDalBlockingWrite;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
use databend_storages_common_table_meta::meta::StatisticsOfSpatialColumns;
use databend_storages_common_table_meta::table::TableCompression;
use geo_index::rtree::RTreeBuilder;
use geo_index::rtree::sort::HilbertSort;
use log::debug;
use log::info;
use opendal::Buffer;
use opendal::Operator;
use parquet::file::metadata::KeyValue;

use crate::io::read::load_spatial_index_files;
use crate::io::write::block_index::BlockIndexLowLevelColumnWriter;
use crate::io::write::block_index::BlockIndexLowLevelWriteContext;
use crate::io::write::block_index::BlockIndexLowLevelWriter;
use crate::io::write::block_index::BlockIndexSpec;
use crate::io::write::block_index::BlockIndexWriteContext;
use crate::io::write::block_index::BlockIndexWriter;
use crate::io::write::block_index::PendingBlockIndexOutput;
use crate::io::write::block_index::PendingIndexFile;
use crate::io::write::block_index::PendingSpatialIndex;
use crate::io::write::block_index::WrittenBlockIndexOutput;
use crate::io::write::block_index::WrittenIndexFile;
use crate::io::write::block_index::WrittenSpatialIndex;
use crate::statistics::SpatialStatsBuilder;

#[derive(Debug, Clone)]
pub struct SpatialIndexState {
    pub location: Location,
    pub size: u64,
    pub data: Buffer,
}

#[derive(Debug, Clone)]
pub struct SpatialIndexBuildResult {
    pub index_state: Option<SpatialIndexState>,
    pub spatial_stats: Option<StatisticsOfSpatialColumns>,
}

pub(crate) struct SpatialIndexWrittenState {
    pub(crate) size: u64,
    pub(crate) spatial_stats: Option<StatisticsOfSpatialColumns>,
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
            if matches!(data_type, TableDataType::Geometry) {
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

    pub fn add_column(&mut self, field_index: usize, column: Column) -> Result<()> {
        if let Some(columns) = self.columns.get_mut(&field_index) {
            columns.push(column.clone());
        }
        if let Some((_, column_id)) = self
            .stats_only_offsets
            .iter()
            .find(|(offset, _)| *offset == field_index)
        {
            let spatial_stat = self.spatial_stats.entry(*column_id).or_default();
            if !spatial_stat.is_srid_mixed() {
                for value in column.iter() {
                    spatial_stat.update_value(value)?;
                    if spatial_stat.is_srid_mixed() {
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn into_write_spec(
        self,
        location: Location,
        num_fields: usize,
    ) -> SpatialIndexWriteSpec {
        SpatialIndexWriteSpec {
            builder: self,
            location,
            num_fields,
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

    pub(crate) fn finalize_to_writer(
        &mut self,
        write: OpenDalBlockingWrite,
    ) -> Result<SpatialIndexWrittenState> {
        let size = match self.build_spatial_index()? {
            Some(result) => {
                let index_schema = TableSchemaRefExt::create(result.index_fields);
                let index_block = DataBlock::new(result.index_columns, 1);
                let (_, write) = block_to_parquet_with_writer(
                    index_schema.as_ref(),
                    index_block,
                    TableCompression::Zstd,
                    false,
                    Some(result.metadata),
                    write,
                )?;
                write.bytes_written()
            }
            None => 0,
        };
        Ok(SpatialIndexWrittenState {
            size,
            spatial_stats: self.finalize_spatial_stats(),
        })
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
                let state = Self::serialize_spatial_index(result, location)?;
                let size = state.size;
                index_state = Some(state);

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

    #[async_backtrace::framed]
    pub async fn finalize_with_existing(
        &mut self,
        operator: Operator,
        settings: &ReadSettings,
        location: &Location,
        existing_location: Option<&Location>,
        existing_column_metas: Option<Vec<(String, SingleColumnMeta)>>,
        existing_index_meta: Option<BTreeMap<String, String>>,
    ) -> Result<SpatialIndexBuildResult> {
        if existing_location.is_none()
            || (existing_column_metas.is_none() && existing_index_meta.is_none())
        {
            return self.finalize(location);
        }

        let start = Instant::now();
        info!(
            "Start build merged spatial R-Tree index for location: {}",
            location.0
        );

        let existing_location = existing_location.unwrap();
        let existing_column_metas = existing_column_metas.unwrap_or_default();
        let existing_column_names = existing_column_metas
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        let existing_columns = if existing_column_names.is_empty() {
            Vec::new()
        } else {
            load_spatial_index_files(
                operator,
                settings,
                &existing_column_names,
                &existing_location.0,
            )
            .await?
        };

        let mut result = self.build_spatial_index()?.unwrap_or(SpatialIndexResult {
            index_fields: Vec::new(),
            index_columns: Vec::new(),
            metadata: Vec::new(),
        });

        for (name, _) in existing_column_metas.into_iter() {
            result
                .index_fields
                .push(TableField::new(&name, TableDataType::Binary));
        }
        for existing_column in existing_columns.into_iter() {
            result
                .index_columns
                .push(BlockEntry::Column(existing_column));
        }
        if let Some(existing_index_meta) = existing_index_meta {
            for (key, value) in existing_index_meta {
                result.metadata.push(KeyValue {
                    key,
                    value: Some(value),
                });
            }
        }

        let index_state = if result.index_fields.is_empty() {
            None
        } else {
            Some(Self::serialize_spatial_index(result, location)?)
        };
        let spatial_stats = self.finalize_spatial_stats();

        let elapsed_ms = start.elapsed().as_millis() as u64;
        metrics_inc_block_spatial_index_generate_milliseconds(elapsed_ms);
        info!(
            "Finish build merged spatial index: location={}, cost={} ms",
            location.0, elapsed_ms
        );

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
                    let Some((bbox, srid)) = extract_bbox_and_srid(value)? else {
                        let _ = spatial_stat.update_value(ScalarRef::Null);
                        continue;
                    };
                    spatial_stat.update_rect_with_srid(bbox, srid);
                    if spatial_stat.is_srid_mixed() {
                        break;
                    }
                    if let Some(bbox) = bbox {
                        rects.push(bbox)
                    }
                }
                // Don't build index if the column SRID is mixed or all rects are empty.
                if !spatial_stat.is_valid() {
                    continue;
                }
                let mut builder = RTreeBuilder::<f64>::new(rects.len() as u32);
                for rect in rects {
                    let (min_x, min_y, max_x, max_y) = rect.corners();
                    builder.add(min_x, min_y, max_x, max_y);
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

    fn serialize_spatial_index(
        result: SpatialIndexResult,
        location: &Location,
    ) -> Result<SpatialIndexState> {
        let SpatialIndexResult {
            index_fields,
            index_columns,
            metadata,
        } = result;

        let index_schema = TableSchemaRefExt::create(index_fields);
        let index_block = DataBlock::new(index_columns, 1);

        let serialized = blocks_to_parquet(
            index_schema.as_ref(),
            vec![index_block],
            TableCompression::Zstd,
            false,
            Some(metadata),
        )?;
        let size = serialized.len() as u64;
        let data = Buffer::from(serialized.payload);

        Ok(SpatialIndexState {
            location: location.clone(),
            size,
            data,
        })
    }
}

pub(crate) struct SpatialIndexWriteSpec {
    builder: SpatialIndexBuilder,
    location: Location,
    num_fields: usize,
}

impl BlockIndexSpec for SpatialIndexWriteSpec {
    fn new_writer(&self, _context: BlockIndexWriteContext) -> Result<Box<dyn BlockIndexWriter>> {
        Ok(Box::new(SpatialIndexWriter {
            builder: self.builder.clone(),
            location: self.location.clone(),
        }))
    }

    fn new_low_level_writer(
        &self,
        context: BlockIndexLowLevelWriteContext,
    ) -> Result<Box<dyn BlockIndexLowLevelWriter>> {
        let write = context.create_write(&self.location);
        Ok(Box::new(SpatialIndexLowLevelWriter {
            builder: self.builder.clone(),
            location: self.location.clone(),
            write: Some(write),
            next_field: 0,
            num_fields: self.num_fields,
        }))
    }
}

struct SpatialIndexWriter {
    builder: SpatialIndexBuilder,
    location: Location,
}

impl BlockIndexWriter for SpatialIndexWriter {
    fn write(&mut self, block: &DataBlock) -> Result<()> {
        self.builder.add_block(block)
    }

    fn finish(mut self: Box<Self>) -> Result<PendingBlockIndexOutput> {
        let result = self.builder.finalize(&self.location)?;
        Ok(PendingBlockIndexOutput {
            spatial: Some(PendingSpatialIndex {
                file: result.index_state.map(|state| PendingIndexFile {
                    location: state.location,
                    data: state.data,
                }),
                statistics: result.spatial_stats,
            }),
            ..Default::default()
        })
    }
}

struct SpatialIndexLowLevelWriter {
    builder: SpatialIndexBuilder,
    location: Location,
    write: Option<OpenDalBlockingWrite>,
    next_field: usize,
    num_fields: usize,
}

impl BlockIndexLowLevelWriter for SpatialIndexLowLevelWriter {
    fn next_column(mut self: Box<Self>) -> Result<Box<dyn BlockIndexLowLevelColumnWriter>> {
        if self.next_field >= self.num_fields {
            return Err(ErrorCode::Internal(
                "spatial index low-level writer has no remaining columns",
            ));
        }
        let field_index = self.next_field;
        self.next_field += 1;
        Ok(Box::new(SpatialIndexLowLevelColumnWriter {
            parent: Some(self),
            field_index,
        }))
    }

    fn finish(mut self: Box<Self>) -> Result<WrittenBlockIndexOutput> {
        if self.next_field != self.num_fields {
            return Err(ErrorCode::Internal(format!(
                "spatial index low-level writer consumed {} of {} columns",
                self.next_field, self.num_fields
            )));
        }
        let write = self
            .write
            .take()
            .ok_or_else(|| ErrorCode::Internal("spatial index blocking output was consumed"))?;
        let result = self.builder.finalize_to_writer(write)?;
        Ok(WrittenBlockIndexOutput {
            spatial: Some(WrittenSpatialIndex {
                file: (result.size > 0).then_some(WrittenIndexFile {
                    location: self.location,
                    size: result.size,
                }),
                statistics: result.spatial_stats,
            }),
            ..Default::default()
        })
    }
}

struct SpatialIndexLowLevelColumnWriter {
    parent: Option<Box<SpatialIndexLowLevelWriter>>,
    field_index: usize,
}

impl BlockIndexLowLevelColumnWriter for SpatialIndexLowLevelColumnWriter {
    fn write(&mut self, column: &Column) -> Result<()> {
        self.parent
            .as_mut()
            .ok_or_else(|| ErrorCode::Internal("spatial index column writer has no parent"))?
            .builder
            .add_column(self.field_index, column.clone())
    }

    fn finish(mut self: Box<Self>) -> Result<Box<dyn BlockIndexLowLevelWriter>> {
        self.parent
            .take()
            .map(|parent| parent as Box<dyn BlockIndexLowLevelWriter>)
            .ok_or_else(|| ErrorCode::Internal("spatial index column writer has no parent"))
    }
}

struct SpatialIndexResult {
    index_fields: Vec<TableField>,
    index_columns: Vec<BlockEntry>,
    metadata: Vec<KeyValue>,
}
