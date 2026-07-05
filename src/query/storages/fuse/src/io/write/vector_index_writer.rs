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
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::F32;
use databend_common_expression::types::VectorColumn;
use databend_common_meta_app::schema::TableIndex;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_metrics::storage::metrics_inc_block_vector_index_generate_milliseconds;
use databend_storages_common_blocks::SerializedParquet;
use databend_storages_common_blocks::blocks_to_parquet;
use databend_storages_common_index::DistanceType;
use databend_storages_common_index::HNSWIndex;
use databend_storages_common_index::normalize_vector;
use databend_storages_common_index::vector_stat_distance;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
use databend_storages_common_table_meta::meta::StatisticsOfVectorColumns;
use databend_storages_common_table_meta::meta::VectorColumnStatistics;
use databend_storages_common_table_meta::table::TableCompression;
use log::debug;
use log::info;
use opendal::Buffer;
use opendal::Operator;
use parquet::file::metadata::KeyValue;

use crate::io::read::load_vector_index_files;

const DEFAULT_M: usize = 16;
const DEFAULT_EF_CONSTRUCT: usize = 100;

#[derive(Debug, Clone)]
pub struct VectorIndexState {
    pub location: Location,
    pub size: u64,
    pub data: Buffer,
}

#[derive(Debug, Clone)]
struct VectorIndexParam {
    index_name: String,
    index_version: String,
    m: usize,
    ef_construct: usize,
    distances: Vec<DistanceType>,
    field_offsets: Vec<(usize, ColumnId)>,
}

#[derive(Clone)]
pub struct VectorIndexBuilder {
    // Parameters for each vector index
    index_params: Vec<VectorIndexParam>,
    field_offsets_set: HashSet<usize>,
    statistics_params: BTreeMap<usize, (ColumnId, DistanceType)>,

    // Collected vector columns
    columns: BTreeMap<usize, Vec<Column>>,
}

pub(crate) struct VectorIndexBuildState {
    pub(crate) index_state: Option<VectorIndexState>,
    pub(crate) vector_stats: Option<StatisticsOfVectorColumns>,
}

impl VectorIndexBuilder {
    pub fn try_create(
        table_indexes: &BTreeMap<String, TableIndex>,
        schema: TableSchemaRef,
        is_sync: bool,
    ) -> Option<VectorIndexBuilder> {
        let mut index_params = Vec::with_capacity(table_indexes.len());
        let mut field_offsets_set = HashSet::new();
        let mut statistics_params = BTreeMap::new();

        for index in table_indexes.values() {
            if !matches!(index.index_type, TableIndexType::Vector) {
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
                    "Ignoring invalid vector index: {}, missing columns",
                    index.name
                );
                continue;
            }

            // Parse index parameters
            let m = match index.options.get("m") {
                Some(value) => value.parse::<usize>().unwrap_or(DEFAULT_M),
                None => DEFAULT_M,
            };

            let ef_construct = match index.options.get("ef_construct") {
                Some(value) => value.parse::<usize>().unwrap_or(DEFAULT_EF_CONSTRUCT),
                None => DEFAULT_EF_CONSTRUCT,
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
                debug!(
                    "Ignoring vector index: {}, no valid distance types",
                    index.name
                );
                continue;
            }
            for (offset, column_id) in &offsets {
                field_offsets_set.insert(*offset);
                // Vector statistics currently use only the first configured distance type
                // to avoid scanning the same vectors multiple times during block writing.
                // If stat-based pruning needs full coverage for multi-distance indexes,
                // extend statistics_params to keep all distance types per column.
                statistics_params
                    .entry(*offset)
                    .or_insert((*column_id, distances[0]));
            }
            info!(
                "Added vector index parameters for {}: m={}, ef_construct={}, distances={:?}",
                index.name, m, ef_construct, distances
            );
            let index_param = VectorIndexParam {
                index_name: index.name.clone(),
                index_version: index.version.clone(),
                m,
                ef_construct,
                distances,
                field_offsets: offsets,
            };
            index_params.push(index_param);
        }

        let mut columns = BTreeMap::new();
        for offset in &field_offsets_set {
            columns.insert(*offset, vec![]);
        }

        if !field_offsets_set.is_empty() {
            Some(VectorIndexBuilder {
                index_params,
                field_offsets_set,
                statistics_params,
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

    pub fn finalize(&mut self, location: &Location) -> Result<VectorIndexState> {
        let start = Instant::now();
        info!("Start build vector HNSW index for location: {}", location.0);

        let concated_columns = self.take_concated_columns()?;
        let result = self.build_vector_index(&concated_columns)?;
        let VectorIndexResult {
            index_fields,
            index_columns,
            metadata,
        } = result;

        let index_schema = TableSchemaRefExt::create(index_fields);
        let index_block = DataBlock::new(index_columns, 1);

        let serialized = blocks_to_parquet(
            index_schema.as_ref(),
            vec![index_block],
            // Zstd has the best compression ratio
            TableCompression::Zstd,
            // No dictionary page for vector index
            false,
            Some(metadata),
        )?;
        let size = serialized.len() as u64;
        let data = Buffer::from(serialized.payload);

        let state = VectorIndexState {
            location: location.clone(),
            size,
            data,
        };

        // Perf.
        let elapsed_ms = start.elapsed().as_millis() as u64;
        {
            metrics_inc_block_vector_index_generate_milliseconds(elapsed_ms);
        }
        info!(
            "Finish build vector HNSW index: location={}, size={} bytes in {} ms",
            location.0, size, elapsed_ms
        );

        Ok(state)
    }

    pub(crate) fn finalize_block(&mut self, location: &Location) -> Result<VectorIndexBuildState> {
        let concated_columns = self.take_concated_columns()?;
        let vector_stats = self.build_vector_statistics(&concated_columns)?;
        if self.index_params.is_empty() {
            return Ok(VectorIndexBuildState {
                index_state: None,
                vector_stats,
            });
        }

        let index_state = self.finalize_with_columns(location, &concated_columns)?;
        Ok(VectorIndexBuildState {
            index_state: Some(index_state),
            vector_stats,
        })
    }

    #[async_backtrace::framed]
    pub(crate) async fn finalize_with_existing(
        &mut self,
        operator: Operator,
        settings: &ReadSettings,
        location: &Location,
        existing_location: Option<&Location>,
        existing_column_metas: Option<Vec<(String, SingleColumnMeta)>>,
        existing_index_meta: Option<BTreeMap<String, String>>,
    ) -> Result<VectorIndexBuildState> {
        // If there's no existing vector index, just use the regular finalize method
        if existing_location.is_none() || existing_column_metas.is_none() {
            return self.finalize_block(location);
        }

        // Process new vector index data
        let start = Instant::now();
        info!(
            "Start build merged vector HNSW index for location: {}",
            location.0
        );

        let existing_location = existing_location.unwrap();
        let existing_column_metas = existing_column_metas.unwrap();

        let existing_column_names = existing_column_metas
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();

        let existing_columns = load_vector_index_files(
            operator,
            settings,
            &existing_column_names,
            &existing_location.0,
        )
        .await?;

        info!(
            "Read existing vector index at location={} in {} ms",
            existing_location.0,
            start.elapsed().as_millis() as u64
        );

        let concated_columns = self.take_concated_columns()?;
        let vector_stats = self.build_vector_statistics(&concated_columns)?;
        let result = self.build_vector_index(&concated_columns)?;
        let VectorIndexResult {
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
        let serialized = blocks_to_parquet(
            index_schema.as_ref(),
            vec![index_block],
            // Zstd has the best compression ratio
            TableCompression::Zstd,
            // No dictionary page for vector index
            false,
            Some(metadata),
        )?;
        let size = serialized.len() as u64;
        let data = Buffer::from(serialized.payload);

        let state = VectorIndexState {
            location: location.clone(),
            size,
            data,
        };

        // Perf.
        let elapsed_ms = start.elapsed().as_millis() as u64;
        {
            metrics_inc_block_vector_index_generate_milliseconds(elapsed_ms);
        }
        info!(
            "Finish build merged vector HNSW index: location={}, size={} bytes in {} ms",
            location.0, size, elapsed_ms
        );

        Ok(VectorIndexBuildState {
            index_state: Some(state),
            vector_stats,
        })
    }

    fn finalize_with_columns(
        &self,
        location: &Location,
        concated_columns: &BTreeMap<usize, Column>,
    ) -> Result<VectorIndexState> {
        let start = Instant::now();
        info!("Start build vector HNSW index for location: {}", location.0);

        let result = self.build_vector_index(concated_columns)?;
        let VectorIndexResult {
            index_fields,
            index_columns,
            metadata,
        } = result;

        let index_schema = TableSchemaRefExt::create(index_fields);
        let index_block = DataBlock::new(index_columns, 1);

        let SerializedParquet { payload, .. } = blocks_to_parquet(
            index_schema.as_ref(),
            vec![index_block],
            TableCompression::Zstd,
            false,
            Some(metadata),
        )?;
        let data = Buffer::from(payload);
        let size = data.len() as u64;
        let state = VectorIndexState {
            location: location.clone(),
            size,
            data,
        };

        let elapsed_ms = start.elapsed().as_millis() as u64;
        {
            metrics_inc_block_vector_index_generate_milliseconds(elapsed_ms);
        }
        info!(
            "Finish build vector HNSW index: location={}, size={} bytes in {} ms",
            location.0, size, elapsed_ms
        );

        Ok(state)
    }

    fn take_concated_columns(&mut self) -> Result<BTreeMap<usize, Column>> {
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

        Ok(concated_columns)
    }

    fn build_vector_index(
        &self,
        concated_columns: &BTreeMap<usize, Column>,
    ) -> Result<VectorIndexResult> {
        let mut index_fields = Vec::new();
        let mut index_columns = Vec::new();
        let mut metadata = Vec::with_capacity(self.index_params.len());

        for index_param in &self.index_params {
            debug!("Building HNSW index for {}", index_param.index_name);
            for (offset, column_id) in &index_param.field_offsets {
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
            let version_meta = KeyValue {
                key: index_param.index_name.clone(),
                value: Some(index_param.index_version.clone()),
            };
            metadata.push(version_meta);
        }

        let result = VectorIndexResult {
            index_fields,
            index_columns,
            metadata,
        };
        Ok(result)
    }

    fn build_vector_statistics(
        &self,
        concated_columns: &BTreeMap<usize, Column>,
    ) -> Result<Option<StatisticsOfVectorColumns>> {
        if self.statistics_params.is_empty() {
            return Ok(None);
        }

        let mut statistics = StatisticsOfVectorColumns::new();
        for (offset, (column_id, distance_type)) in &self.statistics_params {
            let Some(column) = concated_columns.get(offset) else {
                return Err(ErrorCode::Internal("Can't find vector column"));
            };
            let vector_distance_type = distance_type.vector_distance_type();
            if let Some(vector_stats) = vector_statistics_from_column(column, *distance_type)? {
                statistics.insert((*column_id, vector_distance_type), vector_stats);
            }
        }

        Ok((!statistics.is_empty()).then_some(statistics))
    }
}

struct VectorIndexResult {
    index_fields: Vec<TableField>,
    index_columns: Vec<BlockEntry>,
    metadata: Vec<KeyValue>,
}

fn vector_statistics_from_column(
    column: &Column,
    distance_type: DistanceType,
) -> Result<Option<VectorColumnStatistics>> {
    match column {
        Column::Nullable(nullable_column) => match &nullable_column.column {
            Column::Vector(VectorColumn::Float32((values, dimension))) => {
                let valid_rows = nullable_column.validity.true_count();
                let null_rows = nullable_column.validity.null_count();
                if null_rows == 0 {
                    vector_statistics_from_vectors(values.as_slice(), *dimension, distance_type)
                } else {
                    let mut valid_values = Vec::with_capacity(valid_rows * *dimension);
                    for (row, vector) in values.chunks_exact(*dimension).enumerate() {
                        if nullable_column.validity.get_bit(row) {
                            valid_values.extend_from_slice(vector);
                        }
                    }
                    vector_statistics_from_vectors(
                        valid_values.as_slice(),
                        *dimension,
                        distance_type,
                    )
                }
            }
            _ => Ok(None),
        },
        Column::Vector(VectorColumn::Float32((values, dimension))) => {
            vector_statistics_from_vectors(values.as_slice(), *dimension, distance_type)
        }
        _ => Ok(None),
    }
}

fn vector_statistics_from_vectors(
    values: &[F32],
    dimension: usize,
    distance_type: DistanceType,
) -> Result<Option<VectorColumnStatistics>> {
    if dimension == 0 || values.is_empty() || !values.len().is_multiple_of(dimension) {
        return Ok(None);
    }

    let rows = values.len() / dimension;
    if matches!(distance_type, DistanceType::Dot) {
        return vector_dot_statistics_from_vectors(values, dimension, rows);
    }

    vector_l1_l2_statistics_from_vectors(values, dimension, rows, distance_type)
}

fn vector_l1_l2_statistics_from_vectors(
    values: &[F32],
    dimension: usize,
    rows: usize,
    distance_type: DistanceType,
) -> Result<Option<VectorColumnStatistics>> {
    let values = values.iter().map(|value| value.0).collect::<Vec<_>>();
    let mut centroid = vec![0.0_f64; dimension];
    for vector in values.chunks_exact(dimension) {
        for (idx, value) in vector.iter().enumerate() {
            centroid[idx] += *value as f64;
        }
    }

    let centroid = centroid
        .into_iter()
        .map(|value| (value / rows as f64) as f32)
        .collect::<Vec<_>>();

    let mut radius = 0.0_f32;
    let vector_distance_type = distance_type.vector_distance_type();
    for vector in values.chunks_exact(dimension) {
        radius = radius.max(vector_stat_distance(
            vector,
            &centroid,
            vector_distance_type,
        )?);
    }

    Ok(Some(VectorColumnStatistics {
        centroid: centroid.into_iter().map(F32::from).collect(),
        radius: F32::from(radius),
        row_count: rows as u64,
    }))
}

fn vector_dot_statistics_from_vectors(
    values: &[F32],
    dimension: usize,
    rows: usize,
) -> Result<Option<VectorColumnStatistics>> {
    let mut normalized_values = Vec::with_capacity(values.len());
    for vector in values.chunks_exact(dimension) {
        let mut vector = vector.iter().map(|value| value.0).collect::<Vec<_>>();
        normalize_vector(&mut vector);
        normalized_values.extend_from_slice(&vector);
    }

    let mut centroid = vec![0.0_f64; dimension];
    for vector in normalized_values.chunks_exact(dimension) {
        for (idx, value) in vector.iter().enumerate() {
            centroid[idx] += *value as f64;
        }
    }

    let mut centroid = centroid
        .into_iter()
        .map(|value| (value / rows as f64) as f32)
        .collect::<Vec<_>>();
    normalize_vector(&mut centroid);

    let mut radius = 0.0_f32;
    let vector_distance_type = DistanceType::Dot.vector_distance_type();
    for vector in normalized_values.chunks_exact(dimension) {
        radius = radius.max(vector_stat_distance(
            vector,
            &centroid,
            vector_distance_type,
        )?);
    }

    Ok(Some(VectorColumnStatistics {
        centroid: centroid.into_iter().map(F32::from).collect(),
        radius: F32::from(radius),
        row_count: rows as u64,
    }))
}
