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
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::types::F32;
use databend_common_expression::types::VectorColumn;
use databend_common_meta_app::schema::TableIndex;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_metrics::storage::metrics_inc_block_vector_index_generate_milliseconds;
use databend_storages_common_index::DistanceType;
use databend_storages_common_index::HNSWIndex;
use databend_storages_common_index::VectorBundleBuilder;
use databend_storages_common_index::normalize_vector;
use databend_storages_common_index::vector_stat_distance;
use databend_storages_common_table_meta::meta::BlockIndexMeta;
use databend_storages_common_table_meta::meta::StatisticsOfVectorColumns;
use databend_storages_common_table_meta::meta::VectorColumnStatistics;
use databend_storages_common_table_meta::table::VECTOR_INDEX_OPT_DISTANCE;
use databend_storages_common_table_meta::table::VECTOR_INDEX_OPT_EF_CONSTRUCT;
use databend_storages_common_table_meta::table::VECTOR_INDEX_OPT_M;
use databend_storages_common_table_meta::table::VECTOR_INDEX_OPT_REFINE;
use databend_storages_common_table_meta::table::parse_vector_index_refine;
use log::debug;
use log::info;
use opendal::Buffer;

use crate::io::TableMetaLocationGenerator;
use crate::io::write::block_index::BlockIndexSpec;
use crate::io::write::block_index::BlockIndexWriteContext;
use crate::io::write::block_index::BlockIndexWriter;
use crate::io::write::block_index::PendingBlockIndexOutput;
use crate::io::write::block_index::PendingIndexFile;
use crate::io::write::block_index::PendingVectorIndex;

const DEFAULT_M: usize = 16;
const DEFAULT_EF_CONSTRUCT: usize = 100;

/// Whether the index stores FP16 vectors for refinement (`refine = true`).
fn vector_index_refine(options: &BTreeMap<String, String>) -> Result<bool> {
    match options.get(VECTOR_INDEX_OPT_REFINE) {
        None => Ok(false),
        Some(value) => parse_vector_index_refine(value)
            .ok_or_else(|| ErrorCode::IndexOptionInvalid("`refine` must be `true` or `false`")),
    }
}

/// Query-time search parameters, from the `vector_refine_factor` and
/// `vector_search_ef` settings (0 disables / uses the default).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct VectorIndexSearchOptions {
    /// Candidate multiplier; set only when the index stores FP16 data.
    pub refine_factor: Option<usize>,
    pub search_ef: Option<usize>,
}

impl VectorIndexSearchOptions {
    pub fn new(
        index_options: &BTreeMap<String, String>,
        refine_factor: u64,
        search_ef: u64,
    ) -> Result<Self> {
        let refine = refine_factor > 0 && vector_index_refine(index_options)?;
        Ok(Self {
            refine_factor: refine.then_some(refine_factor as usize),
            search_ef: (search_ef > 0).then_some(search_ef as usize),
        })
    }
}

#[derive(Debug, Clone)]
struct VectorIndexParam {
    index_name: String,
    index_version: String,
    m: usize,
    ef_construct: usize,
    refine: bool,
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

pub(crate) struct VectorIndexWriteSpec {
    builder: VectorIndexBuilder,
}

impl BlockIndexSpec for VectorIndexWriteSpec {
    fn new_writer(&self, context: BlockIndexWriteContext) -> Result<Box<dyn BlockIndexWriter>> {
        Ok(Box::new(VectorIndexBlockWriter {
            builder: self.builder.clone(),
            locations: context.meta_locations,
        }))
    }
}

struct VectorIndexBlockWriter {
    builder: VectorIndexBuilder,
    locations: TableMetaLocationGenerator,
}

impl BlockIndexWriter for VectorIndexBlockWriter {
    fn write(&mut self, block: &DataBlock) -> Result<()> {
        self.builder.add_block(block)
    }

    fn finish(mut self: Box<Self>) -> Result<PendingBlockIndexOutput> {
        Ok(PendingBlockIndexOutput {
            vector: Some(self.builder.finalize_block(&self.locations)?),
            ..Default::default()
        })
    }
}

impl VectorIndexBuilder {
    pub(crate) fn into_write_spec(self) -> VectorIndexWriteSpec {
        VectorIndexWriteSpec { builder: self }
    }

    pub fn try_create(
        table_indexes: &BTreeMap<String, TableIndex>,
        schema: TableSchemaRef,
        is_sync: bool,
    ) -> Result<Option<VectorIndexBuilder>> {
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

            // Validated at bind time too; invalid persisted metadata must return an
            // error, not silently omit the index.
            let refine = vector_index_refine(&index.options)?;
            let m = match index.options.get(VECTOR_INDEX_OPT_M) {
                Some(value) => value.parse::<usize>().unwrap_or(DEFAULT_M),
                None => DEFAULT_M,
            };

            let ef_construct = match index.options.get(VECTOR_INDEX_OPT_EF_CONSTRUCT) {
                Some(value) => value.parse::<usize>().unwrap_or(DEFAULT_EF_CONSTRUCT),
                None => DEFAULT_EF_CONSTRUCT,
            };

            let mut distances = Vec::new();
            match index.options.get(VECTOR_INDEX_OPT_DISTANCE) {
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
                // Without refine the bundle contains HNSW components only.
                refine,
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
            Ok(Some(VectorIndexBuilder {
                index_params,
                field_offsets_set,
                statistics_params,
                columns,
            }))
        } else {
            Ok(None)
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

    /// Builds one bundle file per index from the buffered rows.
    pub(crate) fn finalize_block(
        &mut self,
        locations: &TableMetaLocationGenerator,
    ) -> Result<PendingVectorIndex> {
        let start = Instant::now();
        let columns = self.take_concated_columns()?;
        let statistics = self.build_vector_statistics(&columns)?;
        // All columns are concatenated from the same blocks.
        let row_count = columns.values().next().map(Column::len).unwrap_or(0);
        let mut files = Vec::with_capacity(self.index_params.len());
        for param in &self.index_params {
            let mut bundle = VectorBundleBuilder::new(
                param.index_name.clone(),
                param.index_version.clone(),
                row_count as u64,
            );
            for &(offset, column_id) in &param.field_offsets {
                let column = columns
                    .get(&offset)
                    .ok_or_else(|| ErrorCode::Internal("Vector source column missing"))?;
                // Common to all distance representations and optional FP16 pages.
                // Non-nullable and nullable-but-all-valid columns need no bitmap.
                let validity = column
                    .as_nullable()
                    .map(|nullable| &nullable.validity)
                    .filter(|bitmap| bitmap.null_count() != 0);
                if let Some(bitmap) = validity {
                    bundle.add_validity(column_id, bitmap)?;
                }
                for &distance in &param.distances {
                    let (fields, entries) = HNSWIndex::build(
                        param.m,
                        param.ef_construct,
                        column_id,
                        column.clone(),
                        distance,
                    )?;
                    for (field, entry) in fields.into_iter().zip(entries) {
                        let column = entry.to_column();
                        let bytes = column
                            .as_binary()
                            .and_then(|column| column.index(0))
                            .ok_or_else(|| ErrorCode::Internal("Invalid HNSW component"))?;
                        bundle.add_hnsw(field.name().clone(), bytes)?;
                    }
                }
                if param.refine {
                    bundle.add_fp16_data(column_id, column)?;
                }
            }
            let file = PendingIndexFile {
                location: locations.gen_vector_index_v2_location(&param.index_version),
                data: Buffer::from(bundle.finish()?),
            };
            let meta = BlockIndexMeta {
                index_name: param.index_name.clone(),
                index_version: param.index_version.clone(),
                location: file.location.clone(),
                size: file.size(),
            };
            files.push((meta, file));
        }
        metrics_inc_block_vector_index_generate_milliseconds(start.elapsed().as_millis() as u64);
        Ok(PendingVectorIndex { files, statistics })
    }

    fn take_concated_columns(&mut self) -> Result<BTreeMap<usize, Column>> {
        let mut columns = BTreeMap::new();
        for offset in &self.field_offsets_set {
            columns.insert(*offset, vec![]);
        }
        std::mem::swap(&mut self.columns, &mut columns);

        let mut concated_columns = BTreeMap::new();
        for (offset, columns) in columns.into_iter() {
            let concated_column = Column::concat_columns(columns)?;
            concated_columns.insert(offset, concated_column);
        }

        Ok(concated_columns)
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
