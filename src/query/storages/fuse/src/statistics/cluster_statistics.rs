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

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::HashMap;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::BlockThresholds;
use databend_common_expression::ColumnId;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::Domain;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::TableSchema;
use databend_common_expression::compare_scalars;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_functions::aggregates::eval_aggr;
use databend_common_meta_app::schema::TableIndex;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_sql::HILBERT_CLUSTER_DIMENSIONS;
use databend_common_sql::evaluator::BlockOperator;
use databend_storages_common_index::statistics_to_domain;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::PartitionStatistics;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::meta::VectorDistanceType;
use databend_storages_common_table_meta::meta::valid_cluster_stats_hilbert_minmax;
use log::warn;

/// Vector cluster metadata resolved from a vector cluster key expression.
#[derive(Clone, Debug)]
pub struct VectorClusterInfo {
    pub key_index: usize,
    pub column_id: ColumnId,
    pub dimension: usize,
    pub distance_type: VectorDistanceType,
}

/// Runtime metadata for injecting vector cluster ids into the physical sort key.
#[derive(Clone, Debug)]
pub struct VectorClusterOperator {
    pub info: VectorClusterInfo,
    pub vector_column_input_offset: usize,
    pub vector_cluster_id_offset: usize,
}

/// Mutually exclusive runtime layout used to generate cluster statistics.
#[derive(Clone, Debug, Default)]
pub enum ClusterStatsLayout {
    #[default]
    Linear,
    Vector(VectorClusterOperator),
    Hilbert,
}

impl ClusterStatsLayout {
    /// Physical offset of the generated vector proxy key used only for sorting.
    fn proxy_sort_key_offset(&self) -> Option<usize> {
        match self {
            Self::Vector(operator) => Some(operator.vector_cluster_id_offset),
            Self::Linear | Self::Hilbert => None,
        }
    }
}

/// Cluster statistics and reusable state produced for block serialization.
pub struct ClusterStatsState {
    /// Persisted cluster ranges for the block, when clustering is active.
    pub cluster_stats: Option<ClusterStatistics>,
    /// Data block to serialize after temporary cluster-key columns are removed.
    pub data_block: DataBlock,
    /// Direct-column bounds reusable by the column-statistics pass.
    pub column_min_max: HashMap<ColumnId, (Option<Scalar>, Option<Scalar>)>,
}

impl ClusterStatsState {
    fn without_stats(data_block: DataBlock) -> Self {
        Self {
            cluster_stats: None,
            data_block,
            column_min_max: HashMap::new(),
        }
    }
}

/// Resolve vector cluster metadata from table indexes and the vector key column.
pub fn vector_cluster_info_from_column(
    table_indexes: &BTreeMap<String, TableIndex>,
    key_index: usize,
    column_id: ColumnId,
    column_name: &str,
    dimension: usize,
) -> Result<VectorClusterInfo> {
    let distances = table_indexes
        .values()
        .filter(|index| {
            index.index_type == TableIndexType::Vector && index.column_ids.contains(&column_id)
        })
        .map(|index| index.options.get("distance").map(String::as_str));

    let distance_type = VectorDistanceType::from_index_options(column_name, distances)?;
    Ok(VectorClusterInfo {
        key_index,
        column_id,
        dimension,
        distance_type,
    })
}

/// Generates cluster statistics and temporary sort-key columns for block writes.
#[derive(Clone, Default)]
pub struct ClusterStatsGenerator {
    cluster_key_id: u32,

    level: i32,
    block_thresholds: BlockThresholds,

    pub extra_key_num: usize,
    /// Physical offsets of evaluated PARTITION BY expressions.
    pub partition_key_index: Vec<usize>,
    /// Physical offsets used for sorting. Hilbert proxy/vector ids may appear here.
    pub cluster_key_index: Vec<usize>,
    /// Physical offsets persisted into ClusterStatistics min/max.
    stats_key_offsets: Vec<usize>,
    /// Direct source columns keyed by their physical offsets; expressions are absent.
    source_column_ids: HashMap<usize, ColumnId>,
    pub eval_operators: Vec<BlockOperator>,
    layout: ClusterStatsLayout,
    pub out_fields: Vec<DataField>,
    pub func_ctx: FunctionContext,
}

impl ClusterStatsGenerator {
    #![allow(clippy::too_many_arguments)]
    pub fn new(
        cluster_key_id: u32,
        cluster_key_index: Vec<usize>,
        stats_key_offsets: Vec<usize>,
        source_column_ids: HashMap<usize, ColumnId>,
        extra_key_num: usize,
        level: i32,
        block_thresholds: BlockThresholds,
        eval_operators: Vec<BlockOperator>,
        layout: ClusterStatsLayout,
        out_fields: Vec<DataField>,
        func_ctx: FunctionContext,
    ) -> Self {
        Self {
            cluster_key_id,
            cluster_key_index,
            partition_key_index: Vec::new(),
            stats_key_offsets,
            source_column_ids,
            extra_key_num,
            level,
            block_thresholds,
            eval_operators,
            layout,
            out_fields,
            func_ctx,
        }
    }

    pub fn sort_descs(&self) -> Vec<SortColumnDescription> {
        self.cluster_key_index
            .iter()
            .map(|offset| SortColumnDescription {
                offset: *offset,
                asc: true,
                nulls_first: false,
            })
            .collect()
    }

    /// Return the vector operator when vector clustering is active.
    pub fn vector_operator(&self) -> Option<&VectorClusterOperator> {
        match &self.layout {
            ClusterStatsLayout::Vector(operator) => Some(operator),
            ClusterStatsLayout::Linear | ClusterStatsLayout::Hilbert => None,
        }
    }

    pub fn is_linear(&self) -> bool {
        matches!(self.layout, ClusterStatsLayout::Linear)
    }

    pub fn is_hilbert(&self) -> bool {
        matches!(self.layout, ClusterStatsLayout::Hilbert)
    }

    /// Physical offsets of the two evaluated Hilbert dimensions.
    pub fn hilbert_dimension_offsets(&self) -> Result<[usize; HILBERT_CLUSTER_DIMENSIONS]> {
        if !self.is_hilbert() {
            return Err(ErrorCode::Internal(
                "cluster stats generator is not configured for Hilbert clustering",
            ));
        }
        self.stats_key_offsets.as_slice().try_into().map_err(|_| {
            ErrorCode::Internal(format!(
                "Hilbert clustering requires exactly {HILBERT_CLUSTER_DIMENSIONS} dimensions"
            ))
        })
    }

    /// Generate cluster statistics for append and remove temporary columns.
    ///
    /// The input block already contains evaluated cluster key columns.
    /// Direct-column bounds are returned for the following column-statistics pass.
    pub fn gen_stats_for_append(&self, data_block: DataBlock) -> Result<ClusterStatsState> {
        let mut state = self.clusters_statistics(data_block, self.level)?;
        state.data_block.pop_columns(self.extra_key_num);
        Ok(state)
    }

    pub fn extract_partition_stats(
        &self,
        data_block: &DataBlock,
    ) -> Result<Option<PartitionStatistics>> {
        if self.partition_key_index.is_empty() {
            return Ok(None);
        }
        if data_block.is_empty() {
            return Err(ErrorCode::Internal(
                "cannot extract partition statistics from an empty block",
            ));
        }

        // Append blocks already contain evaluated key expressions. Mutation paths may
        // serialize source columns directly, so evaluate key expressions on demand.
        let evaluated_block = if self
            .partition_key_index
            .iter()
            .any(|index| *index >= data_block.num_columns())
        {
            Some(
                self.eval_operators
                    .iter()
                    .try_fold(data_block.clone(), |block, operator| {
                        operator.execute(&self.func_ctx, block)
                    })?,
            )
        } else {
            None
        };
        let data_block = evaluated_block.as_ref().unwrap_or(data_block);
        if self
            .partition_key_index
            .iter()
            .any(|index| *index >= data_block.num_columns())
        {
            return Err(ErrorCode::Internal(
                "partition key columns are missing before block serialization",
            ));
        }

        let mut values = Vec::with_capacity(self.partition_key_index.len());
        for index in &self.partition_key_index {
            let entry = data_block.get_by_offset(*index);
            let value = entry
                .index(0)
                .ok_or_else(|| ErrorCode::Internal("partition key is missing from block"))?;
            if (1..data_block.num_rows()).any(|row| entry.index(row).as_ref() != Some(&value)) {
                return Err(ErrorCode::Internal(
                    "serialized block contains more than one partition",
                ));
            }
            values.push(value.to_owned());
        }
        Ok(Some(PartitionStatistics::new(values)))
    }

    /// Recompute cluster statistics and reusable direct-column bounds during mutation.
    pub fn gen_with_origin_stats(
        &self,
        data_block: DataBlock,
        origin_stats: Option<ClusterStatistics>,
    ) -> Result<ClusterStatsState> {
        let Some(origin_stats) = origin_stats else {
            return Ok(ClusterStatsState::without_stats(data_block));
        };
        if origin_stats.cluster_key_id != self.cluster_key_id {
            return Ok(ClusterStatsState::without_stats(data_block));
        }

        let mut stats_block = data_block.clone();

        if self.is_linear() && !self.cluster_key_index.is_empty() {
            let indices = [0u32, stats_block.num_rows() as u32 - 1];
            stats_block = stats_block.take(indices.as_slice())?;
        }

        stats_block = self
            .eval_operators
            .iter()
            .try_fold(stats_block, |input, op| op.execute(&self.func_ctx, input))?;

        let mut state = self.clusters_statistics(stats_block, origin_stats.level)?;
        state.data_block = data_block;
        Ok(state)
    }

    /// Generate persisted cluster ranges from evaluated key columns.
    fn clusters_statistics(&self, data_block: DataBlock, level: i32) -> Result<ClusterStatsState> {
        // A vector-only cluster key excludes the vector column from
        // `stats_key_offsets` (it has no persisted lexicographic bound), but
        // clustering is still active and callers require `cluster_stats` to be
        // present. Only skip stats when clustering is not configured at all.
        if self.stats_key_offsets.is_empty() && self.is_linear() {
            return Ok(ClusterStatsState::without_stats(data_block));
        }
        let is_hilbert = self.is_hilbert();
        let mut min = Vec::with_capacity(self.stats_key_offsets.len());
        let mut max = Vec::with_capacity(self.stats_key_offsets.len());
        let mut column_min_max = HashMap::with_capacity(self.source_column_ids.len());

        if is_hilbert {
            for key in self.hilbert_dimension_offsets()?.iter().copied() {
                let (left, right, can_reuse_max) = aggregate_cluster_key_min_max(&data_block, key)?;
                if let Some(&column_id) = self.source_column_ids.get(&key) {
                    cache_column_min_max(
                        &mut column_min_max,
                        column_id,
                        &left,
                        &right,
                        can_reuse_max,
                    );
                }
                min.push(left);
                max.push(right);
            }
        } else {
            let proxy_sort_key_position = self
                .proxy_sort_key_offset()
                .and_then(|offset| self.cluster_key_index.iter().position(|key| *key == offset))
                .unwrap_or(self.cluster_key_index.len());
            let mut prefix_is_constant = true;
            for (key_index, key) in self.stats_key_offsets.iter().copied().enumerate() {
                let (left, right, can_reuse_max) = if key_index < proxy_sort_key_position {
                    let val = data_block.get_by_offset(key);
                    let left = unsafe { val.index_unchecked(0) }.to_owned();
                    // The stored max must not exceed the last value in the sorted block.
                    let right = unsafe { val.index_unchecked(val.value().len() - 1) }.to_owned();
                    let null_count = block_entry_null_count(val);
                    let can_reuse_max = null_count == 0 || null_count == data_block.num_rows();
                    (left, right, can_reuse_max)
                } else {
                    aggregate_cluster_key_min_max(&data_block, key)?
                };

                // Lexicographic endpoints are independent column bounds only while every
                // preceding key is constant. Keys after a vector proxy key were aggregated.
                if (key_index >= proxy_sort_key_position || prefix_is_constant)
                    && let Some(&column_id) = self.source_column_ids.get(&key)
                {
                    cache_column_min_max(
                        &mut column_min_max,
                        column_id,
                        &left,
                        &right,
                        can_reuse_max,
                    );
                }
                if key_index < proxy_sort_key_position {
                    prefix_is_constant &= left == right;
                }
                min.push(left);
                max.push(right);
            }
        }
        debug_assert!(
            if is_hilbert {
                min.iter()
                    .zip(&max)
                    .all(|(min, max)| min.as_ref().cmp(&max.as_ref()) != Ordering::Greater)
            } else {
                min.iter()
                    .map(Scalar::as_ref)
                    .cmp(max.iter().map(Scalar::as_ref))
                    != Ordering::Greater
            },
            "cluster statistics: min > max"
        );

        let level = if self.is_linear()
            && min == max
            && self.block_thresholds.check_large_enough(
                data_block.num_rows(),
                data_block.estimate_block_size(data_block.num_columns() - self.extra_key_num),
            ) {
            -1
        } else {
            level
        };

        let cluster_stats = ClusterStatistics::new(self.cluster_key_id, min, max, level);
        Ok(ClusterStatsState {
            cluster_stats: Some(cluster_stats),
            data_block,
            column_min_max,
        })
    }

    fn proxy_sort_key_offset(&self) -> Option<usize> {
        self.layout.proxy_sort_key_offset()
    }
}

fn block_entry_null_count(entry: &BlockEntry) -> usize {
    match entry {
        BlockEntry::Const(value, _, len) => usize::from(matches!(value, Scalar::Null)) * len,
        BlockEntry::Column(column) => {
            let (all_null, validity) = column.validity();
            validity.map_or_else(|| usize::from(all_null) * column.len(), |v| v.null_count())
        }
    }
}

fn cache_column_min_max(
    values: &mut HashMap<ColumnId, (Option<Scalar>, Option<Scalar>)>,
    column_id: ColumnId,
    min: &Scalar,
    max: &Scalar,
    can_reuse_max: bool,
) {
    let cached = values.entry(column_id).or_default();
    cached.0.get_or_insert_with(|| min.clone());
    if can_reuse_max {
        cached.1.get_or_insert_with(|| max.clone());
    }
}

pub fn sort_by_cluster_stats(
    v1: Option<&ClusterStatistics>,
    v2: Option<&ClusterStatistics>,
    default_cluster_key: u32,
) -> Ordering {
    match (v1, v2) {
        (Some(a), Some(b)) => {
            if a.cluster_key_id != default_cluster_key && b.cluster_key_id != default_cluster_key {
                return Ordering::Equal;
            }

            let ord_min = a
                .min()
                .iter()
                .map(Scalar::as_ref)
                .cmp(b.min().iter().map(Scalar::as_ref));
            if ord_min != Ordering::Equal {
                return ord_min;
            }
            a.max()
                .iter()
                .map(Scalar::as_ref)
                .cmp(b.max().iter().map(Scalar::as_ref))
        }
        _ => Ordering::Equal,
    }
}

pub(crate) fn partition_values(
    stats: Option<&PartitionStatistics>,
    partition_key_count: usize,
) -> Option<&[Scalar]> {
    stats
        .map(|stats| stats.values.as_slice())
        .filter(|values| values.len() == partition_key_count)
}

pub(crate) fn same_partition(
    left: Option<&PartitionStatistics>,
    right: Option<&PartitionStatistics>,
    partition_key_count: usize,
) -> bool {
    if partition_key_count == 0 {
        return true;
    }
    match (
        partition_values(left, partition_key_count),
        partition_values(right, partition_key_count),
    ) {
        (Some(left), Some(right)) => left == right,
        _ => false,
    }
}

/// Aggregate one cluster-key range, using NULL as the NULLS LAST upper bound when present.
///
/// The boolean indicates whether the returned max is also reusable as column statistics.
/// A mixed-NULL cluster range ends at NULL, while column max must ignore those NULL values.
pub(crate) fn aggregate_cluster_key_min_max(
    data_block: &DataBlock,
    key: usize,
) -> Result<(Scalar, Scalar, bool)> {
    let entry = data_block.get_by_offset(key).clone();
    let row_count = data_block.num_rows();
    let null_count = block_entry_null_count(&entry);
    if null_count == row_count {
        return Ok((Scalar::Null, Scalar::Null, true));
    }

    let entries = [entry];
    let (min, _) = eval_aggr("min", vec![], &entries, row_count, vec![])?;
    let min = min.index(0).unwrap().to_owned();
    if null_count > 0 {
        return Ok((min, Scalar::Null, false));
    }

    let (max, _) = eval_aggr("max", vec![], &entries, row_count, vec![])?;
    Ok((min, max.index(0).unwrap().to_owned(), true))
}

#[derive(Clone, Copy, Default)]
pub(crate) struct BlockOverlapDepth {
    pub(crate) overlap: usize,
    pub(crate) depth: usize,
}

/// Iterative segment tree answering range-max queries over a fixed sequence.
/// `build` is O(n), `range_max` is O(log n) over an inclusive `[l, r]` range.
pub(crate) struct RangeMaxTree {
    size: usize,
    tree: Vec<usize>,
}

impl RangeMaxTree {
    pub(crate) fn build(values: &[usize]) -> Self {
        let size = values.len();
        debug_assert!(size > 0, "RangeMaxTree requires a non-empty input");
        let mut tree = vec![0usize; size * 2];
        tree[size..(size * 2)].copy_from_slice(values);
        for i in (1..size).rev() {
            tree[i] = tree[i * 2].max(tree[i * 2 + 1]);
        }
        Self { size, tree }
    }

    /// Max over the inclusive range `[l, r]`. Caller must ensure `l <= r < size`.
    pub(crate) fn range_max(&self, l: usize, r: usize) -> usize {
        debug_assert!(l <= r && r < self.size, "range [{l}, {r}] out of bounds");
        let mut lo = l + self.size;
        let mut hi = r + self.size + 1;
        let mut acc = 0usize;
        while lo < hi {
            if lo & 1 == 1 {
                acc = acc.max(self.tree[lo]);
                lo += 1;
            }
            if hi & 1 == 1 {
                hi -= 1;
                acc = acc.max(self.tree[hi]);
            }
            lo >>= 1;
            hi >>= 1;
        }
        acc
    }
}

/// Calculate scalar cluster overlap scores from min/max ranges.
pub(crate) fn calculate_block_overlap_depths(
    ranges: &[(Vec<Scalar>, Vec<Scalar>)],
    cluster_key_types: &[DataType],
) -> Result<Vec<BlockOverlapDepth>> {
    if ranges.is_empty() {
        return Ok(Vec::new());
    }

    let mut points_map: HashMap<&[Scalar], (Vec<usize>, Vec<usize>)> = HashMap::new();
    for (index, (min, max)) in ranges.iter().enumerate() {
        points_map
            .entry(min.as_slice())
            .and_modify(|v| v.0.push(index))
            .or_insert((vec![index], vec![]));
        points_map
            .entry(max.as_slice())
            .and_modify(|v| v.1.push(index))
            .or_insert((vec![], vec![index]));
    }

    let (keys, values): (Vec<_>, Vec<_>) = points_map.into_iter().unzip();
    let indices = compare_scalars(&keys, cluster_key_types)?;
    let point_count = indices.len();
    let unset_pos = usize::MAX;
    let mut point_depths = vec![0usize; point_count];
    let mut start_prefix_sums = vec![0usize; point_count];
    let mut open_pos = vec![unset_pos; ranges.len()];
    let mut close_pos = vec![unset_pos; ranges.len()];
    let mut live = vec![false; ranges.len()];
    let mut live_count = 0usize;
    let mut start_count = 0usize;

    for (pos, idx) in indices.into_iter().enumerate() {
        let start = &values[idx as usize].0;
        let end = &values[idx as usize].1;
        let point_depth = live_count + start.len();
        point_depths[pos] = point_depth;
        start_count += start.len();
        start_prefix_sums[pos] = start_count;

        start.iter().for_each(|idx| {
            if !live[*idx] {
                live[*idx] = true;
                live_count += 1;
            }
            open_pos[*idx] = pos;
        });

        end.iter().for_each(|idx| {
            if live[*idx] {
                live[*idx] = false;
                live_count -= 1;
            }
            close_pos[*idx] = pos;
        });
    }

    let range_max_tree = RangeMaxTree::build(&point_depths);
    let mut stats = vec![BlockOverlapDepth::default(); ranges.len()];
    for idx in 0..ranges.len() {
        let open = open_pos[idx];
        let close = close_pos[idx];
        if open == unset_pos || close == unset_pos || close < open {
            continue;
        }

        // Count starts after this block opens and through its close point, matching
        // the old sweep order where closing blocks were removed after start updates.
        let next_overlap = start_prefix_sums[close] - start_prefix_sums[open];
        stats[idx] = BlockOverlapDepth {
            overlap: point_depths[open].saturating_sub(1) + next_overlap,
            depth: range_max_tree.range_max(open, close),
        };
    }

    Ok(stats)
}

#[derive(Clone)]
pub(crate) struct PreparedClusterKeyExpr {
    expr: Expr<usize>,
    data_type: DataType,
    column_refs: Vec<(usize, DataType, Vec<ColumnId>)>,
}

pub(crate) fn prepare_cluster_key_exprs(
    exprs: &[Expr<usize>],
    schema: &TableSchema,
) -> Vec<PreparedClusterKeyExpr> {
    exprs
        .iter()
        .map(|expr| {
            let data_type = expr.data_type().clone();
            PreparedClusterKeyExpr {
                expr: expr.clone(),
                data_type,
                column_refs: expr
                    .column_refs()
                    .into_iter()
                    .map(|(index, ty)| {
                        let column_ids = schema.field(index).leaf_column_ids();
                        (index, ty, column_ids)
                    })
                    .collect(),
            }
        })
        .collect()
}

/// Reconstruct cluster min/max stats from column stats and prepared key domains.
pub(crate) fn get_min_max_stats(
    prepared_exprs: &[PreparedClusterKeyExpr],
    col_stats: &StatisticsOfColumns,
    cluster_stats: Option<&ClusterStatistics>,
    default_key_id: Option<u32>,
) -> (Vec<Scalar>, Vec<Scalar>) {
    if let Some(v) = cluster_stats.filter(|stats| Some(stats.cluster_key_id) == default_key_id) {
        // Cluster stats min/max are guaranteed when generated; reuse them directly.
        return (v.min().clone(), v.max().clone());
    }

    let func_ctx = FunctionContext::default();
    let mut mins = Vec::with_capacity(prepared_exprs.len());
    let mut maxs = Vec::with_capacity(prepared_exprs.len());
    for prepared_expr in prepared_exprs {
        let input_domains = prepared_expr
            .column_refs
            .iter()
            .map(|(index, ty, column_ids)| {
                let stats = column_ids
                    .iter()
                    .filter_map(|column_id| col_stats.get(column_id))
                    .collect();
                let domain = statistics_to_domain(stats, ty);
                (*index, domain)
            })
            .collect();

        let (_, domain_opt) = ConstantFolder::fold_with_domain(
            &prepared_expr.expr,
            &input_domains,
            &func_ctx,
            &BUILTIN_FUNCTIONS,
        );
        let domain = domain_opt.unwrap_or_else(|| Domain::full(&prepared_expr.data_type));
        let (mut min, mut max) = domain.to_minmax();
        if min.as_ref().cmp(&max.as_ref()) == Ordering::Greater {
            warn!("invalid cluster key expression range, fallback to full domain");
            (min, max) = Domain::full(&prepared_expr.data_type).to_minmax();
        }
        mins.push(min);
        maxs.push(max);
    }

    (mins, maxs)
}

/// Rebuild ClusterStatistics from column stats.
///
/// Hilbert tables persist exactly two independent MBR dimensions as
/// `min=[x_min,y_min]`, `max=[x_max,y_max]`.
pub(crate) fn cluster_stats_from_col_stats(
    prepared_exprs: &[PreparedClusterKeyExpr],
    col_stats: &StatisticsOfColumns,
    cluster_key_id: u32,
    level: i32,
) -> ClusterStatistics {
    let (min, max) = get_min_max_stats(prepared_exprs, col_stats, None, None);
    ClusterStatistics::new(cluster_key_id, min, max, level)
}

/// Reuse persisted Hilbert stats when present; otherwise rebuild the marker tuple from column stats.
pub(crate) fn cluster_stats_for_hilbert_depth(
    prepared_exprs: &[PreparedClusterKeyExpr],
    col_stats: &StatisticsOfColumns,
    current_cluster_stats: Option<&ClusterStatistics>,
    cluster_key_id: u32,
) -> ClusterStatistics {
    if let Some(stats) = current_cluster_stats.filter(|stats| {
        stats.cluster_key_id == cluster_key_id
            && stats.min().len() == prepared_exprs.len()
            && stats.max().len() == prepared_exprs.len()
            && valid_cluster_stats_hilbert_minmax(stats, HILBERT_CLUSTER_DIMENSIONS).is_some()
    }) {
        return stats.clone();
    }

    cluster_stats_from_col_stats(
        prepared_exprs,
        col_stats,
        cluster_key_id,
        current_cluster_stats.map_or(0, |stats| stats.level.max(0)),
    )
}

/// Types used by scalar overlap depth calculation.
pub(crate) fn cluster_key_types_for_depth(exprs: &[Expr<usize>]) -> Vec<DataType> {
    exprs
        .iter()
        .map(|expr| {
            let data_type = expr.data_type();
            if matches!(*data_type, DataType::String) {
                data_type.wrap_nullable()
            } else {
                data_type.clone()
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use databend_common_expression::ColumnRef;
    use databend_common_expression::DataBlock;
    use databend_common_expression::FromData;
    use databend_common_expression::RawExpr;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::type_check::check;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::number::NumberScalar;
    use databend_storages_common_table_meta::meta::ColumnStatistics;

    use super::*;
    use crate::statistics::gen_columns_statistics;

    fn int32_scalar(value: i32) -> Scalar {
        Scalar::Number(NumberScalar::Int32(value))
    }

    fn int32_column_expr(index: usize, name: &str) -> Expr<usize> {
        Expr::ColumnRef(ColumnRef {
            span: None,
            id: index,
            data_type: DataType::Number(NumberDataType::Int32),
            display_name: name.to_string(),
        })
    }

    fn int32_schema(names: &[&str]) -> TableSchema {
        TableSchema::new(
            names
                .iter()
                .map(|name| TableField::new(name, TableDataType::Number(NumberDataType::Int32)))
                .collect(),
        )
    }

    fn int32_col_stats(ranges: &[(u32, i32, i32)]) -> StatisticsOfColumns {
        let mut col_stats = StatisticsOfColumns::new();
        for (column_id, min, max) in ranges {
            col_stats.insert(
                *column_id,
                ColumnStatistics::new(int32_scalar(*min), int32_scalar(*max), 0, 0, None),
            );
        }
        col_stats
    }

    fn stats_generator(
        cluster_key_index: Vec<usize>,
        stats_key_offsets: Vec<usize>,
        source_column_ids: HashMap<usize, ColumnId>,
        extra_key_num: usize,
        level: i32,
        layout: ClusterStatsLayout,
    ) -> ClusterStatsGenerator {
        ClusterStatsGenerator::new(
            7,
            cluster_key_index,
            stats_key_offsets,
            source_column_ids,
            extra_key_num,
            level,
            BlockThresholds::new(1_000_000, 125 * 1024 * 1024, 16 * 1024 * 1024, 1000),
            vec![],
            layout,
            vec![],
            FunctionContext::default(),
        )
    }

    #[test]
    fn test_calculate_block_overlap_depths_keeps_boundary_touch_semantics() -> Result<()> {
        let ranges = vec![
            (vec![int32_scalar(1)], vec![int32_scalar(2)]),
            (vec![int32_scalar(2)], vec![int32_scalar(3)]),
            (vec![int32_scalar(4)], vec![int32_scalar(5)]),
        ];
        let cluster_key_types = vec![DataType::Number(NumberDataType::Int32)];

        let stats = calculate_block_overlap_depths(&ranges, &cluster_key_types)?;

        let actual = stats
            .iter()
            .map(|stat| (stat.overlap, stat.depth))
            .collect::<Vec<_>>();
        assert_eq!(actual, vec![(1, 2), (1, 2), (0, 1)]);
        Ok(())
    }

    #[test]
    fn test_get_min_max_stats_expands_multi_column_range() {
        let schema = int32_schema(&["a", "b"]);
        let exprs = vec![int32_column_expr(0, "a"), int32_column_expr(1, "b")];
        let prepared_exprs = prepare_cluster_key_exprs(&exprs, &schema);
        let col_stats = int32_col_stats(&[(0, 1, 3), (1, 2, 5)]);

        let (min, max) = get_min_max_stats(&prepared_exprs, &col_stats, None, Some(0));

        assert_eq!(min, vec![int32_scalar(1), int32_scalar(2)]);
        assert_eq!(max, vec![int32_scalar(3), int32_scalar(5)]);
    }

    #[test]
    fn test_get_min_max_stats_falls_back_on_invalid_expression_range() {
        let schema = int32_schema(&["a"]);
        let exprs = vec![int32_column_expr(0, "a")];
        let prepared_exprs = prepare_cluster_key_exprs(&exprs, &schema);
        let col_stats = int32_col_stats(&[(0, 10, 1)]);

        let (min, max) = get_min_max_stats(&prepared_exprs, &col_stats, None, Some(0));

        assert_eq!(min, vec![int32_scalar(i32::MIN)]);
        assert_eq!(max, vec![int32_scalar(i32::MAX)]);
    }

    #[test]
    fn test_partition_values_returns_none_for_missing_stats() {
        assert_eq!(partition_values(None, 1), None);
    }

    #[test]
    fn test_partition_values_returns_exact_values() {
        let stats = PartitionStatistics::new(vec![int32_scalar(3)]);
        let values = partition_values(Some(&stats), 1).unwrap();
        assert_eq!(values, &[int32_scalar(3)]);
        assert_eq!(partition_values(Some(&stats), 2), None);
    }

    #[test]
    fn test_partition_and_cluster_statistics_are_independent() -> Result<()> {
        let block = DataBlock::new_from_columns(vec![
            Int32Type::from_data(vec![6, 6]),
            Int32Type::from_data(vec![1, 2]),
        ]);
        let partition_expr = check(
            &RawExpr::FunctionCall {
                span: None,
                name: "plus".to_string(),
                params: vec![],
                args: vec![
                    RawExpr::ColumnRef {
                        span: None,
                        id: 0,
                        data_type: DataType::Number(NumberDataType::Int32),
                        display_name: "p".to_string(),
                    },
                    RawExpr::Constant {
                        span: None,
                        scalar: int32_scalar(1),
                        data_type: None,
                    },
                ],
            },
            &BUILTIN_FUNCTIONS,
        )?;
        let mut generator = ClusterStatsGenerator::new(
            3,
            vec![1],
            vec![1],
            HashMap::from([(1, 1)]),
            1,
            0,
            BlockThresholds::default(),
            vec![BlockOperator::Map {
                exprs: vec![partition_expr],
                projections: None,
            }],
            ClusterStatsLayout::Linear,
            vec![],
            FunctionContext::default(),
        );
        generator.partition_key_index = vec![2];

        let partition_stats = generator.extract_partition_stats(&block)?.unwrap();
        let cluster_stats = generator
            .clusters_statistics(block, 0)?
            .cluster_stats
            .unwrap();

        assert_eq!(partition_stats.values, vec![Scalar::Number(
            NumberScalar::Int64(7)
        )]);
        assert_eq!(cluster_stats.min, vec![int32_scalar(1)]);
        assert_eq!(cluster_stats.max, vec![int32_scalar(2)]);
        Ok(())
    }

    #[test]
    fn test_same_partition_returns_false_when_either_side_has_no_stats() {
        // Without partition metadata on either side, compact must not merge the segments.
        let stats = PartitionStatistics::new(vec![int32_scalar(1)]);
        assert!(!same_partition(None, None, 1));
        assert!(!same_partition(Some(&stats), None, 1));
        assert!(!same_partition(None, Some(&stats), 1));
    }

    #[test]
    fn test_same_partition_returns_true_when_partition_key_count_is_zero() {
        // Tables without PARTITION BY always treat segments as the same partition.
        assert!(same_partition(None, None, 0));
    }

    #[test]
    fn test_same_partition_distinguishes_different_partition_values() {
        let left = PartitionStatistics::new(vec![int32_scalar(0)]);
        let right = PartitionStatistics::new(vec![int32_scalar(1)]);
        assert!(!same_partition(Some(&left), Some(&right), 1));
        assert!(same_partition(Some(&left), Some(&left), 1));
    }

    #[test]
    fn test_cluster_stats_from_col_stats_uses_direct_hilbert_mbr() {
        let schema = int32_schema(&["b", "c"]);
        let exprs = vec![int32_column_expr(0, "b"), int32_column_expr(1, "c")];
        let prepared_exprs = prepare_cluster_key_exprs(&exprs, &schema);
        let col_stats = int32_col_stats(&[(0, 1, 3), (1, 2, 4)]);

        let cluster_stats = cluster_stats_from_col_stats(&prepared_exprs, &col_stats, 7, 0);

        assert_eq!(cluster_stats.cluster_key_id, 7);
        assert_eq!(cluster_stats.min(), &[int32_scalar(1), int32_scalar(2)]);
        assert_eq!(cluster_stats.max(), &[int32_scalar(3), int32_scalar(4)]);
    }

    #[test]
    fn test_cluster_stats_for_hilbert_depth_rebuilds_invalid_mbr() {
        let schema = int32_schema(&["b", "c"]);
        let exprs = vec![int32_column_expr(0, "b"), int32_column_expr(1, "c")];
        let prepared_exprs = prepare_cluster_key_exprs(&exprs, &schema);
        let col_stats = int32_col_stats(&[(0, 1, 3), (1, 2, 4)]);
        let invalid = ClusterStatistics::new(
            7,
            vec![int32_scalar(5), int32_scalar(2)],
            vec![int32_scalar(1), int32_scalar(4)],
            6,
        );

        let rebuilt =
            cluster_stats_for_hilbert_depth(&prepared_exprs, &col_stats, Some(&invalid), 7);
        assert_eq!(rebuilt.min(), &[int32_scalar(1), int32_scalar(2)]);
        assert_eq!(rebuilt.max(), &[int32_scalar(3), int32_scalar(4)]);
        assert_eq!(rebuilt.level, 6);

        let wrong_dimension = ClusterStatistics::new(
            7,
            vec![int32_scalar(1), int32_scalar(2), int32_scalar(3)],
            vec![int32_scalar(4), int32_scalar(5), int32_scalar(6)],
            5,
        );
        let rebuilt =
            cluster_stats_for_hilbert_depth(&prepared_exprs, &col_stats, Some(&wrong_dimension), 7);
        assert_eq!(rebuilt.level, 5);
        assert_eq!(rebuilt.min(), &[int32_scalar(1), int32_scalar(2)]);
        assert_eq!(rebuilt.max(), &[int32_scalar(3), int32_scalar(4)]);
    }

    #[test]
    fn test_hilbert_stats_preserve_level_without_temporary_columns() -> Result<()> {
        let stats_gen = stats_generator(
            vec![],
            vec![0, 1],
            HashMap::from([(0, 0), (1, 1)]),
            0,
            0,
            ClusterStatsLayout::Hilbert,
        );

        let block = DataBlock::new_from_columns(vec![
            Int32Type::from_data(vec![1, 2, 3]),
            Int32Type::from_data(vec![3, 2, 1]),
        ]);
        let state = stats_gen.gen_stats_for_append(block.clone())?;
        let cluster_stats = state.cluster_stats.expect("Hilbert stats should exist");
        assert_eq!(cluster_stats.min(), &[int32_scalar(1), int32_scalar(1)]);
        assert_eq!(cluster_stats.max(), &[int32_scalar(3), int32_scalar(3)]);
        assert_eq!(cluster_stats.level, 0);
        assert_eq!(state.data_block.num_columns(), 2);

        let origin_stats = ClusterStatistics::new(
            7,
            vec![int32_scalar(1), int32_scalar(1)],
            vec![int32_scalar(3), int32_scalar(3)],
            5,
        );
        let state = stats_gen.gen_with_origin_stats(block, Some(origin_stats))?;
        let cluster_stats = state
            .cluster_stats
            .expect("Hilbert stats should be preserved");
        assert_eq!(cluster_stats.min(), &[int32_scalar(1), int32_scalar(1)]);
        assert_eq!(cluster_stats.max(), &[int32_scalar(3), int32_scalar(3)]);
        assert_eq!(cluster_stats.level, 5);
        Ok(())
    }

    #[test]
    fn test_linear_min_max_cache_uses_source_ids_and_constant_prefix() -> Result<()> {
        let stats_gen = stats_generator(
            vec![0, 1],
            vec![0, 1],
            HashMap::from([(0, 10), (1, 20)]),
            0,
            0,
            ClusterStatsLayout::Linear,
        );

        let block = DataBlock::new_from_columns(vec![
            Int32Type::from_data(vec![1, 1, 1]),
            Int32Type::from_data(vec![2, 3, 4]),
        ]);
        let state = stats_gen.gen_stats_for_append(block)?;
        assert_eq!(state.column_min_max.len(), 2);
        assert_eq!(
            state.column_min_max[&10],
            (Some(int32_scalar(1)), Some(int32_scalar(1)))
        );
        assert_eq!(
            state.column_min_max[&20],
            (Some(int32_scalar(2)), Some(int32_scalar(4)))
        );

        let block = DataBlock::new_from_columns(vec![
            Int32Type::from_data(vec![1, 2, 3]),
            Int32Type::from_data(vec![10, 0, 5]),
        ]);
        let state = stats_gen.gen_stats_for_append(block)?;
        assert!(state.column_min_max.contains_key(&10));
        assert!(!state.column_min_max.contains_key(&20));

        let block = DataBlock::new_from_columns(vec![
            Int32Type::from_opt_data(vec![Some(1), Some(2), None]),
            Int32Type::from_data(vec![0, 0, 0]),
        ]);
        let origin_stats = ClusterStatistics::new(
            7,
            vec![int32_scalar(1), int32_scalar(0)],
            vec![Scalar::Null, int32_scalar(0)],
            0,
        );
        let state = stats_gen.gen_with_origin_stats(block, Some(origin_stats))?;
        assert_eq!(state.column_min_max[&10], (Some(int32_scalar(1)), None));
        assert!(!state.column_min_max.contains_key(&20));
        Ok(())
    }

    #[test]
    fn test_hilbert_null_bounds_reuse_preserves_column_stats() -> Result<()> {
        let stats_gen = stats_generator(
            vec![],
            vec![0, 1],
            HashMap::from([(0, 0), (1, 1)]),
            0,
            0,
            ClusterStatsLayout::Hilbert,
        );
        let block = DataBlock::new_from_columns(vec![
            Int32Type::from_opt_data(vec![Some(1), None, Some(3)]),
            Int32Type::from_data(vec![3, 2, 1]),
        ]);

        let state = stats_gen.gen_stats_for_append(block)?;
        let cluster_stats = state.cluster_stats.unwrap();
        assert_eq!(cluster_stats.max(), &[Scalar::Null, int32_scalar(3)]);

        assert_eq!(state.column_min_max[&0], (Some(int32_scalar(1)), None));
        assert_eq!(
            state.column_min_max[&1],
            (Some(int32_scalar(1)), Some(int32_scalar(3)))
        );

        let schema = Arc::new(TableSchema::new(vec![
            TableField::new(
                "a",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::Int32))),
            ),
            TableField::new("b", TableDataType::Number(NumberDataType::Int32)),
        ]));
        let column_stats = gen_columns_statistics(
            &state.data_block,
            Some(HashMap::from([(0, 2), (1, 3)])),
            &schema,
            &BTreeMap::new(),
            state.column_min_max,
        )?;
        let first = &column_stats[&0];
        assert_eq!(first.min(), &int32_scalar(1));
        assert_eq!(first.max(), &int32_scalar(3));
        assert_eq!(first.null_count, 1);

        let block = DataBlock::new_from_columns(vec![
            Int32Type::from_opt_data(vec![None::<i32>, None, None]),
            Int32Type::from_data(vec![3, 2, 1]),
        ]);
        let state = stats_gen.gen_stats_for_append(block)?;
        let cluster_stats = state.cluster_stats.unwrap();
        assert_eq!(cluster_stats.min(), &[Scalar::Null, int32_scalar(1)]);
        assert_eq!(cluster_stats.max(), &[Scalar::Null, int32_scalar(3)]);
        assert_eq!(
            state.column_min_max[&0],
            (Some(Scalar::Null), Some(Scalar::Null))
        );
        Ok(())
    }

    #[test]
    fn test_sort_by_cluster_stats_compares_direct_bounds() {
        let left = ClusterStatistics::new(
            7,
            vec![int32_scalar(2), int32_scalar(0)],
            vec![int32_scalar(3), int32_scalar(1)],
            0,
        );
        let right = ClusterStatistics::new(
            7,
            vec![int32_scalar(1), int32_scalar(0)],
            vec![int32_scalar(4), int32_scalar(1)],
            0,
        );

        assert_eq!(
            sort_by_cluster_stats(Some(&left), Some(&right), 7),
            Ordering::Greater
        );
    }
}
