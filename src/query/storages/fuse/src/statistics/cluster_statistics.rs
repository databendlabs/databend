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

use databend_common_exception::Result;
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
use databend_common_sql::evaluator::BlockOperator;
use databend_storages_common_index::statistics_to_domain;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::meta::VectorDistanceType;
use databend_storages_common_table_meta::meta::cluster_stats_has_hilbert_tuple;
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

/// Runtime metadata for Hilbert marker clustering.
#[derive(Clone, Debug)]
pub struct HilbertClusterOperator {
    /// Input offsets of Hilbert dimensions consumed by TransformHilbertCluster.
    pub dimension_offsets: Vec<usize>,
    /// Recluster-only proxy sort key offset; append leaves this as None.
    pub hilbert_proxy_offset: Option<usize>,
}

/// Mutually exclusive non-scalar cluster operator used while generating stats.
#[derive(Clone, Debug)]
pub enum ClusterStatsOperator {
    Vector(VectorClusterOperator),
    Hilbert(HilbertClusterOperator),
}

impl ClusterStatsOperator {
    /// Offset of the transient sort key that should not be persisted in stats.
    fn transient_sort_key_offset(&self) -> Option<usize> {
        match self {
            Self::Vector(operator) => Some(operator.vector_cluster_id_offset),
            Self::Hilbert(operator) => operator.hilbert_proxy_offset,
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

/// Generates cluster statistics and transient sort-key columns for block writes.
#[derive(Clone, Default)]
pub struct ClusterStatsGenerator {
    cluster_key_id: u32,
    max_page_size: Option<usize>,

    level: i32,
    block_thresholds: BlockThresholds,

    pub extra_key_num: usize,
    /// Physical offsets used for sorting. Hilbert proxy/vector ids may appear here.
    pub cluster_key_index: Vec<usize>,
    /// Physical offsets persisted into ClusterStatistics min/max.
    pub stats_key_offsets: Vec<usize>,
    pub eval_operators: Vec<BlockOperator>,
    pub special_operator: Option<ClusterStatsOperator>,
    pub out_fields: Vec<DataField>,
    pub func_ctx: FunctionContext,
}

impl ClusterStatsGenerator {
    #![allow(clippy::too_many_arguments)]
    /// Create a generator for append, mutation, or recluster block statistics.
    pub fn new(
        cluster_key_id: u32,
        cluster_key_index: Vec<usize>,
        stats_key_offsets: Vec<usize>,
        extra_key_num: usize,
        max_page_size: Option<usize>,
        level: i32,
        block_thresholds: BlockThresholds,
        eval_operators: Vec<BlockOperator>,
        special_operator: Option<ClusterStatsOperator>,
        out_fields: Vec<DataField>,
        func_ctx: FunctionContext,
    ) -> Self {
        Self {
            cluster_key_id,
            cluster_key_index,
            stats_key_offsets,
            extra_key_num,
            max_page_size,
            level,
            block_thresholds,
            eval_operators,
            special_operator,
            out_fields,
            func_ctx,
        }
    }

    /// Build physical sort descriptions for the current cluster key order.
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
        match self.special_operator.as_ref()? {
            ClusterStatsOperator::Vector(operator) => Some(operator),
            ClusterStatsOperator::Hilbert(_) => None,
        }
    }

    /// Generate cluster statistics for append and remove transient columns.
    ///
    /// The input block already contains evaluated cluster key columns.
    pub fn gen_stats_for_append(
        &self,
        mut data_block: DataBlock,
    ) -> Result<(Option<ClusterStatistics>, DataBlock)> {
        let cluster_stats = self.clusters_statistics(&data_block, self.level)?;
        data_block.pop_columns(self.extra_key_num);
        Ok((cluster_stats, data_block))
    }

    /// Recompute cluster statistics for an existing block during mutation.
    pub fn gen_with_origin_stats(
        &self,
        data_block: &DataBlock,
        origin_stats: Option<ClusterStatistics>,
    ) -> Result<Option<ClusterStatistics>> {
        let Some(origin_stats) = origin_stats else {
            return Ok(None);
        };
        if origin_stats.cluster_key_id != self.cluster_key_id {
            return Ok(None);
        }

        let mut block = data_block.clone();

        let has_special_stats_layout =
            self.hilbert_dimension_len() > 0 || self.transient_sort_key_offset().is_some();
        if !has_special_stats_layout && !self.cluster_key_index.is_empty() {
            let indices = vec![0u32, block.num_rows() as u32 - 1];
            block = block.take(indices.as_slice())?;
        }

        block = self
            .eval_operators
            .iter()
            .try_fold(block, |input, op| op.execute(&self.func_ctx, input))?;

        self.clusters_statistics(&block, origin_stats.level)
    }

    /// for string value, only use the first 8 bytes.
    fn clusters_statistics(
        &self,
        data_block: &DataBlock,
        level: i32,
    ) -> Result<Option<ClusterStatistics>> {
        if self.stats_key_offsets.is_empty() {
            return Ok(None);
        }
        let transient_sort_key_offset = self.transient_sort_key_offset();
        let hilbert_len = self.hilbert_dimension_len();
        let scalar_len = self.stats_key_offsets.len().saturating_sub(hilbert_len);
        let mut min = Vec::with_capacity(scalar_len + usize::from(hilbert_len > 0));
        let mut max = Vec::with_capacity(scalar_len + usize::from(hilbert_len > 0));

        let transient_sort_key_position = transient_sort_key_offset
            .and_then(|offset| self.cluster_key_index.iter().position(|key| *key == offset))
            .unwrap_or(self.cluster_key_index.len());
        for (key_index, key) in self
            .stats_key_offsets
            .iter()
            .copied()
            .take(scalar_len)
            .enumerate()
        {
            if key_index < transient_sort_key_position {
                let val = data_block.get_by_offset(key);
                let left = unsafe { val.index_unchecked(0) }.to_owned();
                min.push(left);

                // The stored max must not exceed the last value in the sorted block.
                let right = unsafe { val.index_unchecked(val.value().len() - 1) }.to_owned();
                max.push(right);
            } else {
                let (left, right) = aggregate_cluster_key_min_max(data_block, key)?;
                min.push(left);
                max.push(right);
            }
        }
        if hilbert_len > 0 {
            let mut dim_min = Vec::with_capacity(hilbert_len);
            let mut dim_max = Vec::with_capacity(hilbert_len);
            for key in self.stats_key_offsets.iter().copied().skip(scalar_len) {
                let (left, right) = aggregate_cluster_key_min_max(data_block, key)?;
                dim_min.push(left);
                dim_max.push(right);
            }
            min.push(Scalar::Tuple(dim_min));
            max.push(Scalar::Tuple(dim_max));
        }
        debug_assert!(
            min.iter()
                .map(Scalar::as_ref)
                .cmp(max.iter().map(Scalar::as_ref))
                != Ordering::Greater,
            "cluster statistics: min > max, data may not be sorted by cluster key"
        );

        let level = if self.special_operator.is_none()
            && min == max
            && self.block_thresholds.check_large_enough(
                data_block.num_rows(),
                data_block.estimate_block_size(data_block.num_columns() - self.extra_key_num),
            ) {
            -1
        } else {
            level
        };

        let page_key_count = scalar_len;
        let pages = if let Some(max_page_size) = self.max_page_size.filter(|_| page_key_count > 0) {
            let mut values = Vec::with_capacity(data_block.num_rows() / max_page_size + 1);
            for start in (0..data_block.num_rows()).step_by(max_page_size) {
                let mut tuple_values = Vec::with_capacity(page_key_count);
                for key in self.stats_key_offsets.iter().copied().take(page_key_count) {
                    let val = data_block.get_by_offset(key);
                    let left = unsafe { val.index_unchecked(start) };
                    tuple_values.push(left.to_owned());
                }
                values.push(Scalar::Tuple(tuple_values));
            }
            Some(values)
        } else {
            None
        };

        let cluster_stats = ClusterStatistics::new(self.cluster_key_id, min, max, level, pages);
        Ok(Some(cluster_stats))
    }

    fn transient_sort_key_offset(&self) -> Option<usize> {
        self.special_operator
            .as_ref()
            .and_then(ClusterStatsOperator::transient_sort_key_offset)
    }

    fn hilbert_dimension_len(&self) -> usize {
        match &self.special_operator {
            Some(ClusterStatsOperator::Hilbert(operator)) => operator.dimension_offsets.len(),
            Some(ClusterStatsOperator::Vector(_)) | None => 0,
        }
    }
}

/// Compare two optional cluster statistics values in cluster key order.
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

/// Aggregate min and max for one cluster key column from the full block.
pub(crate) fn aggregate_cluster_key_min_max(
    data_block: &DataBlock,
    key: usize,
) -> Result<(Scalar, Scalar)> {
    let entry = data_block.get_by_offset(key).clone();
    let entries = [entry];
    let (min, _) = eval_aggr("min", vec![], &entries, data_block.num_rows(), vec![])?;
    let (max, _) = eval_aggr("max", vec![], &entries, data_block.num_rows(), vec![])?;
    Ok((
        min.index(0).unwrap().to_owned(),
        max.index(0).unwrap().to_owned(),
    ))
}

/// Per-block scalar overlap score used by recluster candidate selection.
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
    /// Build a range-max tree over immutable point depths.
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
        let overlap = point_depths[open].saturating_sub(1) + next_overlap;
        let depth = range_max_tree.range_max(open, close);
        stats[idx] = BlockOverlapDepth { overlap, depth };
    }

    Ok(stats)
}

/// Prepared scalar cluster expression used to reconstruct min/max from col stats.
#[derive(Clone)]
pub(crate) struct PreparedClusterKeyExpr {
    expr: Expr<usize>,
    data_type: DataType,
    column_refs: Vec<(usize, DataType, Vec<ColumnId>)>,
}

/// Prepare cluster key expressions for min/max reconstruction from column stats.
pub(crate) fn prepare_cluster_key_exprs(
    exprs: &[Expr<usize>],
    schema: &TableSchema,
) -> Vec<PreparedClusterKeyExpr> {
    exprs
        .iter()
        .map(|expr| {
            let data_type = expr.data_type().clone();
            let column_refs = if matches!(data_type.remove_nullable(), DataType::Binary) {
                Vec::new()
            } else {
                expr.column_refs()
                    .into_iter()
                    .map(|(index, ty)| {
                        let column_ids = schema.field(index).leaf_column_ids();
                        (index, ty, column_ids)
                    })
                    .collect()
            };

            PreparedClusterKeyExpr {
                expr: expr.clone(),
                data_type,
                column_refs,
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
        // Since the hilbert index does not calc domain, set min max directly.
        if prepared_expr.data_type.remove_nullable() == DataType::Binary {
            mins.push(Scalar::Binary(vec![]));
            maxs.push(Scalar::Binary(vec![0xFF; 40]));
            continue;
        }

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

/// Rebuild ClusterStatistics from column stats; trailing Hilbert dimensions are packed as a tuple.
pub(crate) fn cluster_stats_from_col_stats(
    prepared_exprs: &[PreparedClusterKeyExpr],
    col_stats: &StatisticsOfColumns,
    cluster_key_id: u32,
    level: i32,
    hilbert_len: usize,
) -> ClusterStatistics {
    let (mut min, mut max) = get_min_max_stats(prepared_exprs, col_stats, None, None);
    if hilbert_len > 0 && min.len() == max.len() && hilbert_len <= min.len() {
        let scalar_len = min.len() - hilbert_len;
        let dim_min = min.split_off(scalar_len);
        let dim_max = max.split_off(scalar_len);
        min.push(Scalar::Tuple(dim_min));
        max.push(Scalar::Tuple(dim_max));
    }
    ClusterStatistics::new(cluster_key_id, min, max, level, None)
}

/// Reuse persisted Hilbert stats when present; otherwise rebuild the marker tuple from column stats.
pub(crate) fn cluster_stats_for_hilbert_depth(
    prepared_exprs: &[PreparedClusterKeyExpr],
    col_stats: &StatisticsOfColumns,
    current_cluster_stats: Option<&ClusterStatistics>,
    cluster_key_id: u32,
    hilbert_len: usize,
) -> ClusterStatistics {
    if let Some(stats) =
        current_cluster_stats.filter(|stats| cluster_stats_has_hilbert_tuple(stats))
    {
        return stats.clone();
    }

    cluster_stats_from_col_stats(
        prepared_exprs,
        col_stats,
        cluster_key_id,
        current_cluster_stats.map_or(0, |stats| stats.level.max(0)),
        hilbert_len,
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
    use databend_common_expression::ColumnRef;
    use databend_common_expression::DataBlock;
    use databend_common_expression::FromData;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::types::BinaryType;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::number::NumberScalar;
    use databend_storages_common_table_meta::meta::ColumnStatistics;

    use super::*;

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
    fn test_cluster_stats_from_col_stats_packs_hilbert_dimensions() {
        let schema = int32_schema(&["b", "c"]);
        let exprs = vec![int32_column_expr(0, "b"), int32_column_expr(1, "c")];
        let prepared_exprs = prepare_cluster_key_exprs(&exprs, &schema);
        let col_stats = int32_col_stats(&[(0, 1, 3), (1, 2, 4)]);

        let cluster_stats = cluster_stats_from_col_stats(&prepared_exprs, &col_stats, 7, 0, 2);

        assert_eq!(cluster_stats.cluster_key_id, 7);
        assert_eq!(cluster_stats.min(), &[Scalar::Tuple(vec![
            int32_scalar(1),
            int32_scalar(2)
        ])]);
        assert_eq!(cluster_stats.max(), &[Scalar::Tuple(vec![
            int32_scalar(3),
            int32_scalar(4)
        ])]);
    }

    #[test]
    fn test_hilbert_proxy_sort_key_is_not_persisted_in_cluster_stats() -> Result<()> {
        let block_thresholds =
            BlockThresholds::new(1_000_000, 125 * 1024 * 1024, 16 * 1024 * 1024, 1000);
        let stats_gen = ClusterStatsGenerator::new(
            7,
            vec![2],
            vec![0, 1],
            1,
            None,
            3,
            block_thresholds,
            vec![],
            Some(ClusterStatsOperator::Hilbert(HilbertClusterOperator {
                dimension_offsets: vec![0, 1],
                hilbert_proxy_offset: Some(2),
            })),
            vec![
                DataField::new("a", DataType::Number(NumberDataType::Int32)),
                DataField::new("b", DataType::Number(NumberDataType::Int32)),
                DataField::new("_hilbert_cluster_sort_key", DataType::Binary),
            ],
            FunctionContext::default(),
        );

        let block = DataBlock::new_from_columns(vec![
            Int32Type::from_data(vec![1, 2, 3]),
            Int32Type::from_data(vec![3, 2, 1]),
            BinaryType::from_data(vec![vec![0], vec![1], vec![2]]),
        ]);

        let (cluster_stats, output_block) = stats_gen.gen_stats_for_append(block)?;
        let cluster_stats = cluster_stats.expect("pure hilbert recluster should keep level stats");

        assert_eq!(cluster_stats.min(), &[Scalar::Tuple(vec![
            int32_scalar(1),
            int32_scalar(1)
        ])]);
        assert_eq!(cluster_stats.max(), &[Scalar::Tuple(vec![
            int32_scalar(3),
            int32_scalar(3)
        ])]);
        assert_eq!(cluster_stats.level, 3);
        assert_eq!(output_block.num_columns(), 2);
        Ok(())
    }

    #[test]
    fn test_hilbert_marker_preserves_pure_hilbert_level_without_proxy() -> Result<()> {
        let block_thresholds =
            BlockThresholds::new(1_000_000, 125 * 1024 * 1024, 16 * 1024 * 1024, 1000);
        let stats_gen = ClusterStatsGenerator::new(
            7,
            vec![],
            vec![0, 1],
            0,
            None,
            0,
            block_thresholds,
            vec![],
            Some(ClusterStatsOperator::Hilbert(HilbertClusterOperator {
                dimension_offsets: vec![0, 1],
                hilbert_proxy_offset: None,
            })),
            vec![
                DataField::new("a", DataType::Number(NumberDataType::Int32)),
                DataField::new("b", DataType::Number(NumberDataType::Int32)),
            ],
            FunctionContext::default(),
        );

        let block = DataBlock::new_from_columns(vec![
            Int32Type::from_data(vec![1, 2, 3]),
            Int32Type::from_data(vec![3, 2, 1]),
        ]);
        let (cluster_stats, output_block) = stats_gen.gen_stats_for_append(block.clone())?;
        let cluster_stats = cluster_stats.expect("pure hilbert append should keep level stats");
        assert_eq!(cluster_stats.min(), &[Scalar::Tuple(vec![
            int32_scalar(1),
            int32_scalar(1)
        ])]);
        assert_eq!(cluster_stats.max(), &[Scalar::Tuple(vec![
            int32_scalar(3),
            int32_scalar(3)
        ])]);
        assert_eq!(cluster_stats.level, 0);
        assert_eq!(output_block.num_columns(), 2);

        let origin_stats = ClusterStatistics::new(
            7,
            vec![Scalar::Tuple(vec![int32_scalar(1), int32_scalar(1)])],
            vec![Scalar::Tuple(vec![int32_scalar(3), int32_scalar(3)])],
            5,
            None,
        );
        let cluster_stats = stats_gen
            .gen_with_origin_stats(&block, Some(origin_stats))?
            .expect("pure hilbert mutation should preserve level stats");
        assert_eq!(cluster_stats.min(), &[Scalar::Tuple(vec![
            int32_scalar(1),
            int32_scalar(1)
        ])]);
        assert_eq!(cluster_stats.max(), &[Scalar::Tuple(vec![
            int32_scalar(3),
            int32_scalar(3)
        ])]);
        assert_eq!(cluster_stats.level, 5);
        Ok(())
    }

    #[test]
    fn test_hilbert_marker_preserves_mixed_prefix_level_without_proxy() -> Result<()> {
        let block_thresholds =
            BlockThresholds::new(1_000_000, 125 * 1024 * 1024, 16 * 1024 * 1024, 1000);
        let stats_gen = ClusterStatsGenerator::new(
            7,
            vec![0],
            vec![0, 1, 2],
            0,
            None,
            0,
            block_thresholds,
            vec![],
            Some(ClusterStatsOperator::Hilbert(HilbertClusterOperator {
                dimension_offsets: vec![1, 2],
                hilbert_proxy_offset: None,
            })),
            vec![
                DataField::new("a", DataType::Number(NumberDataType::Int32)),
                DataField::new("b", DataType::Number(NumberDataType::Int32)),
                DataField::new("c", DataType::Number(NumberDataType::Int32)),
            ],
            FunctionContext::default(),
        );

        let block = DataBlock::new_from_columns(vec![
            Int32Type::from_data(vec![1, 1, 1]),
            Int32Type::from_data(vec![3, 2, 1]),
            Int32Type::from_data(vec![1, 2, 3]),
        ]);
        let (cluster_stats, output_block) = stats_gen.gen_stats_for_append(block)?;
        let cluster_stats =
            cluster_stats.expect("mixed hilbert marker should keep scalar prefix stats");

        assert_eq!(cluster_stats.min(), &[
            int32_scalar(1),
            Scalar::Tuple(vec![int32_scalar(1), int32_scalar(1)])
        ]);
        assert_eq!(cluster_stats.max(), &[
            int32_scalar(1),
            Scalar::Tuple(vec![int32_scalar(3), int32_scalar(3)])
        ]);
        assert_eq!(cluster_stats.level, 0);
        assert_eq!(output_block.num_columns(), 3);
        Ok(())
    }

    #[test]
    fn test_sort_by_cluster_stats_compares_hilbert_tuple() {
        let left = ClusterStatistics::new(
            7,
            vec![Scalar::Tuple(vec![int32_scalar(2)])],
            vec![Scalar::Tuple(vec![int32_scalar(3)])],
            0,
            None,
        );
        let right = ClusterStatistics::new(
            7,
            vec![Scalar::Tuple(vec![int32_scalar(1)])],
            vec![Scalar::Tuple(vec![int32_scalar(4)])],
            0,
            None,
        );

        assert_eq!(
            sort_by_cluster_stats(Some(&left), Some(&right), 7),
            Ordering::Greater
        );
    }
}
