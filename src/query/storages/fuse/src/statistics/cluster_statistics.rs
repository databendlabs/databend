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
use log::warn;

#[derive(Clone, Debug)]
pub struct VectorClusterInfo {
    pub key_index: usize,
    pub column_id: ColumnId,
    pub column_name: String,
    pub dimension: usize,
    pub distance_type: VectorDistanceType,
}

#[derive(Clone, Debug)]
pub struct VectorClusterOperator {
    pub info: VectorClusterInfo,
    pub vector_column_input_offset: usize,
    pub vector_cluster_id_offset: usize,
}

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
        column_name: column_name.to_string(),
        dimension,
        distance_type,
    })
}

#[derive(Clone, Default)]
pub struct ClusterStatsGenerator {
    cluster_key_id: u32,

    level: i32,
    block_thresholds: BlockThresholds,

    pub extra_key_num: usize,
    pub cluster_key_index: Vec<usize>,
    pub operators: Vec<BlockOperator>,
    pub vector_operator: Option<VectorClusterOperator>,
    pub out_fields: Vec<DataField>,
    pub func_ctx: FunctionContext,
}

impl ClusterStatsGenerator {
    #![allow(clippy::too_many_arguments)]
    pub fn new(
        cluster_key_id: u32,
        cluster_key_index: Vec<usize>,
        extra_key_num: usize,
        level: i32,
        block_thresholds: BlockThresholds,
        operators: Vec<BlockOperator>,
        vector_operator: Option<VectorClusterOperator>,
        out_fields: Vec<DataField>,
        func_ctx: FunctionContext,
    ) -> Self {
        Self {
            cluster_key_id,
            cluster_key_index,
            extra_key_num,
            level,
            block_thresholds,
            operators,
            vector_operator,
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

    pub fn operator_extra_key_num(&self) -> usize {
        self.operators
            .iter()
            .map(|op| match op {
                BlockOperator::Map { exprs, .. } => exprs.len(),
                BlockOperator::Project { .. } => 0,
            })
            .sum()
    }

    // This can be used in block append.
    // The input block contains the cluster key block.
    pub fn gen_stats_for_append(
        &self,
        mut data_block: DataBlock,
    ) -> Result<(Option<ClusterStatistics>, DataBlock)> {
        let cluster_stats = self.clusters_statistics(&data_block, self.level)?;
        data_block.pop_columns(self.extra_key_num);
        Ok((cluster_stats, data_block))
    }

    // This can be used in deletion, for an existing block.
    pub fn gen_with_origin_stats(
        &self,
        data_block: &DataBlock,
        origin_stats: Option<ClusterStatistics>,
    ) -> Result<Option<ClusterStatistics>> {
        if origin_stats.is_none() {
            return Ok(None);
        }

        let origin_stats = origin_stats.unwrap();
        if origin_stats.cluster_key_id != self.cluster_key_id {
            return Ok(None);
        }

        let mut block = data_block.clone();

        // For vector cluster keys, scalar keys after the vector key are aggregated from the
        // full block because the block is sorted by the injected vector sort key, not by
        // those scalar suffix keys.
        if self.vector_operator.is_none() && !self.cluster_key_index.is_empty() {
            let indices = vec![0u32, block.num_rows() as u32 - 1];
            block = block.take(indices.as_slice())?;
        }

        block = self
            .operators
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
        if self.cluster_key_index.is_empty() {
            return Ok(None);
        }
        let vector_cluster_id_offset = self.vector_cluster_id_offset();
        let scalar_cluster_key_index = self.scalar_cluster_key_index(vector_cluster_id_offset);
        let mut min = Vec::with_capacity(scalar_cluster_key_index.len());
        let mut max = Vec::with_capacity(scalar_cluster_key_index.len());

        let vector_key_position = vector_cluster_id_offset
            .and_then(|offset| self.cluster_key_index.iter().position(|key| *key == offset))
            .unwrap_or(self.cluster_key_index.len());
        for (key_index, key) in scalar_cluster_key_index.iter().copied() {
            if key_index < vector_key_position {
                let val = data_block.get_by_offset(key);
                let left = unsafe { val.index_unchecked(0) }.to_owned();
                min.push(left);

                // The maximum in cluster statistics neednot larger than the non-trimmed one.
                // So we use trim_min directly.
                let right = unsafe { val.index_unchecked(val.value().len() - 1) }.to_owned();
                max.push(right);
            } else {
                let (left, right) = aggregate_cluster_key_min_max(data_block, key)?;
                min.push(left);
                max.push(right);
            }
        }
        debug_assert!(
            min.iter()
                .map(Scalar::as_ref)
                .cmp(max.iter().map(Scalar::as_ref))
                != Ordering::Greater,
            "cluster statistics: min > max, data may not be sorted by cluster key"
        );

        let level = if self.vector_operator.is_none()
            && min == max
            && self.block_thresholds.check_large_enough(
                data_block.num_rows(),
                data_block.estimate_block_size(data_block.num_columns() - self.extra_key_num),
            ) {
            -1
        } else {
            level
        };

        Ok(Some(ClusterStatistics::new(
            self.cluster_key_id,
            min,
            max,
            level,
        )))
    }

    fn vector_cluster_id_offset(&self) -> Option<usize> {
        self.vector_operator
            .as_ref()
            .map(|vector_operator| vector_operator.vector_cluster_id_offset)
    }

    fn scalar_cluster_key_index(
        &self,
        vector_cluster_id_offset: Option<usize>,
    ) -> Vec<(usize, usize)> {
        self.cluster_key_index
            .iter()
            .copied()
            .enumerate()
            .filter(|(_, key)| Some(*key) != vector_cluster_id_offset)
            .collect()
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

pub fn aggregate_cluster_key_min_max(
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

#[derive(Clone, Copy, Default)]
pub struct BlockOverlapDepth {
    pub overlap: usize,
    pub depth: usize,
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

pub fn calculate_block_overlap_depths(
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

pub(crate) fn get_min_max_stats(
    prepared_exprs: &[PreparedClusterKeyExpr],
    col_stats: &StatisticsOfColumns,
    cluster_stats: Option<&ClusterStatistics>,
    default_key_id: Option<u32>,
) -> (Vec<Scalar>, Vec<Scalar>) {
    if let Some(default_key_id) = default_key_id {
        if let Some(v) = cluster_stats {
            if v.cluster_key_id == default_key_id {
                // Cluster stats min/max are guaranteed when generated; reuse them directly.
                return (v.min().clone(), v.max().clone());
            }
        }
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

#[cfg(test)]
mod tests {
    use databend_common_expression::ColumnRef;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
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
}
