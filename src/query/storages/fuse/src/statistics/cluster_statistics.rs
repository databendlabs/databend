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

use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::Domain;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchema;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::evaluator::BlockOperator;
use databend_storages_common_index::statistics_to_domain;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use log::warn;

#[derive(Clone, Default)]
pub struct ClusterStatsGenerator {
    cluster_key_id: u32,
    extra_key_num: usize,
    max_page_size: Option<usize>,

    level: i32,
    block_thresholds: BlockThresholds,

    pub cluster_key_index: Vec<usize>,
    pub operators: Vec<BlockOperator>,
    pub out_fields: Vec<DataField>,
    pub func_ctx: FunctionContext,
}

impl ClusterStatsGenerator {
    #![allow(clippy::too_many_arguments)]
    pub fn new(
        cluster_key_id: u32,
        cluster_key_index: Vec<usize>,
        extra_key_num: usize,
        max_page_size: Option<usize>,
        level: i32,
        block_thresholds: BlockThresholds,
        operators: Vec<BlockOperator>,
        out_fields: Vec<DataField>,
        func_ctx: FunctionContext,
    ) -> Self {
        Self {
            cluster_key_id,
            cluster_key_index,
            extra_key_num,
            max_page_size,
            level,
            block_thresholds,
            operators,
            out_fields,
            func_ctx,
        }
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

        if !self.cluster_key_index.is_empty() {
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
        let mut min = Vec::with_capacity(self.cluster_key_index.len());
        let mut max = Vec::with_capacity(self.cluster_key_index.len());

        for key in self.cluster_key_index.iter() {
            let val = data_block.get_by_offset(*key);
            let left = unsafe { val.index_unchecked(0) }.to_owned();
            min.push(left);

            // The maximum in cluster statistics neednot larger than the non-trimmed one.
            // So we use trim_min directly.
            let right = unsafe { val.index_unchecked(val.value().len() - 1) }.to_owned();
            max.push(right);
        }

        debug_assert!(
            min.iter()
                .map(Scalar::as_ref)
                .cmp(max.iter().map(Scalar::as_ref))
                != Ordering::Greater,
            "cluster statistics: min > max, data may not be sorted by cluster key"
        );

        let level = if min == max
            && self
                .block_thresholds
                .check_large_enough(data_block.num_rows(), data_block.memory_size())
        {
            -1
        } else {
            level
        };

        let pages = if let Some(max_page_size) = self.max_page_size {
            let mut values = Vec::with_capacity(data_block.num_rows() / max_page_size + 1);
            for start in (0..data_block.num_rows()).step_by(max_page_size) {
                let mut tuple_values = Vec::with_capacity(self.cluster_key_index.len());
                for key in self.cluster_key_index.iter() {
                    let val = data_block.get_by_offset(*key);
                    let left = unsafe { val.index_unchecked(start) };
                    tuple_values.push(left.to_owned());
                }
                values.push(Scalar::Tuple(tuple_values));
            }
            Some(values)
        } else {
            None
        };

        Ok(Some(ClusterStatistics::new(
            self.cluster_key_id,
            min,
            max,
            level,
            pages,
        )))
    }
}

pub fn sort_by_cluster_stats(
    v1: &Option<ClusterStatistics>,
    v2: &Option<ClusterStatistics>,
    default_cluster_key: u32,
) -> Ordering {
    match (v1.as_ref(), v2.as_ref()) {
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

pub fn get_min_max_stats(
    exprs: &[Expr<usize>],
    col_stats: &StatisticsOfColumns,
    cluster_stats: Option<&ClusterStatistics>,
    default_key_id: Option<u32>,
    schema: &TableSchema,
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
    let mut mins = Vec::with_capacity(exprs.len());
    let mut maxs = Vec::with_capacity(exprs.len());
    for expr in exprs {
        // Since the hilbert index does not calc domain, set min max directly.
        if expr.data_type().remove_nullable() == DataType::Binary {
            mins.push(Scalar::Binary(vec![]));
            maxs.push(Scalar::Binary(vec![0xFF; 40]));
            continue;
        }

        let input_domains = expr
            .column_refs()
            .into_iter()
            .map(|(index, ty)| {
                let column_ids = schema.field(index).leaf_column_ids();
                let stats = column_ids
                    .iter()
                    .filter_map(|column_id| col_stats.get(column_id))
                    .collect();
                let domain = statistics_to_domain(stats, &ty);
                (index, domain)
            })
            .collect();

        let (_, domain_opt) =
            ConstantFolder::fold_with_domain(expr, &input_domains, &func_ctx, &BUILTIN_FUNCTIONS);
        let domain = domain_opt.unwrap_or_else(|| Domain::full(expr.data_type()));
        let (mut min, mut max) = domain.to_minmax();
        if min.as_ref().cmp(&max.as_ref()) == Ordering::Greater {
            warn!("invalid cluster key expression range, fallback to full domain");
            (min, max) = Domain::full(expr.data_type()).to_minmax();
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
    fn test_get_min_max_stats_expands_multi_column_range() {
        let schema = int32_schema(&["a", "b"]);
        let exprs = vec![int32_column_expr(0, "a"), int32_column_expr(1, "b")];
        let col_stats = int32_col_stats(&[(0, 1, 3), (1, 2, 5)]);

        let (min, max) = get_min_max_stats(&exprs, &col_stats, None, Some(0), &schema);

        assert_eq!(min, vec![int32_scalar(1), int32_scalar(2)]);
        assert_eq!(max, vec![int32_scalar(3), int32_scalar(5)]);
    }

    #[test]
    fn test_get_min_max_stats_falls_back_on_invalid_expression_range() {
        let schema = int32_schema(&["a"]);
        let exprs = vec![int32_column_expr(0, "a")];
        let col_stats = int32_col_stats(&[(0, 10, 1)]);

        let (min, max) = get_min_max_stats(&exprs, &col_stats, None, Some(0), &schema);

        assert_eq!(min, vec![int32_scalar(i32::MIN)]);
        assert_eq!(max, vec![int32_scalar(i32::MAX)]);
    }
}
