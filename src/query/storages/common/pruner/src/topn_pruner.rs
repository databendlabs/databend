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

use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::RemoteExpr;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchemaRef;
use databend_storages_common_table_meta::meta::BlockMeta;

use crate::BlockMetaIndex;

/// TopN pruner.
/// Pruning for order by x limit N.
#[derive(Clone)]
pub struct TopNPruner {
    schema: TableSchemaRef,
    sort: Vec<(RemoteExpr<String>, bool, bool)>,
    limit: usize,
    filter_only_use_index: bool,
}

impl TopNPruner {
    pub fn create(
        schema: TableSchemaRef,
        sort: Vec<(RemoteExpr<String>, bool, bool)>,
        limit: usize,
        filter_only_use_index: bool,
    ) -> Self {
        Self {
            schema,
            sort,
            limit,
            filter_only_use_index,
        }
    }
}

impl TopNPruner {
    pub fn prune(
        &self,
        metas: Vec<(BlockMetaIndex, Arc<BlockMeta>)>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        if self.sort.len() != 1 {
            return Ok(metas);
        }

        if self.limit >= metas.len() && !self.filter_only_use_index {
            return Ok(metas);
        }

        let (sort, asc, nulls_first) = &self.sort[0];
        // Currently, we only support topn on single-column sort.
        // TODO: support monadic + multi expression + order by cluster key sort.
        let column = if let RemoteExpr::ColumnRef { id, .. } = sort {
            id
        } else {
            return Ok(metas);
        };

        let sort_column_id = if let Ok(index) = self.schema.column_id_of(column.as_str()) {
            index
        } else {
            return Ok(metas);
        };

        // String Type min/max is truncated
        if matches!(
            self.schema.field_with_name(column)?.data_type(),
            TableDataType::String
        ) {
            return Ok(metas);
        }

        let mut id_stats = metas
            .iter()
            .map(|(id, meta)| {
                let stat = meta.col_stats.get(&sort_column_id).ok_or_else(|| {
                    ErrorCode::UnknownException(format!(
                        "Unable to get the colStats by ColumnId: {}",
                        sort_column_id
                    ))
                })?;
                Ok((id.clone(), stat.clone(), meta.clone()))
            })
            .collect::<Result<Vec<_>>>()?;

        if self.filter_only_use_index {
            // For descending order, we determine a lower bound for the Nth largest value.
            // Any block with a max_val below this lower bound can be discarded.
            // For ascending order, we determine an upper bound for the Nth smallest value.
            // Any block with a min_val above this upper bound can be discarded.
            // The threshold is estimated based on the number of matching rows in each block
            // and the block's min_val/max_val.
            let mut topn_count = 0;
            let mut pruned_metas = Vec::new();
            if *asc {
                // Sort in ascending order by the min_val so the most promising candidates go first.
                id_stats.sort_by(|a, b| a.1.min().cmp(b.1.min()));

                // Determine the upper_bound for the Nth smallest value. Once topn_count
                // reaches the limit, any block whose min exceeds this bound can be skipped.
                let mut upper_bound = id_stats[0].1.max();
                for (index, stat, meta) in &id_stats {
                    if stat.min() > upper_bound && topn_count >= self.limit {
                        continue;
                    }
                    let matched_count = index_match_count(index);
                    topn_count += matched_count;
                    if stat.max() > upper_bound {
                        upper_bound = stat.max();
                    }
                    pruned_metas.push((index.clone(), meta.clone()));
                }
            } else {
                // Sort in descending order by the max_val so the most promising candidates go first.
                id_stats.sort_by(|a, b| a.1.max().cmp(b.1.max()).reverse());

                // Determine the lower_bound for the Nth largest value. Once topn_count
                // reaches the limit, any block whose min exceeds this bound can be skipped.
                let mut lower_bound = id_stats[0].1.min();
                for (index, stat, meta) in &id_stats {
                    if stat.max() < lower_bound && topn_count >= self.limit {
                        continue;
                    }
                    let matched_count = index_match_count(index);
                    topn_count += matched_count;
                    if stat.min() < lower_bound {
                        lower_bound = stat.min();
                    }
                    pruned_metas.push((index.clone(), meta.clone()));
                }
            }
            Ok(pruned_metas)
        } else {
            id_stats.sort_by(|a, b| {
                if a.1.null_count + b.1.null_count != 0 && *nulls_first {
                    return a.1.null_count.cmp(&b.1.null_count).reverse();
                }
                // no nulls
                if *asc {
                    a.1.min().cmp(b.1.min())
                } else {
                    a.1.max().cmp(b.1.max()).reverse()
                }
            });

            let pruned_metas = id_stats
                .iter()
                .map(|s| (s.0.clone(), s.2.clone()))
                .take(self.limit)
                .collect();
            Ok(pruned_metas)
        }
    }
}

fn index_match_count(index: &BlockMetaIndex) -> usize {
    if let Some(rows) = &index.matched_rows {
        return rows.len();
    }
    0
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use databend_common_expression::types::number::NumberDataType;
    use databend_common_expression::types::DataType;
    use databend_common_expression::ColumnId;
    use databend_common_expression::Scalar;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_storages_common_table_meta::meta::ColumnMeta;
    use databend_storages_common_table_meta::meta::ColumnStatistics;
    use databend_storages_common_table_meta::meta::Compression;

    use super::*;

    #[test]
    fn test_filter_only_use_index_keeps_overlapping_ranges() {
        let schema = Arc::new(TableSchema::new(vec![TableField::new(
            "c",
            TableDataType::Number(NumberDataType::Int64),
        )]));
        let sort_expr = RemoteExpr::ColumnRef {
            span: None,
            id: "c".to_string(),
            data_type: DataType::Number(NumberDataType::Int64),
            display_name: "c".to_string(),
        };
        let column_id = schema.column_id_of("c").unwrap();

        let metas = vec![
            build_block(column_id, 0, 90, 100, 5),
            build_block(column_id, 1, 80, 95, 3),
            build_block(column_id, 2, 10, 20, 10),
            build_block(column_id, 3, 50, 60, 8),
            build_block(column_id, 4, 98, 105, 2),
            build_block(column_id, 5, 15, 25, 7),
            build_block(column_id, 6, 70, 75, 4),
            build_block(column_id, 7, 1, 5, 1),
        ];

        let test_cases = vec![
            // Find 7 rows descending order
            (false, 7, vec![0, 1, 4]),
            // Find 20 rows descending order
            (false, 20, vec![0, 1, 3, 4, 6]),
            // Find 7 rows ascending order
            (true, 7, vec![2, 5, 7]),
            // Find 20 rows ascending order
            (true, 20, vec![2, 3, 5, 7]),
        ];

        for (asc, limit, expected) in test_cases {
            let pruner = TopNPruner::create(
                schema.clone(),
                vec![(sort_expr.clone(), asc, false)],
                limit,
                true,
            );
            let result = pruner.prune(metas.clone()).unwrap();
            let mut kept_blocks: Vec<_> = result.iter().map(|(idx, _)| idx.block_id).collect();
            kept_blocks.sort_unstable();
            assert_eq!(kept_blocks, expected);
        }
    }

    fn build_block(
        column_id: ColumnId,
        block_id: usize,
        min: i64,
        max: i64,
        matched_rows: usize,
    ) -> (BlockMetaIndex, Arc<BlockMeta>) {
        let mut col_stats = HashMap::new();
        col_stats.insert(
            column_id,
            ColumnStatistics::new(Scalar::from(min), Scalar::from(max), 0, 0, None),
        );

        let column_metas: HashMap<ColumnId, ColumnMeta> = HashMap::new();

        let block_meta = BlockMeta::new(
            matched_rows as u64,
            0,
            0,
            col_stats,
            column_metas,
            None,
            ("block".to_string(), 0),
            None,
            0,
            None,
            None,
            None,
            None,
            None,
            Compression::Lz4,
            None,
        );

        let matched = if matched_rows == 0 {
            None
        } else {
            Some((0..matched_rows).map(|row| (row, None)).collect::<Vec<_>>())
        };

        let index = BlockMetaIndex {
            segment_idx: 0,
            block_idx: block_id,
            range: None,
            page_size: if matched_rows == 0 { 1 } else { matched_rows },
            block_id,
            block_location: format!("block_{block_id}"),
            segment_location: "segment".to_string(),
            snapshot_location: None,
            matched_rows: matched,
            vector_scores: None,
            virtual_block_meta: None,
        };

        (index, Arc::new(block_meta))
    }
}
