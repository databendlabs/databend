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
use databend_common_expression::SEARCH_SCORE_COL_NAME;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::types::number::F32;
use databend_storages_common_table_meta::meta::BlockMeta;

use crate::BlockMetaIndex;

/// Inverted-index based TopN / limit pruner.
///
/// Only applies when the filter is fully answered by the inverted index
/// (`PushDownInfo::filter_only_use_index`), so the exact number of matched
/// rows per block (`BlockMetaIndex::matched_rows`) is known before reading:
/// - `LIMIT N`: keep blocks until N matched rows are covered.
/// - `ORDER BY score() LIMIT N`: prune by the exact matched score range.
/// - `ORDER BY col LIMIT N`: prune by column min/max and matched row counts.
///
/// Value-statistics TopN pruning without an index is handled by the runtime
/// TopN filter, whose boundary comes from the rows actually produced.
#[derive(Clone)]
pub struct TopNPruner {
    schema: TableSchemaRef,
    sort: Vec<(RemoteExpr<String>, bool, bool)>,
    limit: usize,
}

impl TopNPruner {
    pub fn create(
        schema: TableSchemaRef,
        sort: Vec<(RemoteExpr<String>, bool, bool)>,
        limit: usize,
    ) -> Self {
        Self {
            schema,
            sort,
            limit,
        }
    }
}

impl TopNPruner {
    pub fn prune(
        &self,
        metas: Vec<(BlockMetaIndex, Arc<BlockMeta>)>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        if !self.sort.is_empty() {
            self.prune_topn(metas)
        } else {
            self.prune_limit(metas)
        }
    }

    fn prune_limit(
        &self,
        metas: Vec<(BlockMetaIndex, Arc<BlockMeta>)>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        let mut limit_count = 0;
        let mut pruned_metas = Vec::new();
        for (index, meta) in metas.into_iter() {
            let matched_count = index_match_count(&index);
            if matched_count == 0 {
                continue;
            }
            pruned_metas.push((index, meta));
            limit_count += matched_count;
            if limit_count >= self.limit {
                break;
            }
        }
        Ok(pruned_metas)
    }

    fn prune_topn(
        &self,
        metas: Vec<(BlockMetaIndex, Arc<BlockMeta>)>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        if self.sort.len() != 1 || metas.is_empty() {
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
        if *nulls_first {
            return Ok(metas);
        }

        // order by search score
        if column == SEARCH_SCORE_COL_NAME {
            return self.prune_topn_by_score(*asc, metas);
        }

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
            let mut upper_bound = id_stats[0].1.max().clone();
            for (index, stat, _) in &id_stats {
                if *stat.min() > upper_bound && topn_count >= self.limit {
                    continue;
                }
                let matched_count = index_match_count(index);
                if matched_count == 0 {
                    continue;
                }
                topn_count += matched_count;
                if *stat.max() > upper_bound {
                    upper_bound = stat.max().clone();
                }
            }
            for (index, stat, meta) in id_stats.into_iter() {
                if *stat.min() <= upper_bound {
                    pruned_metas.push((index, meta));
                }
            }
        } else {
            // Sort in descending order by the max_val so the most promising candidates go first.
            id_stats.sort_by(|a, b| a.1.max().cmp(b.1.max()).reverse());

            // Determine the lower_bound for the Nth largest value. Once topn_count
            // reaches the limit, any block whose min exceeds this bound can be skipped.
            let mut lower_bound = id_stats[0].1.min().clone();
            for (index, stat, _) in &id_stats {
                if *stat.max() < lower_bound && topn_count >= self.limit {
                    continue;
                }
                let matched_count = index_match_count(index);
                if matched_count == 0 {
                    continue;
                }
                topn_count += matched_count;
                if *stat.min() < lower_bound {
                    lower_bound = stat.min().clone();
                }
            }
            for (index, stat, meta) in id_stats.into_iter() {
                if *stat.max() >= lower_bound {
                    pruned_metas.push((index, meta));
                }
            }
        }
        Ok(pruned_metas)
    }

    fn prune_topn_by_score(
        &self,
        asc: bool,
        metas: Vec<(BlockMetaIndex, Arc<BlockMeta>)>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        if metas.is_empty() {
            return Ok(metas);
        }

        let mut score_stats = Vec::new();
        for (pos, (index, _)) in metas.iter().enumerate() {
            let Some(scores) = &index.matched_scores else {
                return Ok(metas);
            };
            let Some((min_score, max_score)) = block_score_range(scores) else {
                return Ok(metas);
            };
            score_stats.push((pos, min_score, max_score, scores.len()));
        }

        if score_stats.is_empty() {
            return Ok(metas);
        }

        let mut pruned_metas = Vec::new();
        if asc {
            score_stats.sort_by_key(|a| a.1);
            let mut topn_count = 0usize;
            let mut upper_bound = score_stats[0].2;
            for (_, min_score, max_score, matched_count) in &score_stats {
                if *min_score > upper_bound && topn_count >= self.limit {
                    continue;
                }
                topn_count += *matched_count;
                if *max_score > upper_bound {
                    upper_bound = *max_score;
                }
            }
            for (pos, min_score, _, _) in score_stats {
                if min_score <= upper_bound {
                    pruned_metas.push(metas[pos].clone());
                }
            }
        } else {
            score_stats.sort_by_key(|a| std::cmp::Reverse(a.2));
            let mut topn_count = 0usize;
            let mut lower_bound = score_stats[0].1;
            for (_, min_score, max_score, matched_count) in &score_stats {
                if *max_score < lower_bound && topn_count >= self.limit {
                    continue;
                }
                topn_count += *matched_count;
                if *min_score < lower_bound {
                    lower_bound = *min_score;
                }
            }
            for (pos, _, max_score, _) in score_stats {
                if max_score >= lower_bound {
                    pruned_metas.push(metas[pos].clone());
                }
            }
        }
        Ok(pruned_metas)
    }
}

fn index_match_count(index: &BlockMetaIndex) -> usize {
    if let Some(rows) = &index.matched_rows {
        return rows.len();
    }
    0
}

fn block_score_range(scores: &[F32]) -> Option<(F32, F32)> {
    if scores.is_empty() {
        return None;
    }
    // Scores are arranged in descending order,
    // so we can directly get the maximum and minimum score.
    let max_score = scores[0];
    let min_score = scores[scores.len() - 1];
    Some((min_score, max_score))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use databend_common_expression::ColumnId;
    use databend_common_expression::SEARCH_SCORE_COL_NAME;
    use databend_common_expression::Scalar;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::number::NumberDataType;
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
            let pruner =
                TopNPruner::create(schema.clone(), vec![(sort_expr.clone(), asc, false)], limit);
            let result = pruner.prune(metas.clone()).unwrap();
            let mut kept_blocks: Vec<_> = result.iter().map(|(idx, _)| idx.block_id).collect();
            kept_blocks.sort_unstable();
            assert_eq!(kept_blocks, expected);

            // test empty metas
            let result = pruner.prune(vec![]).unwrap();
            assert_eq!(result.len(), 0);
        }
    }

    #[test]
    fn test_prune_topn_by_search_score_desc() {
        let schema = Arc::new(TableSchema::new(vec![TableField::new(
            "c",
            TableDataType::Number(NumberDataType::Int64),
        )]));
        let sort_expr = RemoteExpr::ColumnRef {
            span: None,
            id: SEARCH_SCORE_COL_NAME.to_string(),
            data_type: DataType::Number(NumberDataType::Float32),
            display_name: SEARCH_SCORE_COL_NAME.to_string(),
        };
        let pruner = TopNPruner::create(schema.clone(), vec![(sort_expr, false, false)], 2);
        let column_id = schema.column_id_of("c").unwrap();

        let metas = vec![
            build_block_with_scores(column_id, 0, 0, 0, &[0.9, 0.8]),
            build_block_with_scores(column_id, 1, 0, 0, &[0.85, 0.75]),
            build_block_with_scores(column_id, 2, 0, 0, &[0.3, 0.2]),
        ];

        let result = pruner.prune(metas).unwrap();
        let kept_blocks: Vec<_> = result.iter().map(|(idx, _)| idx.block_id).collect();
        assert_eq!(kept_blocks, vec![0, 1]);
    }

    #[test]
    fn test_prune_limit_by_matched_rows() {
        let schema = Arc::new(TableSchema::new(vec![TableField::new(
            "c",
            TableDataType::Number(NumberDataType::Int64),
        )]));
        let column_id = schema.column_id_of("c").unwrap();
        let metas = vec![
            build_block(column_id, 0, 0, 0, 2),
            build_block(column_id, 1, 0, 0, 0),
            build_block(column_id, 2, 0, 0, 3),
            build_block(column_id, 3, 0, 0, 4),
        ];

        // No ORDER BY: keep blocks with matched rows until the limit is covered.
        let pruner = TopNPruner::create(schema.clone(), vec![], 4);
        let result = pruner.prune(metas).unwrap();
        let kept_blocks: Vec<_> = result.iter().map(|(idx, _)| idx.block_id).collect();
        assert_eq!(kept_blocks, vec![0, 2]);
    }

    fn build_block(
        column_id: ColumnId,
        block_id: usize,
        min: i64,
        max: i64,
        matched_rows: usize,
    ) -> (BlockMetaIndex, Arc<BlockMeta>) {
        let column_stats = ColumnStatistics::new(Scalar::from(min), Scalar::from(max), 0, 0, None);
        build_block_with_stats(column_id, block_id, column_stats, matched_rows)
    }

    fn build_block_with_stats(
        column_id: ColumnId,
        block_id: usize,
        column_stats: ColumnStatistics,
        matched_rows: usize,
    ) -> (BlockMetaIndex, Arc<BlockMeta>) {
        let mut col_stats = HashMap::new();
        col_stats.insert(column_id, column_stats);

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
            None,
            None,
            None,
            Compression::Lz4,
            None,
        );

        let matched = if matched_rows == 0 {
            None
        } else {
            Some((0..matched_rows).collect::<Vec<_>>())
        };

        let index = BlockMetaIndex {
            segment_idx: 0,
            block_idx: block_id,
            range: None,
            block_id,
            block_location: format!("block_{block_id}"),
            segment_location: "segment".to_string(),
            snapshot_location: None,
            matched_rows: matched,
            matched_scores: None,
            vector_scores: None,
            virtual_block_meta: None,
        };

        (index, Arc::new(block_meta))
    }

    fn build_block_with_scores(
        column_id: ColumnId,
        block_id: usize,
        min: i64,
        max: i64,
        scores: &[f32],
    ) -> (BlockMetaIndex, Arc<BlockMeta>) {
        let (mut index, meta) = build_block(column_id, block_id, min, max, scores.len());
        let matched_scores = scores.iter().map(|v| (*v).into()).collect();
        index.matched_scores = Some(matched_scores);
        (index, meta)
    }
}
