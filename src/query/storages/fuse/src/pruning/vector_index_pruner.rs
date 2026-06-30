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

// Logs from this module will show up as "[FUSE-PRUNER] ...".
databend_common_tracing::register_module_tag!("[FUSE-PRUNER]");

use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use databend_common_catalog::plan::Filters;
use databend_common_catalog::plan::VectorIndexInfo;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::RemoteExpr;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::VECTOR_SCORE_COL_NAME;
use databend_common_expression::types::Buffer;
use databend_common_expression::types::F32;
use databend_common_expression::types::NumberColumn;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_metrics::storage::metrics_inc_block_vector_index_pruning_milliseconds;
use databend_common_metrics::storage::metrics_inc_blocks_vector_index_pruning_after;
use databend_common_metrics::storage::metrics_inc_blocks_vector_index_pruning_before;
use databend_common_metrics::storage::metrics_inc_bytes_block_vector_index_pruning_after;
use databend_common_metrics::storage::metrics_inc_bytes_block_vector_index_pruning_before;
use databend_storages_common_index::DistanceType;
use databend_storages_common_index::FixedLengthPriorityQueue;
use databend_storages_common_index::ScoredPointOffset;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_pruner::BlockMetaIndex;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::VectorDistanceType;
use futures_util::future;
use log::info;
use tokio::sync::OwnedSemaphorePermit;

use crate::io::read::VectorIndexReader;
use crate::pruning::PruningContext;
use crate::pruning::PruningCostKind;

type VectorPruningFutureReturn = Pin<Box<dyn Future<Output = Result<VectorPruneResult>> + Send>>;
type VectorPruningFuture =
    Box<dyn FnOnce(OwnedSemaphorePermit) -> VectorPruningFutureReturn + Send + 'static>;

#[derive(Clone)]
struct VectorTopNParam {
    has_filter: bool,
    filter_expr: Option<Expr>,
    asc: bool,
    limit: usize,
}

/// Vector index pruner.
#[derive(Clone)]
pub struct VectorIndexPruner {
    func_ctx: FunctionContext,
    pruning_ctx: Arc<PruningContext>,
    _schema: TableSchemaRef,
    vector_index: VectorIndexInfo,
    vector_reader: VectorIndexReader,
    query_values: Vec<f32>,
    vector_distance_type: VectorDistanceType,
    vector_topn_param: Option<VectorTopNParam>,
}

impl VectorIndexPruner {
    pub fn create(
        pruning_ctx: Arc<PruningContext>,
        schema: TableSchemaRef,
        vector_index: VectorIndexInfo,
        filters: Option<Filters>,
        sort: Vec<(RemoteExpr<String>, bool, bool)>,
        limit: Option<usize>,
    ) -> Result<Self> {
        let func_ctx = pruning_ctx.ctx.get_function_context()?;

        let settings = ReadSettings::from_ctx(&pruning_ctx.ctx)?;
        let distance_type = match vector_index.func_name.as_str() {
            "cosine_distance" => DistanceType::Dot,
            "l1_distance" => DistanceType::L1,
            "l2_distance" => DistanceType::L2,
            _ => unreachable!(),
        };
        let columns = vec![
            format!("{}-{}_graph_links", vector_index.column_id, distance_type),
            format!("{}-{}_graph_data", vector_index.column_id, distance_type),
            format!(
                "{}-{}_encoded_u8_meta",
                vector_index.column_id, distance_type
            ),
            format!(
                "{}-{}_encoded_u8_data",
                vector_index.column_id, distance_type
            ),
        ];

        let query_values =
            unsafe { std::mem::transmute::<Vec<F32>, Vec<f32>>(vector_index.query_values.clone()) };
        let vector_distance_type = distance_type.vector_distance_type();

        let vector_reader = VectorIndexReader::create(
            pruning_ctx.dal.clone(),
            settings,
            distance_type,
            columns,
            query_values.clone(),
        );

        // If the filter only has the vector score column, we can filter the scores.
        let filter_expr = if let Some(filters) = &filters {
            let filter = filters.filter.as_expr(&BUILTIN_FUNCTIONS);
            let column_refs = filter.column_refs();
            if column_refs.len() == 1 && column_refs.contains_key(VECTOR_SCORE_COL_NAME) {
                let filter_expr = filter.project_column_ref(|_| Ok(0))?;
                Some(filter_expr)
            } else {
                None
            }
        } else {
            None
        };

        let mut vector_topn_param = None;
        // If the first sort expr is the vector score column and has the limit value,
        // we can do vector TopN prune to filter out the blocks.
        if !sort.is_empty() && limit.is_some() {
            let (sort_expr, asc, _nulls_first) = &sort[0];
            if let RemoteExpr::ColumnRef { id, .. } = sort_expr {
                if id == VECTOR_SCORE_COL_NAME {
                    let limit = limit.unwrap();
                    vector_topn_param = Some(VectorTopNParam {
                        has_filter: filters.is_some(),
                        filter_expr,
                        asc: *asc,
                        limit,
                    });
                }
            }
        }

        Ok(Self {
            func_ctx,
            pruning_ctx,
            _schema: schema,
            vector_index,
            vector_reader,
            query_values,
            vector_distance_type,
            vector_topn_param,
        })
    }
}

impl VectorIndexPruner {
    pub async fn prune(
        &self,
        metas: Vec<(BlockMetaIndex, Arc<BlockMeta>)>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        if let Some(param) = &self.vector_topn_param {
            let start = Instant::now();
            // Perf.
            {
                let block_size = metas.iter().map(|(_, m)| m.block_size).sum();
                metrics_inc_blocks_vector_index_pruning_before(metas.len() as u64);
                metrics_inc_bytes_block_vector_index_pruning_before(block_size);
                self.pruning_ctx
                    .pruning_stats
                    .set_blocks_vector_index_pruning_before(metas.len() as u64);
            }
            // If there are no filter conditions and sort is in ascending order,
            // we can use the HNSW index to get the results.
            // Otherwise, we need to calculate all the scores and then filter them
            // by conditions or sort them in descending order to get the results.
            let pruned_metas = if !param.has_filter && param.asc {
                self.pruning_ctx
                    .pruning_cost
                    .measure_async(
                        PruningCostKind::BlocksVector,
                        self.vector_index_hnsw_topn_prune(param.limit, metas),
                    )
                    .await?
            } else {
                self.pruning_ctx
                    .pruning_cost
                    .measure_async(
                        PruningCostKind::BlocksVector,
                        self.vector_index_topn_prune(
                            param.filter_expr.as_ref(),
                            param.asc,
                            param.limit,
                            metas,
                        ),
                    )
                    .await?
            };

            let elapsed = start.elapsed().as_millis() as u64;
            // Perf.
            {
                let block_size = pruned_metas.iter().map(|(_, m)| m.block_size).sum();
                metrics_inc_blocks_vector_index_pruning_after(pruned_metas.len() as u64);
                metrics_inc_bytes_block_vector_index_pruning_after(block_size);
                self.pruning_ctx
                    .pruning_stats
                    .set_blocks_vector_index_pruning_after(pruned_metas.len() as u64);
                metrics_inc_block_vector_index_pruning_milliseconds(elapsed);
            }
            if !param.has_filter && param.asc {
                info!("Vector index hnsw topn prune elapsed: {elapsed}");
            } else {
                info!("Vector index calculate score topn prune elapsed: {elapsed}");
            }

            return Ok(pruned_metas);
        }

        // Unable to do prune, fallback to only calculating the score
        self.vector_index_scores(metas).await
    }

    async fn vector_index_hnsw_topn_prune(
        &self,
        limit: usize,
        metas: Vec<(BlockMetaIndex, Arc<BlockMeta>)>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        let candidates = self.vector_prune_candidates(metas)?;
        let (candidates, stat_skipped_blocks) =
            self.filter_candidates_by_topn_stats(candidates, limit);
        if stat_skipped_blocks > 0 {
            info!("Vector stat pruned {stat_skipped_blocks} blocks before hnsw topn");
        }

        let keep_metas = candidates
            .into_iter()
            .map(|candidate| (candidate.block_meta_index, candidate.block_meta))
            .collect();

        let results = self
            .process_vector_pruning_tasks(
                keep_metas,
                move |vector_reader, row_count, location| async move {
                    vector_reader.prune(limit, row_count, &location).await
                },
            )
            .await?;

        let mut top_queue = FixedLengthPriorityQueue::new(limit);
        let len = results.len();
        let mut vector_prune_result_map = HashMap::with_capacity(len);
        for vector_prune_result in results {
            for vector_score in &vector_prune_result.scores {
                top_queue.push(vector_score.clone());
            }
            vector_prune_result_map.insert(vector_prune_result.block_idx, vector_prune_result);
        }

        let top_scores = top_queue.into_sorted_vec();
        let top_indexes: HashSet<usize> = top_scores.iter().map(|s| s.index).collect();

        let mut pruned_metas = Vec::with_capacity(top_indexes.len());
        for index in 0..len {
            if !top_indexes.contains(&index) {
                continue;
            }
            let vector_prune_result = vector_prune_result_map.remove(&index).unwrap();

            let mut vector_scores = Vec::new();
            for top_score in &top_scores {
                if top_score.index == index {
                    vector_scores.push((top_score.row_idx as usize, top_score.score));
                }
            }
            let mut block_meta_index = vector_prune_result.block_meta_index;
            block_meta_index.vector_scores = Some(vector_scores);

            pruned_metas.push((block_meta_index, vector_prune_result.block_meta));
        }

        Ok(pruned_metas)
    }

    async fn vector_index_topn_prune(
        &self,
        filter_expr: Option<&Expr>,
        asc: bool,
        limit: usize,
        metas: Vec<(BlockMetaIndex, Arc<BlockMeta>)>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        if metas.is_empty() {
            return Ok(vec![]);
        }

        let results = self
            .process_vector_pruning_tasks(
                metas,
                move |vector_reader, row_count, location| async move {
                    vector_reader.generate_scores(row_count, &location).await
                },
            )
            .await?;

        let mut top_queue = FixedLengthPriorityQueue::new(limit);
        let len = results.len();
        let mut vector_prune_result_map = HashMap::with_capacity(len);
        for vector_prune_result in results {
            if let Some(filter_expr) = filter_expr {
                // If has filter expr, use scores to build a block and do filtering.
                let num_rows = vector_prune_result.block_meta.row_count as usize;
                let mut builder = Vec::with_capacity(num_rows);
                for score in &vector_prune_result.scores {
                    builder.push(F32::from(score.score));
                }
                let column = Column::Number(NumberColumn::Float32(Buffer::from(builder)));
                let block = DataBlock::new(vec![BlockEntry::from(column)], num_rows);
                let evaluator = Evaluator::new(&block, &self.func_ctx, &BUILTIN_FUNCTIONS);
                let res = evaluator.run(filter_expr)?;
                let res_column = res.into_full_column(filter_expr.data_type(), num_rows);
                let res_column = res_column.remove_nullable();
                let bitmap = res_column.as_boolean().unwrap();
                // All the scores do not meet the conditions, ignore this block.
                if bitmap.null_count() == num_rows {
                    continue;
                }

                if !asc {
                    for (i, vector_score) in vector_prune_result.scores.iter().enumerate() {
                        if bitmap.get_bit(i) {
                            // If asc is false, we want to keep the largest scores,
                            // modify the score to reverse the ordering
                            let modified_score = vector_score.negative_score();
                            top_queue.push(modified_score);
                        }
                    }
                } else {
                    for (i, vector_score) in vector_prune_result.scores.iter().enumerate() {
                        if bitmap.get_bit(i) {
                            top_queue.push(vector_score.clone());
                        }
                    }
                }
            } else if !asc {
                for vector_score in vector_prune_result.scores.iter() {
                    let modified_score = vector_score.negative_score();
                    top_queue.push(modified_score);
                }
            } else {
                for vector_score in vector_prune_result.scores.iter() {
                    top_queue.push(vector_score.clone());
                }
            }

            vector_prune_result_map.insert(vector_prune_result.block_idx, vector_prune_result);
        }

        let top_scores = top_queue.into_sorted_vec();
        let top_indexes: HashSet<usize> = top_scores.iter().map(|s| s.index).collect();

        let mut pruned_metas = Vec::with_capacity(top_indexes.len());
        for index in 0..len {
            if !top_indexes.contains(&index) {
                continue;
            }
            let vector_prune_result = vector_prune_result_map.remove(&index).unwrap();

            let mut vector_scores = Vec::with_capacity(vector_prune_result.scores.len());
            for vector_score in &vector_prune_result.scores {
                vector_scores.push((vector_score.row_idx as usize, vector_score.score));
            }
            let mut block_meta_index = vector_prune_result.block_meta_index;
            block_meta_index.vector_scores = Some(vector_scores);

            pruned_metas.push((block_meta_index, vector_prune_result.block_meta));
        }

        Ok(pruned_metas)
    }

    async fn vector_index_scores(
        &self,
        metas: Vec<(BlockMetaIndex, Arc<BlockMeta>)>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        let start = Instant::now();

        let results = self
            .pruning_ctx
            .pruning_cost
            .measure_async(
                PruningCostKind::BlocksVector,
                self.process_vector_pruning_tasks(
                    metas,
                    |vector_reader, row_count, location| async move {
                        vector_reader.generate_scores(row_count, &location).await
                    },
                ),
            )
            .await?;

        let mut vector_prune_result_map = HashMap::with_capacity(results.len());
        for vector_prune_result in results {
            vector_prune_result_map.insert(vector_prune_result.block_idx, vector_prune_result);
        }

        let len = vector_prune_result_map.len();
        let mut new_metas = Vec::with_capacity(len);
        for index in 0..len {
            let vector_prune_result = vector_prune_result_map.remove(&index).unwrap();

            let mut vector_scores = Vec::with_capacity(vector_prune_result.scores.len());
            for vector_score in &vector_prune_result.scores {
                vector_scores.push((vector_score.row_idx as usize, vector_score.score));
            }
            let mut block_meta_index = vector_prune_result.block_meta_index;
            block_meta_index.vector_scores = Some(vector_scores);

            new_metas.push((block_meta_index, vector_prune_result.block_meta));
        }

        let elapsed = start.elapsed().as_millis() as u64;
        // Perf.
        {
            metrics_inc_block_vector_index_pruning_milliseconds(elapsed);
        }
        info!("Vector index calculate score elapsed: {elapsed}");

        Ok(new_metas)
    }

    // Helper function to process vector pruning tasks with different vector reader operations
    async fn process_vector_pruning_tasks<F, Fut>(
        &self,
        metas: Vec<(BlockMetaIndex, Arc<BlockMeta>)>,
        vector_reader_op: F,
    ) -> Result<Vec<VectorPruneResult>>
    where
        F: Fn(VectorIndexReader, usize, String) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<Vec<ScoredPointOffset>>> + Send,
    {
        let pruning_runtime = &self.pruning_ctx.pruning_runtime;
        let pruning_semaphore = &self.pruning_ctx.pruning_semaphore;

        let mut block_meta_indexes = metas.into_iter().enumerate();
        let pruning_tasks = std::iter::from_fn(move || {
            block_meta_indexes
                .next()
                .map(|(index, (block_meta_index, block_meta))| {
                    let vector_reader = self.vector_reader.clone();
                    let index_name = self.vector_index.index_name.clone();
                    let vector_reader_op = vector_reader_op.clone();

                    let v: VectorPruningFuture = Box::new(move |permit: OwnedSemaphorePermit| {
                        Box::pin(async move {
                            let _permit = permit;

                            let Some(location) = &block_meta.vector_index_location else {
                                return Err(ErrorCode::StorageUnavailable(format!(
                                    "vector index {} file don't exist, need refresh",
                                    index_name
                                )));
                            };

                            let row_count = block_meta.row_count as usize;
                            let score_offsets =
                                vector_reader_op(vector_reader, row_count, location.0.clone())
                                    .await?;

                            let mut vector_scores = Vec::with_capacity(score_offsets.len());
                            for score_offset in score_offsets {
                                let vector_score = VectorScore {
                                    index,
                                    row_idx: score_offset.idx,
                                    score: F32::from(score_offset.score),
                                };
                                vector_scores.push(vector_score);
                            }

                            Ok(VectorPruneResult {
                                block_idx: index,
                                scores: vector_scores,
                                block_meta_index,
                                block_meta,
                            })
                        })
                    });
                    v
                })
        });

        let join_handlers = pruning_runtime
            .try_spawn_batch_with_owned_semaphore(pruning_semaphore.clone(), pruning_tasks)
            .await?;

        let joint = future::try_join_all(join_handlers)
            .await
            .map_err(|e| ErrorCode::StorageOther(format!("vector pruning failure: {}", e)))?;

        let mut results = Vec::with_capacity(joint.len());
        for result in joint {
            results.push(result?);
        }

        Ok(results)
    }

    fn vector_prune_candidates(
        &self,
        metas: Vec<(BlockMetaIndex, Arc<BlockMeta>)>,
    ) -> Result<Vec<VectorPruneCandidate>> {
        metas
            .into_iter()
            .map(|(block_meta_index, block_meta)| {
                let vector_stat = self.vector_stat_score_domain(block_meta.as_ref())?;
                Ok(VectorPruneCandidate {
                    block_meta_index,
                    block_meta,
                    score_domain: vector_stat.map(|(score_domain, _)| score_domain),
                    vector_stat_row_count: vector_stat.map(|(_, row_count)| row_count),
                })
            })
            .collect()
    }

    fn filter_candidates_by_topn_stats(
        &self,
        candidates: Vec<VectorPruneCandidate>,
        limit: usize,
    ) -> (Vec<VectorPruneCandidate>, usize) {
        if limit == 0 {
            let skipped_blocks = candidates.len();
            return (Vec::new(), skipped_blocks);
        }

        // Build a safe threshold for ascending vector TopN.
        //
        // Each block vector stat gives a score interval [lower, upper] that
        // contains every row score in that block. If the blocks with the
        // smallest upper bounds already contain at least `limit` rows, then
        // those rows are guaranteed to have scores <= the last selected upper
        // bound. Any other block whose lower bound is greater than that
        // threshold cannot contribute to ORDER BY score ASC LIMIT N.
        //
        // Example, LIMIT 10:
        //   A: rows=6, [0.10, 0.20]
        //   B: rows=4, [0.15, 0.30]
        //   C: rows=8, [0.35, 0.50]
        //   D: rows=8, [0.05, 0.80]
        // A + B already provide 10 rows with scores <= 0.30, so C can be
        // skipped because all its rows have scores > 0.30. D must be kept
        // because it may contain rows with scores as low as 0.05.
        let mut upper_bounds = candidates
            .iter()
            .filter_map(|candidate| {
                let (_, upper_bound) = candidate.score_domain?;
                let row_count = candidate.vector_stat_row_count?;
                Some((upper_bound, row_count))
            })
            .collect::<Vec<_>>();
        upper_bounds.sort_by(|left, right| left.0.partial_cmp(&right.0).unwrap_or(Ordering::Equal));

        let target_rows = limit as u64;
        let mut row_count = 0u64;
        let mut threshold = None;
        for (upper_bound, rows) in upper_bounds {
            row_count = row_count.saturating_add(rows);
            if row_count >= target_rows {
                threshold = Some(upper_bound);
                break;
            }
        }

        let Some(threshold) = threshold else {
            return (candidates, 0);
        };

        let mut skipped_blocks = 0usize;
        let mut keep_candidates = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let Some(lower_bound) = candidate.score_lower_bound() else {
                keep_candidates.push(candidate);
                continue;
            };

            // There are already at least `limit` rows in blocks whose maximum
            // possible score is not greater than `threshold`. A block whose
            // minimum possible score is greater than that threshold cannot
            // contribute rows to the ascending vector TopN result.
            if lower_bound > threshold {
                skipped_blocks += 1;
                continue;
            }

            keep_candidates.push(candidate);
        }

        (keep_candidates, skipped_blocks)
    }

    fn vector_stat_score_domain(
        &self,
        block_meta: &BlockMeta,
    ) -> Result<Option<((f32, f32), u64)>> {
        let Some(vector_stats) = block_meta.vector_stats.as_ref() else {
            return Ok(None);
        };
        let Some(vector_stat) =
            vector_stats.get(&(self.vector_index.column_id, self.vector_distance_type))
        else {
            return Ok(None);
        };

        let score_domain =
            vector_stat.distance_domain(&self.query_values, self.vector_distance_type)?;
        Ok(Some((score_domain, vector_stat.row_count)))
    }
}

struct VectorPruneCandidate {
    block_meta_index: BlockMetaIndex,
    block_meta: Arc<BlockMeta>,
    score_domain: Option<(f32, f32)>,
    vector_stat_row_count: Option<u64>,
}

impl VectorPruneCandidate {
    fn score_lower_bound(&self) -> Option<f32> {
        self.score_domain.map(|(lower_bound, _)| lower_bound)
    }
}

struct VectorPruneResult {
    // the block index in segment
    block_idx: usize,
    scores: Vec<VectorScore>,
    block_meta_index: BlockMetaIndex,
    block_meta: Arc<BlockMeta>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct VectorScore {
    index: usize,
    row_idx: u32,
    score: F32,
}

impl Ord for VectorScore {
    fn cmp(&self, other: &Self) -> Ordering {
        // reverse order to keep lower score.
        other.score.cmp(&self.score)
    }
}

impl PartialOrd for VectorScore {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl VectorScore {
    // Create a modified vector score with negated score to reverse ordering
    fn negative_score(&self) -> Self {
        Self {
            index: self.index,
            row_idx: self.row_idx,
            score: -self.score,
        }
    }
}
