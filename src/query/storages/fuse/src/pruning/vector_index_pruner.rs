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
use databend_common_expression::types::Bitmap;
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
use databend_storages_common_table_meta::meta::IndexFileLocation;
use databend_storages_common_table_meta::meta::VectorColumnStatistics;
use databend_storages_common_table_meta::meta::VectorDistanceType;
use futures_util::future;
use log::info;
use tokio::sync::OwnedSemaphorePermit;

use crate::io::VectorIndexSearchOptions;
use crate::io::read::VectorIndexIdentity;
use crate::io::read::VectorIndexReader;
use crate::pruning::PruningContext;
use crate::pruning::PruningCostKind;

#[derive(Clone)]
struct VectorTopNParam {
    has_filter: bool,
    filter_expr: Option<Expr>,
    asc: bool,
    limit: usize,
    candidate_limit: usize,
}

/// Vector index pruner.
#[derive(Clone)]
pub struct VectorIndexPruner {
    func_ctx: FunctionContext,
    pruning_ctx: Arc<PruningContext>,
    vector_index: VectorIndexInfo,
    vector_reader: VectorIndexReader,
    query_values: Vec<f32>,
    vector_distance_type: VectorDistanceType,
    vector_topn_param: Option<VectorTopNParam>,
    refine_options: VectorIndexSearchOptions,
}

impl VectorIndexPruner {
    pub fn create(
        pruning_ctx: Arc<PruningContext>,
        _schema: TableSchemaRef,
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

        let query_values =
            unsafe { std::mem::transmute::<Vec<F32>, Vec<f32>>(vector_index.query_values.clone()) };
        let vector_distance_type = distance_type.vector_distance_type();

        let query_settings = pruning_ctx.ctx.get_settings();
        let refine_options = VectorIndexSearchOptions::new(
            &vector_index.index_options,
            query_settings.get_vector_refine_factor()?,
            query_settings.get_vector_search_ef()?,
            query_settings.get_vector_probe_block_percent()?,
        )?;
        let vector_reader = VectorIndexReader::create(
            pruning_ctx.dal.clone(),
            settings,
            distance_type,
            query_values.clone(),
            VectorIndexIdentity {
                name: vector_index.index_name.clone(),
                version: vector_index.index_version.clone(),
                column_id: vector_index.column_id,
            },
            refine_options,
        );

        // A filter on the vector score column only can be applied to the scores.
        let filter_expr = match &filters {
            Some(filters) => {
                let filter = filters.filter.as_expr(&BUILTIN_FUNCTIONS);
                let column_refs = filter.column_refs();
                if column_refs.len() == 1 && column_refs.contains_key(VECTOR_SCORE_COL_NAME) {
                    Some(filter.project_column_ref(|_| Ok(0))?)
                } else {
                    None
                }
            }
            None => None,
        };

        // Ordering by the vector score first with a limit allows vector TopN pruning.
        let vector_topn_param = match (sort.first(), limit) {
            (Some((RemoteExpr::ColumnRef { id, .. }, asc, _)), Some(limit))
                if id == VECTOR_SCORE_COL_NAME =>
            {
                // Prepare the budget once for all pruning batches. Saturation treats
                // an overflowing budget as effectively unlimited.
                let candidate_limit = match refine_options.refine_factor {
                    Some(factor) => limit.saturating_mul(factor),
                    None => limit,
                };
                Some(VectorTopNParam {
                    has_filter: filters.is_some(),
                    filter_expr,
                    asc: *asc,
                    limit,
                    candidate_limit,
                })
            }
            _ => None,
        };

        Ok(Self {
            func_ctx,
            pruning_ctx,
            vector_index,
            vector_reader,
            query_values,
            vector_distance_type,
            vector_topn_param,
            refine_options,
        })
    }

    pub async fn prune(
        &self,
        metas: Vec<(BlockMetaIndex, Arc<BlockMeta>)>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        let Some(param) = &self.vector_topn_param else {
            // Not a vector TopN: only compute the scores.
            return self.vector_index_scores(metas).await;
        };
        if self.refine_options.refine_factor.is_some()
            && param.has_filter
            && param.filter_expr.is_none()
        {
            // Ordinary column predicates are evaluated after reading the data, so
            // scores of rows that fail them must not cause blocks to be discarded.
            return self.vector_index_scores(metas).await;
        }

        let start = Instant::now();
        let block_size = metas.iter().map(|(_, m)| m.block_size).sum();
        metrics_inc_blocks_vector_index_pruning_before(metas.len() as u64);
        metrics_inc_bytes_block_vector_index_pruning_before(block_size);
        self.pruning_ctx
            .pruning_stats
            .set_blocks_vector_index_pruning_before(metas.len() as u64);

        // Without filters, ascending order is answered by HNSW search; otherwise all
        // scores are computed, then filtered or ordered descending.
        let use_hnsw = !param.has_filter && param.asc;
        let topn = async {
            if use_hnsw {
                self.vector_index_hnsw_topn_prune(param, metas).await
            } else {
                self.vector_index_topn_prune(param, metas).await
            }
        };
        let pruned_metas = self
            .pruning_ctx
            .pruning_cost
            .measure_async(PruningCostKind::BlocksVector, topn)
            .await?;

        let elapsed = start.elapsed().as_millis() as u64;
        let block_size = pruned_metas.iter().map(|(_, m)| m.block_size).sum();
        metrics_inc_blocks_vector_index_pruning_after(pruned_metas.len() as u64);
        metrics_inc_bytes_block_vector_index_pruning_after(block_size);
        self.pruning_ctx
            .pruning_stats
            .set_blocks_vector_index_pruning_after(pruned_metas.len() as u64);
        metrics_inc_block_vector_index_pruning_milliseconds(elapsed);
        let kind = if use_hnsw { "hnsw" } else { "calculate score" };
        info!("Vector index {kind} topn prune elapsed: {elapsed}");
        Ok(pruned_metas)
    }

    /// Ascending TopN without filters, answered by HNSW search of each block.
    async fn vector_index_hnsw_topn_prune(
        &self,
        param: &VectorTopNParam,
        metas: Vec<(BlockMetaIndex, Arc<BlockMeta>)>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        let limit = param.limit;
        let candidate_limit = param.candidate_limit;
        if limit == 0 || metas.is_empty() {
            return Ok(Vec::new());
        }
        let refine = self.refine_options.refine_factor.is_some();
        let candidates = self.vector_prune_candidates(metas)?;
        // Safe pruning: the bounds hold for the exact scores of the rows, so a pruned
        // block holds no row of the exact TopN. This does not depend on the SQ8 or
        // FP16 scores used to rank the searched rows, so it also applies with
        // refinement.
        let (candidates, skipped_blocks) = self.filter_candidates_by_topn_stats(candidates, limit);
        if skipped_blocks > 0 {
            info!("Vector stat pruned {skipped_blocks} blocks before hnsw topn");
        }
        let candidates = match self.refine_options.probe_block_percent {
            Some(percent) => {
                let (candidates, skipped_blocks) =
                    probe_nearest_blocks(candidates, percent, candidate_limit);
                if skipped_blocks > 0 {
                    info!("Vector probe skipped {skipped_blocks} blocks before hnsw topn");
                }
                candidates
            }
            None => candidates,
        };
        let metas = candidates
            .into_iter()
            .map(|candidate| (candidate.block_meta_index, candidate.block_meta))
            .collect();

        let mut results = self
            .score_blocks(metas, move |reader, row_count, location| async move {
                reader.prune(candidate_limit, row_count, &location).await
            })
            .await?;
        self.search_deep_blocks(&mut results, candidate_limit)
            .await?;
        if refine {
            let candidates = select_refine_candidates(
                results
                    .iter()
                    .flat_map(|result| result.scores.iter().cloned()),
                candidate_limit,
            );
            results = self.refine_candidates(results, candidates).await?;
        }

        // Keep the global TopK rows, attached to their blocks.
        let mut top_queue = FixedLengthPriorityQueue::new(limit);
        for score in results.iter().flat_map(|result| result.scores.iter()) {
            top_queue.push(score.clone());
        }
        let mut top_rows: HashMap<usize, Vec<(usize, F32)>> = HashMap::new();
        for score in top_queue.into_sorted_vec() {
            top_rows
                .entry(score.index)
                .or_default()
                .push((score.row_idx as usize, score.score));
        }
        Ok(results
            .into_iter()
            .filter_map(|result| {
                let scores = top_rows.remove(&result.block_idx)?;
                Some(result.into_block_meta(scores))
            })
            .collect())
    }

    /// Searches again, with a larger ef, the blocks that supply many of the global
    /// candidates (see `deep_blocks`). Their new results replace the first ones.
    async fn search_deep_blocks(
        &self,
        results: &mut [VectorPruneResult],
        candidate_limit: usize,
    ) -> Result<()> {
        // The ef of the first search, as `HNSWIndex::search_with_ef` derives it.
        let base_ef = self
            .refine_options
            .search_ef
            .unwrap_or_else(|| candidate_limit.saturating_mul(4))
            .max(candidate_limit);
        let block_scores: Vec<&[VectorScore]> = results
            .iter()
            .map(|result| result.scores.as_slice())
            .collect();
        let Some((deep, ef)) = deep_blocks(&block_scores, candidate_limit, base_ef) else {
            return Ok(());
        };
        let metas = deep
            .iter()
            .map(|&i| {
                (
                    results[i].block_meta_index.clone(),
                    results[i].block_meta.clone(),
                )
            })
            .collect();
        let searched = self
            .score_blocks(metas, move |reader, row_count, location| async move {
                reader
                    .search(candidate_limit, Some(ef), row_count, &location)
                    .await
            })
            .await?;
        for (i, result) in deep.into_iter().zip(searched) {
            let block_idx = results[i].block_idx;
            results[i].scores = result
                .scores
                .into_iter()
                .map(|score| VectorScore {
                    index: block_idx,
                    ..score
                })
                .collect();
        }
        Ok(())
    }

    /// TopN with a score filter or in descending order: every row is scored. A
    /// selected block keeps the scores of all its rows.
    async fn vector_index_topn_prune(
        &self,
        param: &VectorTopNParam,
        metas: Vec<(BlockMetaIndex, Arc<BlockMeta>)>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        if param.limit == 0 || metas.is_empty() {
            return Ok(Vec::new());
        }
        // Descending order and score filters need the scores of every row: the
        // returned blocks are read whole, and a row without a score would get the
        // placeholder score, which ranks first in descending order and passes a
        // lower-bound filter. With refinement, every row is scored with FP16 data
        // (one read per block, not cached), as for full-row scoring, so the filter
        // and TopN below use the FP16 scores.
        let refine = self.refine_options.refine_factor.is_some();
        let results = self
            .score_blocks(metas, move |reader, row_count, location| async move {
                if refine {
                    reader.generate_refined_scores(row_count, &location).await
                } else {
                    reader.generate_scores(row_count, &location).await
                }
            })
            .await?;

        let passed = self.eval_score_filters(param.filter_expr.as_ref(), &results)?;
        let mut top_queue = FixedLengthPriorityQueue::new(param.limit);
        for score in ranked_scores(&results, &passed, param.asc) {
            top_queue.push(score);
        }
        let top_blocks: HashSet<usize> = top_queue
            .into_sorted_vec()
            .iter()
            .map(|score| score.index)
            .collect();
        Ok(results
            .into_iter()
            .filter(|result| top_blocks.contains(&result.block_idx))
            .map(VectorPruneResult::into_block_meta_with_all_scores)
            .collect())
    }

    /// Rescores the candidate rows (`candidates`, selected across all blocks from
    /// approximate scores) with FP16 data. Returns only the blocks with candidates;
    /// the caller performs the final global TopK.
    async fn refine_candidates(
        &self,
        results: Vec<VectorPruneResult>,
        mut candidates: HashMap<usize, Vec<u32>>,
    ) -> Result<Vec<VectorPruneResult>> {
        let start = Instant::now();
        let candidate_rows: usize = candidates.values().map(Vec::len).sum();
        let mut offsets_by_path = HashMap::with_capacity(candidates.len());
        let mut metas = Vec::with_capacity(candidates.len());
        for result in results {
            if let Some(offsets) = candidates.remove(&result.block_idx) {
                let location = self.index_file(&result.block_meta)?;
                offsets_by_path.insert(location.path().to_string(), offsets);
                metas.push((result.block_meta_index, result.block_meta));
            }
        }
        let offsets_by_path = Arc::new(offsets_by_path);
        let refined = self
            .score_blocks(metas, move |reader, row_count, location| {
                let offsets_by_path = offsets_by_path.clone();
                async move {
                    let offsets = &offsets_by_path[location.path()];
                    reader.refine_scores(row_count, &location, offsets).await
                }
            })
            .await?;
        info!(
            "Vector FP16 refine: candidate_rows={candidate_rows}, blocks={}, elapsed_ms={}",
            refined.len(),
            start.elapsed().as_millis()
        );
        Ok(refined)
    }

    /// Score filter result of each block; `None` without a filter.
    fn eval_score_filters(
        &self,
        filter_expr: Option<&Expr>,
        results: &[VectorPruneResult],
    ) -> Result<Vec<Option<Bitmap>>> {
        results
            .iter()
            .map(|result| match filter_expr {
                Some(expr) if !result.scores.is_empty() => {
                    self.eval_score_filter(expr, &result.scores).map(Some)
                }
                _ => Ok(None),
            })
            .collect()
    }

    /// Evaluates the score-only predicate `filter_expr` for each score. Scores may
    /// be sparse because NULL vectors are not index candidates; the bitmap follows
    /// `scores`, which keep their original row ids.
    fn eval_score_filter(&self, filter_expr: &Expr, scores: &[VectorScore]) -> Result<Bitmap> {
        let num_rows = scores.len();
        let column = Column::Number(NumberColumn::Float32(Buffer::from(
            scores.iter().map(|score| score.score).collect::<Vec<_>>(),
        )));
        let block = DataBlock::new(vec![BlockEntry::from(column)], num_rows);
        let evaluator = Evaluator::new(&block, &self.func_ctx, &BUILTIN_FUNCTIONS);
        let res = evaluator.run(filter_expr)?;
        let res_column = res
            .into_full_column(filter_expr.data_type(), num_rows)
            .remove_nullable();
        Ok(res_column.as_boolean().unwrap().clone())
    }

    /// Scores of all rows, without TopN pruning.
    async fn vector_index_scores(
        &self,
        metas: Vec<(BlockMetaIndex, Arc<BlockMeta>)>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        let start = Instant::now();
        // Every row's score is output. With refinement, scores are FP16 as in a
        // refined TopN, so a row's score does not depend on the query having a
        // limit. Each block's FP16 data is read whole in one uncached request
        // (vector_refine_factor = 0 uses SQ8 scores instead).
        let refine = self.refine_options.refine_factor.is_some();
        let scoring = self.score_blocks(metas, move |reader, row_count, location| async move {
            if refine {
                reader.generate_refined_scores(row_count, &location).await
            } else {
                reader.generate_scores(row_count, &location).await
            }
        });
        let results = self
            .pruning_ctx
            .pruning_cost
            .measure_async(PruningCostKind::BlocksVector, scoring)
            .await?;
        // Full-row scoring must retain even all-NULL blocks. Missing scores keep
        // the internal column's MAX placeholder until nullable scores are supported.
        let metas = results
            .into_iter()
            .map(VectorPruneResult::into_block_meta_with_all_scores)
            .collect();

        let elapsed = start.elapsed().as_millis() as u64;
        metrics_inc_block_vector_index_pruning_milliseconds(elapsed);
        info!("Vector index calculate score elapsed: {elapsed}");
        Ok(metas)
    }

    /// The index file of a block, or an error asking to refresh the index.
    fn index_file(&self, block_meta: &BlockMeta) -> Result<IndexFileLocation> {
        let index = &self.vector_index;
        block_meta
            .vector_index_file(&index.index_name, &index.index_version)
            .ok_or_else(|| {
                ErrorCode::StorageUnavailable(format!(
                    "vector index {} file don't exist, need refresh",
                    index.index_name
                ))
            })
    }

    /// Runs `op` on the index file of every block, concurrently (bounded by the
    /// pruning semaphore), and returns the scores of each block in block order.
    async fn score_blocks<F, Fut>(
        &self,
        metas: Vec<(BlockMetaIndex, Arc<BlockMeta>)>,
        op: F,
    ) -> Result<Vec<VectorPruneResult>>
    where
        F: Fn(VectorIndexReader, usize, IndexFileLocation) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<Vec<ScoredPointOffset>>> + Send,
    {
        let mut metas = metas.into_iter().enumerate();
        let tasks = std::iter::from_fn(|| {
            let (block_idx, (block_meta_index, block_meta)) = metas.next()?;
            let reader = self.vector_reader.clone();
            let location = self.index_file(&block_meta);
            let op = op.clone();
            Some(move |permit: OwnedSemaphorePermit| async move {
                let _permit = permit;
                let row_count = block_meta.row_count as usize;
                let scores = op(reader, row_count, location?)
                    .await?
                    .into_iter()
                    .map(|score| VectorScore {
                        index: block_idx,
                        row_idx: score.idx,
                        score: F32::from(score.score),
                    })
                    .collect();
                Ok(VectorPruneResult {
                    block_idx,
                    scores,
                    block_meta_index,
                    block_meta,
                })
            })
        });

        let handles = self
            .pruning_ctx
            .pruning_runtime
            .try_spawn_batch_with_owned_semaphore(self.pruning_ctx.pruning_semaphore.clone(), tasks)
            .await?;
        future::try_join_all(handles)
            .await
            .map_err(|e| ErrorCode::StorageOther(format!("vector pruning failure: {e}")))?
            .into_iter()
            .collect()
    }

    fn vector_prune_candidates(
        &self,
        metas: Vec<(BlockMetaIndex, Arc<BlockMeta>)>,
    ) -> Result<Vec<VectorPruneCandidate>> {
        metas
            .into_iter()
            .map(|(block_meta_index, block_meta)| {
                // Compute from the statistics before `block_meta` moves into the
                // candidate; they borrow from it.
                let vector_stat = self.vector_stat(block_meta.as_ref());
                let score_domain = vector_stat.and_then(|stat| {
                    conservative_score_domain(stat, &self.query_values, self.vector_distance_type)
                });
                let centroid_distance = vector_stat.and_then(|stat| {
                    centroid_distance(stat, &self.query_values, self.vector_distance_type)
                        .map(|distance| distance as f32)
                });
                let vector_stat_row_count = vector_stat.map(|stat| stat.row_count);
                Ok(VectorPruneCandidate {
                    block_meta_index,
                    block_meta,
                    score_domain,
                    centroid_distance,
                    vector_stat_row_count,
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

    /// Vector statistics of the indexed column of a block, for its distance type.
    fn vector_stat<'a>(&self, block_meta: &'a BlockMeta) -> Option<&'a VectorColumnStatistics> {
        block_meta
            .vector_stats
            .as_ref()?
            .get(&(self.vector_index.column_id, self.vector_distance_type))
    }
}

/// Bounds `[lower, upper]` of the scores of all rows of a block, from its vector
/// statistics, widened for floating point error so that pruning by them never
/// removes a row that could rank higher:
///
/// - the statistics radius and the row scores are computed in f32 over `dim` terms,
///   so each may be off by about `dim * f32::EPSILON` relative to the exact value;
/// - for cosine, an error `e` of a cosine changes its angle by up to `acos(1 - e)`,
///   which is large near 0 and PI, where `acos` is steep.
///
/// The distance between the query and the centroid is computed in f64. Returns
/// `None`, so the block is kept, when the statistics cannot bound the scores: a
/// dimension mismatch, non-finite values, or a zero query or centroid for cosine.
fn conservative_score_domain(
    stat: &VectorColumnStatistics,
    query: &[f32],
    distance: VectorDistanceType,
) -> Option<(f32, f32)> {
    let radius = f64::from(stat.radius.0);
    if !radius.is_finite() || radius < 0.0 {
        return None;
    }
    let d = centroid_distance(stat, query, distance)?;
    let relative = (query.len() as f64 * f64::from(f32::EPSILON)).min(1.0);
    let (lower, upper) = match distance {
        VectorDistanceType::L1 | VectorDistanceType::L2 => {
            let slack = relative * (d + radius) + f64::from(f32::MIN_POSITIVE);
            ((d - radius - slack).max(0.0), d + radius + slack)
        }
        VectorDistanceType::Dot => {
            let angle = d;
            // Covers the error of the radius, an angle computed from an f32 cosine.
            let slack = (1.0 - relative).acos();
            let lower = (angle - radius - slack).max(0.0);
            let upper = (angle + radius + slack).min(std::f64::consts::PI);
            // Covers the f32 error of the cosine distance of each row.
            (
                (1.0 - lower.cos() - relative).max(0.0),
                1.0 - upper.cos() + relative,
            )
        }
    };
    (lower.is_finite() && upper.is_finite()).then_some((lower as f32, upper as f32))
}

/// Distance between the query and the centroid of a block, in f64, in the units of
/// the statistics radius: L1 or L2 distance, or the angle for cosine. `None` for a
/// dimension mismatch, non-finite values, or a zero query or centroid for cosine.
fn centroid_distance(
    stat: &VectorColumnStatistics,
    query: &[f32],
    distance: VectorDistanceType,
) -> Option<f64> {
    if query.is_empty() || stat.centroid.len() != query.len() {
        return None;
    }
    let pairs = || {
        query
            .iter()
            .zip(&stat.centroid)
            .map(|(q, c)| (f64::from(*q), f64::from(c.0)))
    };
    let d = match distance {
        VectorDistanceType::L1 => pairs().map(|(q, c)| (q - c).abs()).sum::<f64>(),
        VectorDistanceType::L2 => pairs().map(|(q, c)| (q - c).powi(2)).sum::<f64>().sqrt(),
        VectorDistanceType::Dot => {
            let (dot, qq, cc) = pairs().fold((0.0, 0.0, 0.0), |(dot, qq, cc), (q, c)| {
                (dot + q * c, qq + q * q, cc + c * c)
            });
            if qq == 0.0 || cc == 0.0 {
                return None;
            }
            (dot / (qq.sqrt() * cc.sqrt())).clamp(-1.0, 1.0).acos()
        }
    };
    d.is_finite().then_some(d)
}

/// Approximate block pruning: keeps the `percent` of the blocks with vector
/// statistics whose centroids are nearest to the query, and more while they hold
/// fewer than `min_rows` rows, so that the search can produce the requested
/// candidates. Blocks without usable statistics are always kept. The score bounds
/// are only for safe pruning: in high dimensions block radii are as large as the
/// spread of centroid distances, so ranking by the lower bound ranks by radius
/// rather than by direction, and keeps wide blocks far from the query.
/// Returns the kept blocks and the number skipped.
fn probe_nearest_blocks(
    candidates: Vec<VectorPruneCandidate>,
    percent: usize,
    min_rows: usize,
) -> (Vec<VectorPruneCandidate>, usize) {
    let distances = candidates
        .iter()
        .map(|candidate| {
            Some((
                candidate.centroid_distance?,
                candidate.vector_stat_row_count?,
            ))
        })
        .collect::<Vec<_>>();
    let keep = probe_block_mask(&distances, percent, min_rows);
    let skipped = keep.iter().filter(|keep| !**keep).count();
    let candidates = candidates
        .into_iter()
        .zip(keep)
        .filter_map(|(candidate, keep)| keep.then_some(candidate))
        .collect();
    (candidates, skipped)
}

/// The blocks kept by `probe_nearest_blocks`, given their centroid distances and row
/// counts. Equal distances keep the block order.
fn probe_block_mask(
    distances: &[Option<(f32, u64)>],
    percent: usize,
    min_rows: usize,
) -> Vec<bool> {
    let mut keep = distances.iter().map(Option::is_none).collect::<Vec<_>>();
    let mut ranked = distances
        .iter()
        .enumerate()
        .filter_map(|(idx, distance)| distance.map(|distance| (idx, distance)))
        .collect::<Vec<_>>();
    ranked.sort_by(|(_, (d1, _)), (_, (d2, _))| d1.total_cmp(d2));
    let quota = ranked.len().saturating_mul(percent).div_ceil(100);
    let mut rows = 0u64;
    for (rank, (idx, (_, row_count))) in ranked.into_iter().enumerate() {
        if rank >= quota && rows >= min_rows as u64 {
            break;
        }
        keep[idx] = true;
        rows = rows.saturating_add(row_count);
    }
    keep
}

/// Scores that pass the score filter, ranked so that the best is the lowest
/// (negated for descending order).
fn ranked_scores<'a>(
    results: &'a [VectorPruneResult],
    passed: &'a [Option<Bitmap>],
    asc: bool,
) -> impl Iterator<Item = VectorScore> + 'a {
    results
        .iter()
        .zip(passed)
        .flat_map(move |(result, passed)| {
            result
                .scores
                .iter()
                .enumerate()
                .filter(move |(i, _)| passed.as_ref().is_none_or(|bitmap| bitmap.get_bit(*i)))
                .map(move |(_, score)| {
                    if asc {
                        score.clone()
                    } else {
                        score.negative_score()
                    }
                })
        })
}

/// A block supplying more than `1 / DEEP_BLOCK_SHARE` of the global candidates is
/// searched again.
const DEEP_BLOCK_SHARE: usize = 4;

/// Blocks to search again after the first HNSW search of every block, and the ef
/// to search them with. Each block was searched for its `candidate_limit` nearest
/// rows with the same `base_ef`, but HNSW finds the first rows of its results more
/// reliably than the last ones. A block that supplies `d` of the global candidates
/// (the `candidate_limit` lowest scores) needs its first `d` rows right; when the
/// candidates concentrate in few blocks, as in vector-clustered tables, `d` gets
/// close to `candidate_limit` and those rows are missed more often. The blocks
/// supplying more than `candidate_limit / DEEP_BLOCK_SHARE` candidates are searched
/// again with `base_ef` scaled by `d * DEEP_BLOCK_SHARE / candidate_limit` (at most
/// `DEEP_BLOCK_SHARE` times) for the largest `d`. `block_scores[i]` are the scores of
/// block `i`; returns the block positions, or `None` when no block is deep.
fn deep_blocks(
    block_scores: &[&[VectorScore]],
    candidate_limit: usize,
    base_ef: usize,
) -> Option<(Vec<usize>, usize)> {
    if candidate_limit == 0 {
        return None;
    }
    let mut queue = FixedLengthPriorityQueue::new(candidate_limit);
    for (block, scores) in block_scores.iter().enumerate() {
        for score in scores.iter() {
            queue.push(VectorScore {
                index: block,
                ..score.clone()
            });
        }
    }
    let mut supplied = vec![0usize; block_scores.len()];
    for score in queue.into_sorted_vec() {
        supplied[score.index] += 1;
    }
    let deep: Vec<usize> = (0..supplied.len())
        .filter(|&block| supplied[block] * DEEP_BLOCK_SHARE > candidate_limit)
        .collect();
    let max_supplied = deep.iter().map(|&block| supplied[block]).max()?;
    let ef = base_ef
        .saturating_mul(max_supplied * DEEP_BLOCK_SHARE)
        .div_ceil(candidate_limit);
    Some((deep, ef))
}

/// Keeps the `candidate_limit` lowest scores (callers negate scores for descending
/// order) and groups their row ids by block.
fn select_refine_candidates(
    scores: impl Iterator<Item = VectorScore>,
    candidate_limit: usize,
) -> HashMap<usize, Vec<u32>> {
    if candidate_limit == 0 {
        return HashMap::new();
    }
    let mut queue = FixedLengthPriorityQueue::new(candidate_limit);
    for score in scores {
        queue.push(score);
    }
    let mut offsets: HashMap<usize, Vec<u32>> = HashMap::new();
    for score in queue.into_sorted_vec() {
        offsets.entry(score.index).or_default().push(score.row_idx);
    }
    offsets
}

struct VectorPruneCandidate {
    block_meta_index: BlockMetaIndex,
    block_meta: Arc<BlockMeta>,
    /// Conservative bounds of the row scores, for safe pruning.
    score_domain: Option<(f32, f32)>,
    /// Distance between the query and the block centroid, for approximate pruning.
    centroid_distance: Option<f32>,
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

impl VectorPruneResult {
    /// The block with `vector_scores` (row id, score) attached.
    fn into_block_meta(self, vector_scores: Vec<(usize, F32)>) -> (BlockMetaIndex, Arc<BlockMeta>) {
        let mut block_meta_index = self.block_meta_index;
        block_meta_index.vector_scores = Some(vector_scores);
        (block_meta_index, self.block_meta)
    }

    /// The block with the scores of all its scored rows attached.
    fn into_block_meta_with_all_scores(self) -> (BlockMetaIndex, Arc<BlockMeta>) {
        let scores = self
            .scores
            .iter()
            .map(|score| (score.row_idx as usize, score.score))
            .collect();
        self.into_block_meta(scores)
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deep_blocks() {
        let scores = |block: usize, values: &[f32]| {
            values
                .iter()
                .enumerate()
                .map(|(row, value)| VectorScore {
                    index: block,
                    row_idx: row as u32,
                    score: F32::from(*value),
                })
                .collect::<Vec<_>>()
        };
        // 8 candidates: block 0 supplies 6, block 1 supplies 2, block 2 none.
        let near = scores(0, &[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.9, 0.95]);
        let mid = scores(1, &[0.15, 0.25, 0.7, 0.8, 0.85, 0.9, 0.95, 0.99]);
        let far = scores(2, &[0.8, 0.9, 0.95, 0.99, 1.0, 1.1, 1.2, 1.3]);
        let blocks = [near.as_slice(), mid.as_slice(), far.as_slice()];
        // Only block 0 supplies more than a quarter; ef 32 scaled by 6 * 4 / 8.
        assert_eq!(deep_blocks(&blocks, 8, 32), Some((vec![0], 96)));
        // Spread candidates search no block again.
        let spread = [&near[..2], &mid[..2], &far[..2], &near[2..4], &mid[2..4]];
        assert_eq!(deep_blocks(&spread, 8, 32), None);
        // A single block supplies every candidate: at most 4 times the ef.
        assert_eq!(deep_blocks(&[near.as_slice()], 8, 32), Some((vec![0], 128)));
        assert_eq!(deep_blocks(&blocks, 0, 32), None);
    }

    #[test]
    fn test_refine_candidate_budget_is_global() {
        let scores = [(0, 91, 0.5_f32), (0, 17, 0.2), (1, 42, 0.1), (2, 99, 0.9)]
            .into_iter()
            .map(|(index, row_idx, score)| VectorScore {
                index,
                row_idx,
                score: F32::from(score),
            });
        assert_eq!(
            select_refine_candidates(scores, 2),
            HashMap::from([(0, vec![17]), (1, vec![42])])
        );
    }

    #[test]
    fn test_conservative_score_domain_bounds_row_scores() -> Result<()> {
        use databend_storages_common_index::normalize_vector;
        use databend_storages_common_index::vector_stat_distance;

        // Deterministic points spread around a direction, and queries far and near.
        let dim = 16;
        let point = |seed: usize| -> Vec<f32> {
            (0..dim)
                .map(|d| 1.0 + (((seed * 31 + d * 17) % 97) as f32 / 97.0 - 0.5))
                .collect()
        };
        let points: Vec<Vec<f32>> = (0..50).map(point).collect();
        let queries = [point(1000), vec![-1.0; dim], points[0].clone()];
        // The row scores, as computed by the SQL distance functions (in f32).
        let score = |q: &[f32], x: &[f32], distance| match distance {
            VectorDistanceType::Dot => {
                let dot: f32 = q.iter().zip(x).map(|(a, b)| a * b).sum();
                let qq: f32 = q.iter().map(|a| a * a).sum();
                let xx: f32 = x.iter().map(|a| a * a).sum();
                1.0 - dot / (qq.sqrt() * xx.sqrt())
            }
            _ => vector_stat_distance(q, x, distance).unwrap(),
        };
        for distance in [
            VectorDistanceType::L1,
            VectorDistanceType::L2,
            VectorDistanceType::Dot,
        ] {
            // Statistics as written by the vector index writer.
            let rows: Vec<Vec<f32>> = points
                .iter()
                .map(|x| {
                    let mut x = x.clone();
                    if matches!(distance, VectorDistanceType::Dot) {
                        normalize_vector(&mut x);
                    }
                    x
                })
                .collect();
            let mut centroid = vec![0.0_f32; dim];
            for row in &rows {
                for (c, v) in centroid.iter_mut().zip(row) {
                    *c += v / rows.len() as f32;
                }
            }
            if matches!(distance, VectorDistanceType::Dot) {
                normalize_vector(&mut centroid);
            }
            let radius = rows
                .iter()
                .map(|row| vector_stat_distance(row, &centroid, distance).unwrap())
                .fold(0.0_f32, f32::max);
            let stat = VectorColumnStatistics {
                centroid: centroid.iter().copied().map(F32::from).collect(),
                radius: F32::from(radius),
                row_count: rows.len() as u64,
            };
            for query in &queries {
                let (lower, upper) = conservative_score_domain(&stat, query, distance).unwrap();
                for x in &points {
                    let score = score(query, x, distance);
                    assert!(
                        lower <= score && score <= upper,
                        "{distance:?} {score} not in [{lower}, {upper}]"
                    );
                }
            }
            // Statistics that cannot bound the scores keep the block.
            let mismatch = VectorColumnStatistics {
                centroid: vec![F32::from(1.0)],
                ..stat.clone()
            };
            assert_eq!(
                conservative_score_domain(&mismatch, &queries[0], distance),
                None
            );
            let nan = VectorColumnStatistics {
                radius: F32::from(f32::NAN),
                ..stat.clone()
            };
            assert_eq!(conservative_score_domain(&nan, &queries[0], distance), None);
        }
        let stat = VectorColumnStatistics {
            centroid: vec![F32::from(1.0); dim],
            radius: F32::from(0.1),
            row_count: 1,
        };
        assert_eq!(
            conservative_score_domain(&stat, &vec![0.0; dim], VectorDistanceType::Dot),
            None
        );
        Ok(())
    }

    #[test]
    fn test_probe_block_mask() {
        let distances = [
            Some((0.5, 10)),
            None,
            Some((0.1, 10)),
            Some((0.3, 10)),
            Some((0.2, 10)),
        ];
        // Half of the four blocks with statistics, nearest centroids first; the block
        // without statistics is always kept.
        assert_eq!(probe_block_mask(&distances, 50, 10), [
            false, true, true, false, true
        ]);
        // More blocks are kept until they hold the requested candidates.
        assert_eq!(probe_block_mask(&distances, 50, 25), [
            false, true, true, true, true
        ]);
        assert_eq!(probe_block_mask(&distances, 50, 1000), [true; 5]);
        // At least one block.
        assert_eq!(probe_block_mask(&distances, 1, 0), [
            false, true, true, false, false
        ]);
    }

    #[test]
    fn test_probe_ranks_by_centroid_not_lower_bound() {
        // A near, tight block and a far, wide one; the wide block has the smaller
        // lower bound, so ranking by it would probe the far block.
        let stat = |x: f32, radius: f32| VectorColumnStatistics {
            centroid: vec![F32::from(x), F32::from(0.0)],
            radius: F32::from(radius),
            row_count: 10,
        };
        let (near, far) = (stat(1.0, 0.1), stat(5.0, 5.0));
        let query = [0.0, 0.0];
        let distance = VectorDistanceType::L2;
        let lower = |stat| conservative_score_domain(stat, &query, distance).unwrap().0;
        assert!(lower(&far) < lower(&near));
        let distances = [&near, &far].map(|stat| {
            Some((
                centroid_distance(stat, &query, distance).unwrap() as f32,
                10,
            ))
        });
        assert_eq!(probe_block_mask(&distances, 50, 0), [true, false]);
        // Cosine ranks by angle; statistics without a usable centroid keep the block.
        let angle = centroid_distance(&stat(1.0, 0.1), &[0.0, 1.0], VectorDistanceType::Dot);
        assert!((angle.unwrap() - std::f64::consts::FRAC_PI_2).abs() < 1e-12);
        assert_eq!(
            centroid_distance(&near, &[0.0; 2], VectorDistanceType::Dot),
            None
        );
        assert_eq!(centroid_distance(&near, &[0.0; 3], distance), None);
    }
}
