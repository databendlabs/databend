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
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::VECTOR_SCORE_COL_NAME;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::Buffer;
use databend_common_expression::types::DataType;
use databend_common_expression::types::F32;
use databend_common_expression::types::NumberColumn;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
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
use databend_storages_common_table_meta::meta::VectorDistanceType;
use futures_util::future;
use log::info;
use tokio::sync::OwnedSemaphorePermit;

use crate::io::VectorIndexSearchOptions;
use crate::io::read::VectorIndexIdentity;
use crate::io::read::VectorIndexReader;
use crate::pruning::PruningContext;
use crate::pruning::PruningCostKind;

// Experimental margins for SQ8 candidate filtering, not quantization error bounds.
// Keep them independent of the refinement candidate budget.
const REFINE_COSINE_FILTER_MARGIN: f64 = 0.02;
const REFINE_RELATIVE_FILTER_MARGIN: f64 = 0.05;
const REFINE_MIN_FILTER_MARGIN: f64 = 1e-3;

#[derive(Clone)]
struct VectorTopNParam {
    has_filter: bool,
    filter_expr: Option<Expr>,
    candidate_filter: Option<Expr>,
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
                // Prepare the budget and filter once for all pruning batches.
                // Saturation treats an overflowing budget as effectively unlimited.
                let (candidate_limit, candidate_filter) = match refine_options.refine_factor {
                    Some(factor) => (
                        limit.saturating_mul(factor),
                        filter_expr
                            .as_ref()
                            .map(|expr| widen_score_filter(expr, vector_distance_type)),
                    ),
                    None => (limit, None),
                };
                Some(VectorTopNParam {
                    has_filter: filters.is_some(),
                    filter_expr,
                    candidate_filter,
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
        // Statistics bound the original Float32 vectors, not the FP16 scores used by
        // refinement, so blocks are only pruned by them without refinement.
        let candidates = if refine {
            candidates
        } else {
            let (candidates, skipped_blocks) =
                self.filter_candidates_by_topn_stats(candidates, limit);
            if skipped_blocks > 0 {
                info!("Vector stat pruned {skipped_blocks} blocks before hnsw topn");
            }
            candidates
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
        let mut results = self
            .score_blocks(metas, move |reader, row_count, location| async move {
                reader.generate_scores(row_count, &location).await
            })
            .await?;
        if self.refine_options.refine_factor.is_some() {
            // Rows passing the original predicate on SQ8 scores fill the candidate
            // budget, as without widening. Rows that pass only the widened predicate
            // (near the threshold, where SQ8 may misjudge) get a separate budget, so
            // they add candidates rather than crowding out the confident ones.
            // The original predicate is applied below to the FP16 scores.
            let passed = self.eval_score_filters(param.filter_expr.as_ref(), &results)?;
            let mut candidates = select_refine_candidates(
                ranked_scores(&results, &passed, param.asc),
                param.candidate_limit,
            );
            if param.candidate_filter.is_some() {
                let widened = self.eval_score_filters(param.candidate_filter.as_ref(), &results)?;
                let near = near_threshold(
                    results.iter().map(|result| result.scores.len()),
                    &passed,
                    &widened,
                );
                let extra = select_refine_candidates(
                    ranked_scores(&results, &near, param.asc),
                    param.candidate_limit,
                );
                // Disjoint from `candidates`: `near` excludes rows passing `passed`.
                for (block, rows) in extra {
                    candidates.entry(block).or_default().extend(rows);
                }
            }
            results = self.refine_candidates(results, candidates).await?;
        }

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
        // Unsupported candidate predicates widen to TRUE. Treat that as no filter
        // rather than materializing a score column and an all-true bitmap per block.
        let filter_expr = match filter_expr {
            Some(Expr::Constant(value)) if matches!(&value.scalar, Scalar::Boolean(true)) => None,
            other => other,
        };
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

/// Widen a score-only predicate for approximate candidate selection. Unsupported
/// expressions pass all candidates; the original predicate remains unchanged and
/// is evaluated after FP16 scoring. In particular, do not rewrite inside NOT.
fn widen_score_filter(expr: &Expr, distance: VectorDistanceType) -> Expr {
    try_widen_score_filter(expr, distance)
        .unwrap_or_else(|| Expr::constant(Scalar::Boolean(true), None))
}

fn try_widen_score_filter(expr: &Expr, distance: VectorDistanceType) -> Option<Expr> {
    let call = match expr {
        Expr::Constant(value) if matches!(&value.scalar, Scalar::Boolean(_)) => {
            return Some(expr.clone());
        }
        Expr::FunctionCall(call) => call,
        _ => return None,
    };
    let name = call.function.signature.name.as_str();
    if matches!(
        name,
        "and" | "and_filters" | "or" | "or_filters" | "is_true"
    ) {
        let args = call
            .args
            .iter()
            .map(|arg| widen_score_filter(arg, distance))
            .collect::<Vec<_>>();
        return check_function(call.span, name, &[], &args, &BUILTIN_FUNCTIONS).ok();
    }
    let (reversed, lower_threshold) = match name {
        "gt" => ("lt", true),
        "gte" => ("lte", true),
        "lt" => ("gt", false),
        "lte" => ("gte", false),
        _ => return None,
    };
    let [left, right] = call.args.as_slice() else {
        return None;
    };
    // Normalize comparisons to score OP constant; reversing also flips the margin.
    let (name, score, value, lower_threshold) = if is_score_column(left) {
        (name, left, right, lower_threshold)
    } else if is_score_column(right) {
        (reversed, right, left, !lower_threshold)
    } else {
        return None;
    };
    let Expr::Constant(value) = value else {
        return None;
    };
    let threshold = match &value.scalar {
        Scalar::Number(value) => value.to_f64().0,
        Scalar::Decimal(value) => value.to_float64(),
        _ => return None,
    };
    if !threshold.is_finite() {
        return None;
    }
    let margin = match distance {
        VectorDistanceType::Dot => REFINE_COSINE_FILTER_MARGIN,
        VectorDistanceType::L1 | VectorDistanceType::L2 => {
            (threshold.abs() * REFINE_RELATIVE_FILTER_MARGIN).max(REFINE_MIN_FILTER_MARGIN)
        }
    };
    let threshold = if lower_threshold {
        threshold - margin
    } else {
        threshold + margin
    };
    if !threshold.is_finite() {
        return None;
    }
    let args = [
        score.clone(),
        Expr::constant(
            Scalar::Number(NumberScalar::Float64(threshold.into())),
            None,
        ),
    ];
    check_function(call.span, name, &[], &args, &BUILTIN_FUNCTIONS).ok()
}

/// The projected score is column zero. Only lossless widening casts are allowed;
/// arithmetic or narrowing casts need different margins and are not rewritten.
fn is_score_column(expr: &Expr) -> bool {
    match expr {
        Expr::ColumnRef(column) => column.id == 0,
        Expr::Cast(cast)
            if !cast.is_try
                && cast.dest_type.remove_nullable()
                    == DataType::Number(NumberDataType::Float64) =>
        {
            is_score_column(&cast.expr)
        }
        _ => false,
    }
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

/// Rows that pass the widened predicate but not the original one, per block.
/// `None` in `original` (no predicate) excludes every row; `None` in `widened`
/// (widened to TRUE) passes every row.
fn near_threshold(
    lengths: impl Iterator<Item = usize>,
    original: &[Option<Bitmap>],
    widened: &[Option<Bitmap>],
) -> Vec<Option<Bitmap>> {
    lengths
        .zip(original.iter().zip(widened))
        .map(|(len, (original, widened))| {
            let rejected = match original {
                Some(original) => !original,
                None => Bitmap::new_zeroed(len),
            };
            Some(match widened {
                Some(widened) => &rejected & widened,
                None => rejected,
            })
        })
        .collect()
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
    use databend_common_expression::ColumnRef;

    use super::*;

    fn score_filter(name: &str, threshold: f64, reversed: bool) -> Result<Expr> {
        let score = Expr::ColumnRef(ColumnRef {
            span: None,
            id: 0,
            data_type: DataType::Number(NumberDataType::Float32),
            display_name: VECTOR_SCORE_COL_NAME.to_string(),
        });
        let threshold = Expr::constant(
            Scalar::Number(NumberScalar::Float64(threshold.into())),
            None,
        );
        let args = if reversed {
            [threshold, score]
        } else {
            [score, threshold]
        };
        check_function(None, name, &[], &args, &BUILTIN_FUNCTIONS)
    }

    fn eval_filter(expr: &Expr, scores: &[f32]) -> Result<Vec<bool>> {
        let column = Column::Number(NumberColumn::Float32(
            scores
                .iter()
                .copied()
                .map(F32::from)
                .collect::<Vec<_>>()
                .into(),
        ));
        let block = DataBlock::new(vec![BlockEntry::from(column)], scores.len());
        let context = FunctionContext::default();
        let value = Evaluator::new(&block, &context, &BUILTIN_FUNCTIONS).run(expr)?;
        let column = value
            .into_full_column(expr.data_type(), scores.len())
            .remove_nullable();
        Ok(column.as_boolean().unwrap().iter().collect())
    }

    #[test]
    fn test_refine_candidate_filter_margins() -> Result<()> {
        for (op, threshold, reversed, distance, inside, outside) in [
            ("gt", 0.5, false, VectorDistanceType::Dot, 0.49, 0.47),
            ("lt", 0.5, false, VectorDistanceType::Dot, 0.51, 0.53),
            ("lt", 0.5, true, VectorDistanceType::Dot, 0.49, 0.47),
            ("gt", 100.0, false, VectorDistanceType::L1, 96.0, 94.0),
            ("lt", 0.0, false, VectorDistanceType::L2, 0.0005, 0.002),
        ] {
            let original = score_filter(op, threshold, reversed)?;
            let widened = widen_score_filter(&original, distance);
            assert_eq!(eval_filter(&widened, &[inside, outside])?, vec![
                true, false
            ]);
            assert_eq!(eval_filter(&original, &[inside, outside])?, vec![
                false, false
            ]);
        }
        Ok(())
    }

    #[test]
    fn test_refine_candidate_filter_combinations() -> Result<()> {
        for (name, left, right, values, expected, original_expected) in [
            (
                "and_filters",
                score_filter("gt", 0.5, false)?,
                score_filter("lt", 0.8, false)?,
                [0.47, 0.49, 0.6, 0.81, 0.83],
                [false, true, true, true, false],
                [false, false, true, false, false],
            ),
            (
                "or_filters",
                score_filter("lt", 0.2, false)?,
                score_filter("gt", 0.8, false)?,
                [0.1, 0.21, 0.5, 0.79, 0.9],
                [true, true, false, true, true],
                [true, false, false, false, true],
            ),
        ] {
            let original = check_function(None, name, &[], &[left, right], &BUILTIN_FUNCTIONS)?;
            let original = check_function(None, "is_true", &[], &[original], &BUILTIN_FUNCTIONS)?;
            let widened = widen_score_filter(&original, VectorDistanceType::Dot);
            assert_eq!(eval_filter(&widened, &values)?, expected.to_vec());
            assert_eq!(eval_filter(&original, &values)?, original_expected.to_vec());
        }
        let negated = check_function(
            None,
            "not",
            &[],
            &[score_filter("gt", 0.5, false)?],
            &BUILTIN_FUNCTIONS,
        )?;
        assert_eq!(
            widen_score_filter(&negated, VectorDistanceType::Dot),
            Expr::constant(Scalar::Boolean(true), None)
        );
        Ok(())
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
    fn test_refine_near_threshold_rows() {
        let bitmap = |bits: &[bool]| Some(Bitmap::from_iter(bits.iter().copied()));
        let near = near_threshold(
            [3, 2, 2].into_iter(),
            &[bitmap(&[true, false, false]), bitmap(&[true, false]), None],
            &[bitmap(&[true, true, false]), None, None],
        );
        let bits = |bitmap: &Option<Bitmap>| bitmap.as_ref().unwrap().iter().collect::<Vec<_>>();
        // Widened but not original; widened to TRUE; no original predicate.
        assert_eq!(bits(&near[0]), vec![false, true, false]);
        assert_eq!(bits(&near[1]), vec![false, true]);
        assert_eq!(bits(&near[2]), vec![false, false]);
    }
}
