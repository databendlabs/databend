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
use std::collections::HashMap;
use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::base::tokio::sync::OwnedSemaphorePermit;
use databend_common_catalog::plan::Filters;
use databend_common_catalog::plan::VectorIndexInfo;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::F32;
use databend_common_expression::RemoteExpr;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::VECTOR_SCORE_COL_NAME;
use databend_common_metrics::storage::metrics_inc_block_vector_index_pruning_milliseconds;
use databend_common_metrics::storage::metrics_inc_blocks_vector_index_pruning_after;
use databend_common_metrics::storage::metrics_inc_blocks_vector_index_pruning_before;
use databend_common_metrics::storage::metrics_inc_bytes_block_vector_index_pruning_after;
use databend_common_metrics::storage::metrics_inc_bytes_block_vector_index_pruning_before;
use databend_storages_common_index::DistanceType;
use databend_storages_common_index::FixedLengthPriorityQueue;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_pruner::BlockMetaIndex;
use databend_storages_common_table_meta::meta::BlockMeta;
use futures_util::future;
use log::info;

use crate::io::read::VectorIndexReader;
use crate::pruning::PruningContext;

type VectorPruningFutureReturn = Pin<Box<dyn Future<Output = Result<VectorPruneResult>> + Send>>;
type VectorPruningFuture =
    Box<dyn FnOnce(OwnedSemaphorePermit) -> VectorPruningFutureReturn + Send + 'static>;

/// Vector index pruner.
#[derive(Clone)]
pub struct VectorIndexPruner {
    pruning_ctx: Arc<PruningContext>,
    _schema: TableSchemaRef,
    vector_index: VectorIndexInfo,
    filters: Option<Filters>,
    sort: Vec<(RemoteExpr<String>, bool, bool)>,
    limit: Option<usize>,
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
        Ok(Self {
            pruning_ctx,
            _schema: schema,
            vector_index,
            filters,
            sort,
            limit,
        })
    }
}

impl VectorIndexPruner {
    pub async fn prune(
        &self,
        metas: Vec<(BlockMetaIndex, Arc<BlockMeta>)>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        let settings = ReadSettings::from_ctx(&self.pruning_ctx.ctx)?;
        let distance_type = match self.vector_index.func_name.as_str() {
            "cosine_distance" => DistanceType::Dot,
            "l1_distance" => DistanceType::L1,
            "l2_distance" => DistanceType::L2,
            _ => unreachable!(),
        };
        let columns = vec![
            format!(
                "{}-{}_graph_links",
                self.vector_index.column_id, distance_type
            ),
            format!(
                "{}-{}_graph_data",
                self.vector_index.column_id, distance_type
            ),
            format!(
                "{}-{}_encoded_u8_meta",
                self.vector_index.column_id, distance_type
            ),
            format!(
                "{}-{}_encoded_u8_data",
                self.vector_index.column_id, distance_type
            ),
        ];

        let query_values = unsafe {
            std::mem::transmute::<Vec<F32>, Vec<f32>>(self.vector_index.query_values.clone())
        };

        let vector_reader = VectorIndexReader::create(
            self.pruning_ctx.dal.clone(),
            settings,
            distance_type,
            columns,
            query_values,
        );

        // @TODO support filters
        if self.filters.is_none() && !self.sort.is_empty() && self.limit.is_some() {
            let (sort, asc, _nulls_first) = &self.sort[0];
            if let RemoteExpr::ColumnRef { id, .. } = sort {
                if id == VECTOR_SCORE_COL_NAME && *asc {
                    let limit = self.limit.unwrap();
                    return self
                        .vector_index_topn_prune(vector_reader, limit, metas)
                        .await;
                }
            }
        }

        self.vector_index_prune(vector_reader, metas).await
    }

    async fn vector_index_topn_prune(
        &self,
        vector_reader: VectorIndexReader,
        limit: usize,
        metas: Vec<(BlockMetaIndex, Arc<BlockMeta>)>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        let pruning_runtime = &self.pruning_ctx.pruning_runtime;
        let pruning_semaphore = &self.pruning_ctx.pruning_semaphore;

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

        let mut block_meta_indexes = metas.into_iter().enumerate();
        let pruning_tasks = std::iter::from_fn(move || {
            block_meta_indexes
                .next()
                .map(|(index, (block_meta_index, block_meta))| {
                    let vector_reader = vector_reader.clone();
                    let index_name = self.vector_index.index_name.clone();

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
                                vector_reader.prune(limit, row_count, &location.0).await?;

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
            .map_err(|e| ErrorCode::StorageOther(format!("vector topn pruning failure, {}", e)))?;

        let mut top_queue = FixedLengthPriorityQueue::new(limit);
        let mut vector_prune_result_map = HashMap::with_capacity(joint.len());
        for vector_prune_result in joint {
            let vector_prune_result = vector_prune_result?;

            for vector_score in &vector_prune_result.scores {
                top_queue.push(vector_score.clone());
            }
            vector_prune_result_map.insert(vector_prune_result.block_idx, vector_prune_result);
        }

        let top_scores = top_queue.into_sorted_vec();
        let top_indexes: HashSet<usize> = top_scores.iter().map(|s| s.index).collect();

        let mut pruned_metas = Vec::with_capacity(top_indexes.len());
        let len = vector_prune_result_map.len();
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
        info!("[FUSE-PRUNER] Vector index topn prune elapsed: {elapsed}");

        Ok(pruned_metas)
    }

    async fn vector_index_prune(
        &self,
        vector_reader: VectorIndexReader,
        metas: Vec<(BlockMetaIndex, Arc<BlockMeta>)>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        // can't use vector index topn to prune, only generate vector scores.
        let pruning_runtime = &self.pruning_ctx.pruning_runtime;
        let pruning_semaphore = &self.pruning_ctx.pruning_semaphore;

        let start = Instant::now();
        let mut block_meta_indexes = metas.into_iter().enumerate();
        let pruning_tasks = std::iter::from_fn(move || {
            block_meta_indexes
                .next()
                .map(|(index, (block_meta_index, block_meta))| {
                    let vector_reader = vector_reader.clone();
                    let index_name = self.vector_index.index_name.clone();

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
                            let score_offsets = vector_reader
                                .generate_scores(row_count, &location.0)
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
            .map_err(|e| ErrorCode::StorageOther(format!("vector pruning failure, {}", e)))?;

        let mut vector_prune_result_map = HashMap::with_capacity(joint.len());
        for vector_prune_result in joint {
            let vector_prune_result = vector_prune_result?;
            vector_prune_result_map.insert(vector_prune_result.block_idx, vector_prune_result);
        }

        let len = vector_prune_result_map.len();
        let mut new_metas = Vec::with_capacity(len);
        for index in 0..len {
            let vector_prune_result = vector_prune_result_map.remove(&index).unwrap();
            let mut vector_scores =
                Vec::with_capacity(vector_prune_result.block_meta.row_count as usize);
            for score in &vector_prune_result.scores {
                vector_scores.push((score.row_idx as usize, score.score));
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
        info!("[FUSE-PRUNER] Vector index prune elapsed: {elapsed}");

        Ok(new_metas)
    }
}

// result of block pruning
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
