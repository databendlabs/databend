//  Copyright 2023 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;

use common_base::base::tokio::sync::OwnedSemaphorePermit;
use common_catalog::plan::BLOCK_NAME;
use common_catalog::plan::SEGMENT_NAME;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::StringType;
use common_expression::FromData;
use common_expression::RemoteExpr;
use common_expression::TableSchemaRef;
use futures_util::future;
use storages_common_cache::LoadParams;
use storages_common_pruner::BlockMetaIndex;
use storages_common_table_meta::meta::BlockMeta;

use super::fuse_pruner::eval_filters_with_one_column;
use super::SegmentLocation;
use crate::io::MetaReaders;
use crate::metrics::*;
use crate::pruning::BlockPruner;
use crate::pruning::PruningContext;

/// Segment level pruning: range pruning.
pub struct SegmentPruner {
    pub pruning_ctx: Arc<PruningContext>,
    pub table_schema: TableSchemaRef,
}

impl SegmentPruner {
    pub fn create(
        pruning_ctx: Arc<PruningContext>,
        table_schema: TableSchemaRef,
    ) -> Result<SegmentPruner> {
        Ok(SegmentPruner {
            pruning_ctx,
            table_schema,
        })
    }

    #[async_backtrace::framed]
    pub async fn pruning(
        &self,
        mut segment_locs: Vec<SegmentLocation>,
        filter: Option<&RemoteExpr<String>>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        if segment_locs.is_empty() {
            return Ok(vec![]);
        }

        let seg_filter = filter.and_then(|f| f.ignore_except(SEGMENT_NAME));
        if let Some(filter) = seg_filter {
            // Prune segments by locations if there are internal column `_segment_name` in the push down predicates.
            let seg_name_col = StringType::from_data(
                segment_locs
                    .iter()
                    .map(|loc| loc.location.0.as_bytes().to_vec())
                    .collect::<Vec<_>>(),
            );
            let res = eval_filters_with_one_column(
                &self.pruning_ctx.ctx.get_function_context()?,
                SEGMENT_NAME,
                seg_name_col,
                &filter,
            )?;

            if res.unset_bits() == res.len() {
                // All segments are pruned.
                return Ok(vec![]);
            }

            segment_locs = segment_locs
                .into_iter()
                .zip(res.iter())
                .filter(|(_, v)| *v)
                .map(|(loc, _)| loc)
                .collect();
        }

        // Build pruning tasks.
        let mut segments = segment_locs.into_iter().enumerate();
        let limit_pruner = self.pruning_ctx.limit_pruner.clone();
        let block_filter = filter.and_then(|f| f.ignore_except(BLOCK_NAME));
        let pruning_tasks = std::iter::from_fn(|| {
            // pruning tasks are executed concurrently, check if limit exceeded before proceeding
            if limit_pruner.exceeded() {
                None
            } else {
                segments.next().map(|(segment_idx, segment_location)| {
                    let pruning_ctx = self.pruning_ctx.clone();
                    let table_schema = self.table_schema.clone();
                    let block_filter = block_filter.clone();
                    move |permit| async move {
                        Self::segment_pruning(
                            pruning_ctx,
                            permit,
                            table_schema,
                            segment_idx,
                            segment_location,
                            block_filter,
                        )
                        .await
                    }
                })
            }
        });

        // Run tasks and collect the results.
        let pruning_runtime = self.pruning_ctx.pruning_runtime.clone();
        let pruning_semaphore = self.pruning_ctx.pruning_semaphore.clone();
        let handlers = pruning_runtime
            .try_spawn_batch_with_owned_semaphore(pruning_semaphore, pruning_tasks)
            .await?;
        let joint = future::try_join_all(handlers)
            .await
            .map_err(|e| ErrorCode::StorageOther(format!("segment pruning failure, {}", e)))?;

        let metas = joint
            .into_iter()
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect();

        Ok(metas)
    }

    // Pruning segment with range pruner, then pruning on Block.
    #[async_backtrace::framed]
    async fn segment_pruning(
        pruning_ctx: Arc<PruningContext>,
        permit: OwnedSemaphorePermit,
        table_schema: TableSchemaRef,
        segment_idx: usize,
        segment_location: SegmentLocation,
        filter: Option<RemoteExpr<String>>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        let dal = pruning_ctx.dal.clone();
        let pruning_stats = pruning_ctx.pruning_stats.clone();

        // Keep in mind that segment_info_read must need a schema
        let segment_reader = MetaReaders::segment_info_reader(dal, table_schema.clone());
        let (location, ver) = segment_location.location.clone();
        let segment_info = segment_reader
            .read(&LoadParams {
                location,
                len_hint: None,
                ver,
                put_cache: true,
            })
            .await?;

        // IO job of reading segment done, release the permit, allows more concurrent pruners
        // Note that it is required to explicitly release this permit before pruning blocks, to avoid deadlock.
        drop(permit);

        let total_bytes = segment_info.total_bytes();
        // Perf.
        {
            metrics_inc_segments_range_pruning_before(1);
            metrics_inc_bytes_segment_range_pruning_before(total_bytes);

            pruning_stats.set_segments_range_pruning_before(1);
        }

        // Segment range pruning.
        let range_pruner = pruning_ctx.range_pruner.clone();
        let result = if range_pruner.should_keep(&segment_info.summary.col_stats) {
            // Perf.
            {
                metrics_inc_segments_range_pruning_after(1);
                metrics_inc_bytes_segment_range_pruning_after(total_bytes);

                pruning_stats.set_segments_range_pruning_after(1);
            }

            // Block pruner.
            let block_pruner = BlockPruner::create(pruning_ctx)?;
            block_pruner
                .pruning(segment_idx, segment_location, &segment_info, filter)
                .await?
        } else {
            vec![]
        };

        Ok(result)
    }
}
