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

use common_base::base::tokio::sync::Semaphore;
use common_base::runtime::Runtime;
use common_catalog::plan::PushDownInfo;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::FunctionContext;
use common_expression::TableSchemaRef;
use futures_util::future;
use opendal::Operator;
use storages_common_pruner::BlockMetaIndex;
use storages_common_pruner::LimiterPruner;
use storages_common_pruner::LimiterPrunerCreator;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::Location;

/// Segment level pruning.
pub struct SegmentPruner {
    pub func_ctx: FunctionContext,
    pub pruning_runtime: Arc<Runtime>,
    pub pruning_semaphore: Arc<Semaphore>,
    pub operator: Operator,
    pub table_schema: TableSchemaRef,
    pub limit_pruner: LimiterPruner,
}

impl SegmentPruner {
    pub fn create(
        func_ctx: FunctionContext,
        pruning_runtime: Arc<Runtime>,
        pruning_semaphore: Arc<Semaphore>,
        operator: Operator,
        table_schema: TableSchemaRef,
        push_down: &Option<PushDownInfo>,
    ) -> Result<SegmentPruner> {
        // Build limit pruner.
        // In case that limit is none, an unlimited limiter will be returned.
        let limit = push_down
            .as_ref()
            .filter(|p| p.order_by.is_empty() && p.filters.is_empty())
            .and_then(|p| p.limit);
        let limit_pruner = LimiterPrunerCreator::create(limit);

        Ok(SegmentPruner {
            func_ctx,
            pruning_runtime,
            pruning_semaphore,
            operator,
            table_schema,
            limit_pruner,
        })
    }

    pub async fn pruning(
        &self,
        segment_locs: Vec<Location>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        if segment_locs.is_empty() {
            return Ok(vec![]);
        }

        // Build pruning tasks.
        let mut segments = segment_locs.into_iter().enumerate();
        let pruning_tasks = std::iter::from_fn(|| {
            // pruning tasks are executed concurrently, check if limit exceeded before proceeding
            if self.limit_pruner.exceeded() {
                None
            } else {
                segments.next().map(|(_segment_idx, _segment_location)| {
                    move |_permit| async move { Self::prune_segment().await }
                })
            }
        });

        // Run tasks and collect the results.
        let join_handlers = self
            .pruning_runtime
            .try_spawn_batch_with_owned_semaphore(self.pruning_semaphore.clone(), pruning_tasks)
            .await?;

        let joint = future::try_join_all(join_handlers)
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

    async fn prune_segment() -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        todo!()
    }
}
