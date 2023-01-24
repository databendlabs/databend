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

use common_base::runtime::Runtime;
use common_catalog::plan::PushDownInfo;
use common_exception::Result;
use common_expression::FunctionContext;
use storages_common_pruner::BlockMetaIndex;
use storages_common_pruner::LimiterPruner;
use storages_common_pruner::LimiterPrunerCreator;
use storages_common_table_meta::meta::BlockMeta;

/// Segment level pruning.
pub struct SegmentPruner {
    pub func_ctx: FunctionContext,
    pub pruning_runtime: Arc<Runtime>,
    pub limit_pruner: LimiterPruner,
}

impl SegmentPruner {
    pub fn create(
        func_ctx: FunctionContext,
        pruning_runtime: Arc<Runtime>,
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
            limit_pruner,
        })
    }

    pub fn pruning(&self) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        todo!()
    }
}
