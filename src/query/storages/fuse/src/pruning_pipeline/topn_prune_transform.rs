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

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_metrics::storage::metrics_inc_blocks_topn_pruning_after;
use databend_common_metrics::storage::metrics_inc_blocks_topn_pruning_before;
use databend_common_metrics::storage::metrics_inc_bytes_block_topn_pruning_after;
use databend_common_metrics::storage::metrics_inc_bytes_block_topn_pruning_before;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline_transforms::BlockMetaAccumulatingTransform;
use databend_common_pipeline_transforms::BlockMetaAccumulatingTransformer;
use databend_storages_common_pruner::BlockMetaIndex;
use databend_storages_common_pruner::TopNPruner;
use databend_storages_common_table_meta::meta::BlockMeta;

use crate::pruning::PruningContext;
use crate::pruning_pipeline::block_prune_result_meta::BlockPruneResult;

// TopNPruneTransform is a processor that will accumulate the block meta and not push to
// downstream until all data is received and pruned.
pub struct TopNPruneTransform {
    pruning_ctx: Arc<PruningContext>,
    topn_pruner: TopNPruner,
    metas: Vec<(BlockMetaIndex, Arc<BlockMeta>)>,
}

impl BlockMetaAccumulatingTransform<BlockPruneResult> for TopNPruneTransform {
    const NAME: &'static str = "TopNPruneTransform";

    fn transform(&mut self, data: BlockPruneResult) -> Result<Option<DataBlock>> {
        self.metas.extend(data.block_metas);
        Ok(None)
    }

    fn on_finish(&mut self, _output: bool) -> Result<Option<DataBlock>> {
        self.do_topn_prune()
    }
}

impl TopNPruneTransform {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        pruning_ctx: Arc<PruningContext>,
        topn_pruner: TopNPruner,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(
            BlockMetaAccumulatingTransformer::create(input, output, TopNPruneTransform {
                pruning_ctx,
                topn_pruner,
                metas: vec![],
            }),
        ))
    }
    fn do_topn_prune(&self) -> Result<Option<DataBlock>> {
        // Perf.
        {
            let block_size = self.metas.iter().map(|(_, m)| m.block_size).sum();
            metrics_inc_blocks_topn_pruning_before(self.metas.len() as u64);
            metrics_inc_bytes_block_topn_pruning_before(block_size);
            self.pruning_ctx
                .pruning_stats
                .set_blocks_topn_pruning_before(self.metas.len() as u64);
        }
        let pruned_metas = self
            .topn_pruner
            .prune(self.metas.clone())
            .unwrap_or_else(|_| self.metas.clone());

        // Perf.
        {
            let block_size = pruned_metas.iter().map(|(_, m)| m.block_size).sum();
            metrics_inc_blocks_topn_pruning_after(pruned_metas.len() as u64);
            metrics_inc_bytes_block_topn_pruning_after(block_size);
            self.pruning_ctx
                .pruning_stats
                .set_blocks_topn_pruning_after(pruned_metas.len() as u64);
        }

        if pruned_metas.is_empty() {
            Ok(None)
        } else {
            Ok(Some(DataBlock::empty_with_meta(BlockPruneResult::create(
                pruned_metas,
            ))))
        }
    }
}
