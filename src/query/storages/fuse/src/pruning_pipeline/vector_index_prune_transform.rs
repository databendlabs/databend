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

// Logs from this module will show up as "[PROCESSOR-ASYNC-TASK] ...".
databend_common_tracing::register_module_tag!("[PROCESSOR-ASYNC-TASK]");

use std::sync::Arc;
use std::time::Instant;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_transforms::AsyncAccumulatingTransform;
use databend_common_pipeline_transforms::AsyncAccumulatingTransformer;
use databend_storages_common_pruner::BlockMetaIndex;
use databend_storages_common_table_meta::meta::BlockMeta;
use log::info;

use crate::pruning::VectorIndexPruner;
use crate::pruning_pipeline::block_prune_result_meta::BlockPruneResult;

// VectorIndexPruneTransform is a processor that will accumulate the block meta and not push to
// downstream until all data is received and pruned.
pub struct VectorIndexPruneTransform {
    vector_index_pruner: VectorIndexPruner,
    metas: Vec<(BlockMetaIndex, Arc<BlockMeta>)>,
}

#[async_trait::async_trait]
impl AsyncAccumulatingTransform for VectorIndexPruneTransform {
    const NAME: &'static str = "VectorIndexPruneTransform";

    async fn transform(&mut self, mut data: DataBlock) -> Result<Option<DataBlock>> {
        if let Some(ptr) = data.take_meta() {
            if let Some(meta) = BlockPruneResult::downcast_from(ptr) {
                self.metas.extend(meta.block_metas);
                return Ok(None);
            }
        }
        Err(ErrorCode::Internal(
            "Cannot downcast meta to BlockPruneResult",
        ))
    }

    async fn on_finish(&mut self, _output: bool) -> Result<Option<DataBlock>> {
        self.do_vector_index_prune().await
    }
}

impl VectorIndexPruneTransform {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        vector_index_pruner: VectorIndexPruner,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(AsyncAccumulatingTransformer::create(
            input,
            output,
            VectorIndexPruneTransform {
                vector_index_pruner,
                metas: vec![],
            },
        )))
    }

    async fn do_vector_index_prune(&self) -> Result<Option<DataBlock>> {
        let start = Instant::now();
        let pruned = self.vector_index_pruner.prune(self.metas.clone()).await?;
        let elapsed = start.elapsed().as_millis() as u64;
        info!("Vector index prune transform elapsed: {elapsed}");

        if pruned.is_empty() {
            Ok(None)
        } else {
            Ok(Some(DataBlock::empty_with_meta(BlockPruneResult::create(
                pruned,
            ))))
        }
    }
}
