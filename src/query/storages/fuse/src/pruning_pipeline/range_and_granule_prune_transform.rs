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

use std::any::Any;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;

use crate::pruning::BlockPruner;
use crate::pruning::RuntimeStatsPruner;
use crate::pruning_pipeline::RuntimeFilterPruneContext;
use crate::pruning_pipeline::block_metas_meta::BlockMetasMeta;
use crate::pruning_pipeline::block_prune_result_meta::BlockPruneResult;
use crate::pruning_pipeline::granule_prune_result_meta::GranulePruneResult;

enum State {
    Consume,
    WaitRuntimeFilter(BlockMetasMeta),
    Prune {
        meta: BlockMetasMeta,
        runtime_stats_pruner: Option<Arc<RuntimeStatsPruner>>,
    },
}

/// Waits for runtime statistics asynchronously, then runs range and granule
/// pruning synchronously on a pipeline executor thread.
pub struct RangeAndGranulePruneTransform {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    state: State,
    output_data: Option<DataBlock>,
    block_pruner: Arc<BlockPruner>,
    runtime_filter_prune_context: Option<RuntimeFilterPruneContext>,
    has_async_block_pruner: bool,
}

impl RangeAndGranulePruneTransform {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        block_pruner: Arc<BlockPruner>,
        runtime_filter_prune_context: Option<RuntimeFilterPruneContext>,
        has_async_block_pruner: bool,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(Self {
            input,
            output,
            state: State::Consume,
            output_data: None,
            block_pruner,
            runtime_filter_prune_context,
            has_async_block_pruner,
        })))
    }
}

#[async_trait::async_trait]
impl Processor for RangeAndGranulePruneTransform {
    fn name(&self) -> String {
        "RangeAndGranulePruneTransform".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if matches!(self.state, State::WaitRuntimeFilter(_)) {
            return Ok(Event::Async);
        }
        if matches!(self.state, State::Prune { .. }) {
            return Ok(Event::Sync);
        }

        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }
        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }
        if let Some(data) = self.output_data.take() {
            self.output.push_data(Ok(data));
            return Ok(Event::NeedConsume);
        }
        if self.input.has_data() {
            let mut data = self.input.pull_data().unwrap()?;
            let meta = data
                .take_meta()
                .and_then(BlockMetasMeta::downcast_from)
                .ok_or_else(|| ErrorCode::Internal("Cannot downcast meta to BlockMetasMeta"))?;
            self.state = if self.runtime_filter_prune_context.is_some() {
                State::WaitRuntimeFilter(meta)
            } else {
                State::Prune {
                    meta,
                    runtime_stats_pruner: None,
                }
            };
            return self.event();
        }
        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        let State::Prune {
            meta,
            runtime_stats_pruner,
        } = std::mem::replace(&mut self.state, State::Consume)
        else {
            return Err(ErrorCode::Internal(
                "RangeAndGranulePruneTransform entered an invalid sync state",
            ));
        };

        let blocks = self.block_pruner.range_and_granule_pruning(
            meta.segment_location,
            meta.block_metas,
            runtime_stats_pruner,
        )?;
        if !blocks.is_empty() {
            let output_meta = if self.has_async_block_pruner {
                GranulePruneResult::create(blocks)
            } else {
                BlockPruneResult::create(
                    blocks
                        .into_iter()
                        .map(|block| (block.block_meta_index, block.block_meta))
                        .collect(),
                )
            };
            self.output_data = Some(DataBlock::empty_with_meta(output_meta));
        }
        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        let State::WaitRuntimeFilter(meta) = std::mem::replace(&mut self.state, State::Consume)
        else {
            return Err(ErrorCode::Internal(
                "RangeAndGranulePruneTransform entered an invalid async state",
            ));
        };
        let runtime_stats_pruner = self
            .runtime_filter_prune_context
            .as_ref()
            .expect("state requires runtime filter context")
            .runtime_stats_pruner()
            .await?;
        self.state = State::Prune {
            meta,
            runtime_stats_pruner,
        };
        Ok(())
    }
}
