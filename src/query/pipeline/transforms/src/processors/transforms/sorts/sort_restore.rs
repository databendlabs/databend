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

use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;

use super::core::algorithm::SortAlgorithm;
use super::sort_spill::OutputData;
use super::sort_spill::SortSpill;
use super::Base;
use super::SortBound;
use super::SortBoundNext;
use super::SortCollectedMeta;
use crate::traits::DataBlockSpill;
use crate::HookTransform;
use crate::HookTransformer;
use crate::MemorySettings;

pub struct TransformSortRestore<A: SortAlgorithm, S: DataBlockSpill> {
    input: Vec<SortCollectedMeta>,
    output: Option<DataBlock>,

    /// If the next transform of current transform is [`super::transform_multi_sort_merge::MultiSortMergeProcessor`],
    /// we can generate and output the order column to avoid the extra converting in the next transform.
    remove_order_col: bool,
    memory_settings: MemorySettings,

    base: Base<S>,
    inner: Option<SortSpill<A, S>>,
}

impl<A, S> TransformSortRestore<A, S>
where
    A: SortAlgorithm + Send + 'static,
    S: DataBlockSpill,
{
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        base: Base<S>,
        output_order_col: bool,
        memory_settings: MemorySettings,
    ) -> Result<HookTransformer<Self>> {
        Ok(HookTransformer::new(input, output, Self {
            input: Vec::new(),
            output: None,
            remove_order_col: !output_order_col,
            base,
            inner: None,
            memory_settings,
        }))
    }
}

#[async_trait::async_trait]
impl<A, S> HookTransform for TransformSortRestore<A, S>
where
    A: SortAlgorithm + 'static,
    A::Rows: 'static,
    S: DataBlockSpill,
{
    const NAME: &'static str = "TransformSortRestore";

    fn on_input(&mut self, mut block: DataBlock) -> Result<()> {
        assert!(self.inner.is_none());
        let meta = block
            .take_meta()
            .and_then(SortCollectedMeta::downcast_from)
            .expect("require a SortCollectedMeta");
        self.input.push(meta);
        Ok(())
    }

    fn on_output(&mut self) -> Result<Option<DataBlock>> {
        Ok(self.output.take())
    }

    fn need_process(&self, input_finished: bool) -> Option<Event> {
        if input_finished && (self.inner.is_some() || !self.input.is_empty()) {
            Some(Event::Async)
        } else {
            None
        }
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        let spill_sort = match &mut self.inner {
            Some(inner) => inner,
            None => {
                debug_assert!(!self.input.is_empty());
                let sequences = self
                    .input
                    .iter_mut()
                    .flat_map(|meta| meta.sequences.drain(..))
                    .collect();

                let meta = self.input.pop().unwrap();
                self.input.clear();
                self.inner
                    .insert(SortSpill::from_meta(self.base.clone(), SortCollectedMeta {
                        sequences,
                        ..meta
                    }))
            }
        };

        let OutputData {
            block,
            bound: (bound_index, _),
            finish,
        } = spill_sort.on_restore(&self.memory_settings).await?;
        if let Some(block) = block {
            let mut block =
                block.add_meta(Some(SortBound::create(bound_index, SortBoundNext::More)))?;
            if self.remove_order_col {
                block.pop_columns(1);
            }
            self.output = Some(block);
        }
        if finish {
            self.inner = None;
        }
        Ok(())
    }
}

pub struct SortBoundEdge {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    data: Option<DataBlock>,
}

impl SortBoundEdge {
    pub fn new(input: Arc<InputPort>, output: Arc<OutputPort>) -> Self {
        Self {
            input,
            output,
            data: None,
        }
    }
}

impl Processor for SortBoundEdge {
    fn name(&self) -> String {
        String::from("SortBoundEdge")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    #[fastrace::trace(name = "SortBoundEdge::event")]
    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if self.data.is_none() {
            if self.input.is_finished() {
                self.output.finish();
                return Ok(Event::Finished);
            }
            let Some(block) = self.input.pull_data().transpose()? else {
                self.input.set_need_data();
                return Ok(Event::NeedData);
            };
            self.data = Some(block);
        }

        if self.input.is_finished() {
            let mut block = self.data.take().unwrap();
            let mut meta = block
                .take_meta()
                .and_then(SortBound::downcast_from)
                .expect("require a SortBound");
            meta.next = SortBoundNext::Last;
            self.output
                .push_data(Ok(block.add_meta(Some(meta.boxed()))?));
            self.output.finish();
            return Ok(Event::Finished);
        }

        let Some(incoming) = self.input.pull_data().transpose()? else {
            self.input.set_need_data();
            return Ok(Event::NeedData);
        };

        let incoming_index = incoming
            .get_meta()
            .and_then(SortBound::downcast_ref_from)
            .expect("require a SortBound")
            .index;

        let mut output = self.data.replace(incoming).unwrap();
        let output_meta = output
            .mut_meta()
            .and_then(SortBound::downcast_mut)
            .expect("require a SortBound");

        output_meta.next = SortBoundNext::Next(incoming_index);

        log::debug!(output_meta:? = output_meta; "output");

        self.output.push_data(Ok(output));
        Ok(Event::NeedConsume)
    }
}
