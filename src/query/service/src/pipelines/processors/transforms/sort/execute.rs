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
use databend_common_expression::DataSchemaRef;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_transforms::processors::sort::algorithm::SortAlgorithm;

use super::sort_spill::SortSpill;
use super::Base;
use super::SortCollectedMeta;
use crate::spillers::Spiller;

pub struct TransformSortExecute<A: SortAlgorithm> {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    /// If the next transform of current transform is [`super::transform_multi_sort_merge::MultiSortMergeProcessor`],
    /// we can generate and output the order column to avoid the extra converting in the next transform.
    remove_order_col: bool,

    base: Base,
    inner: Option<SortSpill<A>>,
}

impl<A> TransformSortExecute<A>
where A: SortAlgorithm
{
    pub(super) fn new(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        schema: DataSchemaRef,
        limit: Option<usize>,
        spiller: Arc<Spiller>,
        output_order_col: bool,
    ) -> Result<Self> {
        let sort_row_offset = schema.fields().len() - 1;
        Ok(Self {
            input,
            output,
            remove_order_col: !output_order_col,
            base: Base {
                schema,
                spiller,
                sort_row_offset,
                limit,
            },
            inner: None,
        })
    }

    fn output_block(&self, mut block: DataBlock) {
        if self.remove_order_col {
            block.pop_columns(1);
        }
        self.output.push_data(Ok(block));
    }
}

#[async_trait::async_trait]
impl<A> Processor for TransformSortExecute<A>
where
    A: SortAlgorithm + 'static,
    A::Rows: 'static,
{
    fn name(&self) -> String {
        "TransformSortExecute".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(mut block) = self.input.pull_data().transpose()? {
            assert!(self.inner.is_none());
            let meta = block
                .take_meta()
                .and_then(SortCollectedMeta::downcast_from)
                .expect("require a SortCollectedMeta");

            self.inner = Some(SortSpill::<A>::from_meta(self.base.clone(), meta));
            return Ok(Event::Async);
        }

        if self.input.is_finished() {
            Ok(Event::Async)
        } else {
            self.input.set_need_data();
            Ok(Event::NeedData)
        }
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        let Some(spill_sort) = &mut self.inner else {
            unreachable!()
        };
        let (block, finish) = spill_sort.on_restore().await?;
        if let Some(block) = block {
            assert!(!self.output.has_data());
            self.output_block(block);
        }
        if finish {
            self.output.finish();
        }
        Ok(())
    }
}
