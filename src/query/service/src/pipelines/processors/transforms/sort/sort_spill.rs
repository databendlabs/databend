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
use std::collections::VecDeque;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;

use super::SortSimpleState;

pub struct TransformSortSimpleWait {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data: VecDeque<DataBlock>,
    blocks: Vec<DataBlock>,
    state: Arc<SortSimpleState>,
}

impl TransformSortSimpleWait {
    pub fn new(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        state: Arc<SortSimpleState>,
    ) -> Self {
        Self {
            input,
            output,
            output_data: VecDeque::new(),
            blocks: Vec::new(),
            state,
        }
    }
}

#[async_trait::async_trait]
impl Processor for TransformSortSimpleWait {
    fn name(&self) -> String {
        "TransformSortSimpleWait".to_string()
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

        if let Some(data_block) = self.output_data.pop_front() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if self.input.has_data() {
            self.blocks.push(self.input.pull_data().unwrap()?);
            self.input.set_need_data();
            return Ok(Event::NeedData);
        }

        if self.input.is_finished() {
            if self.blocks.is_empty() {
                self.output.finish();
                return Ok(Event::Finished);
            }

            return if self.state.done.has_notified() {
                Ok(Event::Sync)
            } else {
                Ok(Event::Async)
            };
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        debug_assert!(!self.blocks.is_empty());
        self.output_data = VecDeque::from(std::mem::take(&mut self.blocks));
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        self.state.done.notified().await;
        self.output_data = VecDeque::from(std::mem::take(&mut self.blocks));
        Ok(())
    }
}
