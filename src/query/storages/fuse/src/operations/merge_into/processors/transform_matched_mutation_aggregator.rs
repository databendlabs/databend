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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;

use crate::operations::merge_into::mutator::MatchedAggregator;

enum State {
    Consume,
    Accumulate(DataBlock),
    Prepare,
    Output,
}

struct TransformMatchedMutationAggregator {
    inner: MatchedAggregator,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    state: State,
    tasks: VecDeque<DataBlock>,
}

impl TransformMatchedMutationAggregator {
    fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        inner: MatchedAggregator,
    ) -> Box<dyn Processor> {
        Box::new(Self {
            inner,
            input,
            output,
            state: State::Consume,
            tasks: VecDeque::new(),
        })
    }
}

#[async_trait::async_trait]
impl Processor for TransformMatchedMutationAggregator {
    fn name(&self) -> String {
        "MatchedAggregator".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        match self.state {
            State::Accumulate(_) | State::Prepare => return Ok(Event::Async),
            State::Output => {
                if !self.output.can_push() {
                    return Ok(Event::NeedConsume);
                }
                if let Some(task) = self.tasks.pop_front() {
                    self.output.push_data(Ok(task));
                    return Ok(Event::NeedConsume);
                }
                self.output.finish();
                return Ok(Event::Finished);
            }
            State::Consume => {}
        }

        if self.input.has_data() {
            let data = self.input.pull_data().ok_or_else(|| {
                ErrorCode::Internal("matched aggregator input reported data but returned none")
            })??;
            self.state = State::Accumulate(data);
            return Ok(Event::Async);
        }
        if self.input.is_finished() {
            self.state = State::Prepare;
            return Ok(Event::Async);
        }
        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Consume) {
            State::Accumulate(data) => self.inner.accumulate(data).await,
            State::Prepare => {
                self.tasks = self.inner.prepare_tasks().await?;
                self.state = State::Output;
                Ok(())
            }
            _ => Err(ErrorCode::Internal(
                "invalid matched aggregator async state",
            )),
        }
    }
}

impl MatchedAggregator {
    pub fn into_pipe_item(self) -> PipeItem {
        let input = InputPort::create();
        let output = OutputPort::create();
        let processor =
            TransformMatchedMutationAggregator::create(input.clone(), output.clone(), self);
        PipeItem::create(ProcessorPtr::create(processor), vec![input], vec![output])
    }
}
