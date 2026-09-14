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
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;

use crate::operations::common::MutationLogs;
use crate::operations::merge_into::mutator::BatchCompletion;
use crate::operations::merge_into::mutator::MatchedBlockMutationTask;
use crate::operations::merge_into::mutator::PreparedMatchedMutation;

enum State {
    Consume,
    Prepare(MatchedBlockMutationTask),
    Build(PreparedMatchedMutation, BatchCompletion),
    Output(DataBlock, BatchCompletion),
}

pub struct TransformMatchedBlockMutation {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    state: State,
}

impl TransformMatchedBlockMutation {
    pub fn into_pipe_item() -> PipeItem {
        let input = InputPort::create();
        let output = OutputPort::create();
        let processor = Box::new(Self {
            input: input.clone(),
            output: output.clone(),
            state: State::Consume,
        });
        PipeItem::create(ProcessorPtr::create(processor), vec![input], vec![output])
    }
}

#[async_trait::async_trait]
impl Processor for TransformMatchedBlockMutation {
    fn name(&self) -> String {
        "MatchedBlockMutationWorker".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        match &self.state {
            State::Prepare(_) => return Ok(Event::Async),
            State::Build(_, _) => return Ok(Event::Async),
            State::Output(_, _) => {
                if !self.output.can_push() {
                    return Ok(Event::NeedConsume);
                }
                let State::Output(data, completion) =
                    std::mem::replace(&mut self.state, State::Consume)
                else {
                    unreachable!()
                };
                self.output.push_data(Ok(data));
                completion.complete();
                return Ok(Event::NeedConsume);
            }
            State::Consume => {}
        }

        if self.input.has_data() {
            let data = self.input.pull_data().ok_or_else(|| {
                ErrorCode::Internal("matched block worker input reported data but returned none")
            })??;
            self.state = State::Prepare(MatchedBlockMutationTask::try_from(data)?);
            return Ok(Event::Async);
        }
        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }
        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        Err(ErrorCode::Internal(
            "matched block worker has no synchronous work",
        ))
    }

    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Consume) {
            State::Prepare(task) => {
                let (mutation, completion) = task.prepare().await?;
                match mutation {
                    Some(mutation) if mutation.needs_build() => {
                        self.state = State::Build(mutation, completion);
                    }
                    Some(mutation) => {
                        let (entry, logical_updated_rows, logical_deleted_rows) =
                            mutation.finish().await?;
                        self.state = State::Output(
                            DataBlock::empty_with_meta(Box::new(MutationLogs {
                                entries: vec![entry],
                                logical_updated_rows,
                                logical_deleted_rows,
                            })),
                            completion,
                        );
                    }
                    None => completion.complete(),
                }
            }
            State::Build(mutation, completion) => {
                let (entry, logical_updated_rows, logical_deleted_rows) = mutation.finish().await?;
                self.state = State::Output(
                    DataBlock::empty_with_meta(Box::new(MutationLogs {
                        entries: vec![entry],
                        logical_updated_rows,
                        logical_deleted_rows,
                    })),
                    completion,
                );
            }
            _ => {
                return Err(ErrorCode::Internal(
                    "matched block worker has no async work",
                ));
            }
        }
        Ok(())
    }
}
