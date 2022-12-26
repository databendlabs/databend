// Copyright 2022 Datafuse Labs.
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

use async_channel::Receiver;
use common_exception::Result;
use common_expression::Chunk;
use common_expression::DataSchemaRef;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;

pub struct TransformMergeChunk {
    finished: bool,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    input_data: Option<Chunk>,
    output_data: Option<Chunk>,
    schema: DataSchemaRef,
    pairs: Vec<(String, String)>,

    receiver: Receiver<Chunk>,
    receiver_result: Option<Chunk>,
}

impl TransformMergeChunk {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        schema: DataSchemaRef,
        pairs: Vec<(String, String)>,
        receiver: Receiver<Chunk>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(TransformMergeChunk {
            finished: false,
            input,
            output,
            input_data: None,
            output_data: None,
            schema,
            pairs,
            receiver,
            receiver_result: None,
        })))
    }

    fn project_chunk(&self, chunk: Chunk, is_left: bool) -> Result<Chunk> {
        let num_rows = chunk.num_rows();
        let columns = self
            .pairs
            .iter()
            .map(|(left, right)| {
                if is_left {
                    Ok(chunk.get_by_offset(self.schema.index_of(left)?).clone())
                } else {
                    Ok(chunk.get_by_offset(self.schema.index_of(right)?).clone())
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Chunk::new(columns, num_rows))
    }
}

#[async_trait::async_trait]
impl Processor for TransformMergeChunk {
    fn name(&self) -> String {
        "TransformMergeChunk".to_string()
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

        if let Some(output_data) = self.output_data.take() {
            self.output.push_data(Ok(output_data));
            return Ok(Event::NeedConsume);
        }

        if self.input_data.is_some() || self.receiver_result.is_some() {
            return Ok(Event::Sync);
        }

        if let Ok(result) = self.receiver.try_recv() {
            self.receiver_result = Some(result);
            return Ok(Event::Sync);
        }

        if self.input.is_finished() {
            if !self.finished {
                return Ok(Event::Async);
            }
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.input.has_data() {
            self.input_data = Some(self.input.pull_data().unwrap()?);
            return Ok(Event::Sync);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(input_data) = self.input_data.take() {
            if let Some(receiver_result) = self.receiver_result.take() {
                self.output_data = Some(Chunk::concat(&[
                    self.project_chunk(input_data, true)?,
                    self.project_chunk(receiver_result, false)?,
                ])?);
            } else {
                self.output_data = Some(self.project_chunk(input_data, true)?);
            }
        } else if let Some(receiver_result) = self.receiver_result.take() {
            self.output_data = Some(self.project_chunk(receiver_result, false)?);
        }

        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        if !self.finished {
            if let Ok(result) = self.receiver.recv().await {
                self.receiver_result = Some(result);
                return Ok(());
            }
            self.finished = true;
        }
        Ok(())
    }
}
