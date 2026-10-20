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
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::EventCause;
use databend_common_pipeline::core::ExecutorWaker;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use petgraph::graph::NodeIndex;

use super::hash_shuffle_writer::HashShuffleWriter;
use super::hash_shuffle_writer::ShuffleNode;
use crate::servers::flight::v1::scatter::FlightScatter;

/// Hash shuffle sender.
///
/// With an output port, the rows of its own partition are pushed to the output
/// instead of going through a channel. Without one, it is a sink that sends every
/// partition through channels.
pub struct HashSendTransform {
    input: Arc<InputPort>,
    output: Option<Arc<OutputPort>>,
    /// Partition of this sender; only used with an output port.
    local_pos: usize,
    writer: HashShuffleWriter,
}

impl HashSendTransform {
    pub fn create_item(
        worker_id: usize,
        local_pos: usize,
        scatter: Arc<Box<dyn FlightScatter>>,
        nodes: Vec<ShuffleNode>,
        waker: Arc<ExecutorWaker>,
        budget: usize,
    ) -> PipeItem {
        let output = OutputPort::create();
        let writer = HashShuffleWriter::create(worker_id, waker, scatter, nodes, budget);
        Self::create(Some(output.clone()), local_pos, writer, vec![output])
    }

    pub fn create_sink_item(
        worker_id: usize,
        scatter: Arc<Box<dyn FlightScatter>>,
        nodes: Vec<ShuffleNode>,
        waker: Arc<ExecutorWaker>,
        budget: usize,
    ) -> PipeItem {
        let writer = HashShuffleWriter::create(worker_id, waker, scatter, nodes, budget);
        Self::create(None, 0, writer, vec![])
    }

    fn create(
        output: Option<Arc<OutputPort>>,
        local_pos: usize,
        writer: HashShuffleWriter,
        outputs: Vec<Arc<OutputPort>>,
    ) -> PipeItem {
        let input = InputPort::create();
        let processor = ProcessorPtr::create(Box::new(Self {
            input: input.clone(),
            output,
            local_pos,
            writer,
        }));
        PipeItem::create(processor, vec![input], outputs)
    }

    fn finish(&mut self) -> Result<Event> {
        self.input.finish();
        if let Some(output) = &self.output {
            output.finish();
        }
        self.writer.close_all();
        Ok(Event::Finished)
    }
}

impl Processor for HashSendTransform {
    fn name(&self) -> String {
        match self.output {
            Some(_) => String::from("HashSendTransform"),
            None => String::from("HashSendSink"),
        }
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event_with_cause(&mut self, cause: EventCause) -> Result<Event> {
        if self
            .writer
            .poll_sending(matches!(cause, EventCause::Other))?
        {
            return Ok(Event::NeedConsume);
        }

        if self.output.as_ref().is_some_and(|x| x.is_finished()) {
            self.writer.close_partition(self.local_pos);
        }

        if self.writer.all_closed() {
            return self.finish();
        }

        if self.input.has_data() {
            let data_block = self.input.pull_data().unwrap()?;

            if self.writer.add_block(data_block)? {
                let output = self.output.as_ref().filter(|x| x.can_push());
                let own = output.map(|_| self.local_pos);
                let (own_block, pending) = self.writer.flush(own)?;

                let pushed = match (output, own_block) {
                    (Some(output), Some(block)) => {
                        output.push_data(Ok(block));
                        true
                    }
                    _ => false,
                };

                if pending || pushed {
                    return Ok(Event::NeedConsume);
                }
            }
        }

        if self.input.is_finished() {
            // The output is finished first, so the rows of the own partition go
            // through its local channel like every other partition.
            if let Some(output) = &self.output {
                output.finish();
            }

            if self.writer.flush(None)?.1 {
                return Ok(Event::NeedConsume);
            }

            return self.finish();
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn details_status(&self) -> Option<String> {
        Some(match self.output {
            Some(_) => format!(
                "local_pos={}, {}",
                self.local_pos,
                self.writer.details_status()
            ),
            None => self.writer.details_status(),
        })
    }

    fn set_id(&mut self, id: NodeIndex) {
        self.writer.set_id(id);
    }
}
