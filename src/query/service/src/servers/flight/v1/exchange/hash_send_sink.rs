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
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use petgraph::graph::NodeIndex;

use super::hash_shuffle_writer::HashShuffleWriter;
use super::hash_shuffle_writer::ShuffleNode;
use crate::servers::flight::v1::scatter::FlightScatter;

/// Hash shuffle sender that delivers every partition through channels.
pub struct HashSendSink {
    input: Arc<InputPort>,
    writer: HashShuffleWriter,
}

impl HashSendSink {
    pub fn create_item(
        worker_id: usize,
        scatter: Arc<Box<dyn FlightScatter>>,
        nodes: Vec<ShuffleNode>,
        waker: Arc<ExecutorWaker>,
        budget: usize,
    ) -> PipeItem {
        let input = InputPort::create();
        let processor = ProcessorPtr::create(Box::new(Self {
            input: input.clone(),
            writer: HashShuffleWriter::create(worker_id, waker, scatter, nodes, budget),
        }));
        PipeItem::create(processor, vec![input], vec![])
    }
}

impl Processor for HashSendSink {
    fn name(&self) -> String {
        String::from("HashSendSink")
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

        if self.writer.all_closed() {
            self.input.finish();
            self.writer.stop_sending_all();
            return Ok(Event::Finished);
        }

        // Flush before taking more input until the writer is below its budget and
        // has at most one piece waiting per partition.
        loop {
            while self.writer.needs_flush() {
                // `None` sends every partition through channels, so no block is returned.
                let (_, sends_pending) = self.writer.flush(None)?;
                if sends_pending {
                    return Ok(Event::NeedConsume);
                }
            }

            if !self.input.has_data() {
                break;
            }
            let data_block = self.input.pull_data().unwrap()?;
            self.writer.add_block(data_block)?;
        }

        if self.input.is_finished() {
            while self.writer.has_queued() {
                let (_, sends_pending) = self.writer.flush(None)?;
                if sends_pending {
                    return Ok(Event::NeedConsume);
                }
            }

            self.input.finish();
            self.writer.stop_sending_all();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn details_status(&self) -> Option<String> {
        Some(self.writer.details_status())
    }

    fn set_id(&mut self, id: NodeIndex) {
        self.writer.set_id(id);
    }
}
