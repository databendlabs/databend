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

//! Receive side of broadcast and hash exchanges. Merges the blocks from its input
//! port (the local sender's own partition) with those from its InboundChannel.

use std::any::Any;
use std::sync::Arc;
use std::task::Poll;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::EventCause;
use databend_common_pipeline::core::ExecutorWaker;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use petgraph::graph::NodeIndex;

use super::block_coalescer::BlockCoalescer;
use super::block_coalescer::CoalesceThreshold;
use crate::servers::flight::v1::network::InboundChannel;
use crate::servers::flight::v1::network::SyncTaskHandle;
use crate::servers::flight::v1::network::SyncTaskSet;

pub struct ExchangeRecvTransform {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    id: NodeIndex,
    tasks: SyncTaskSet,
    receiver: Arc<dyn InboundChannel>,
    handle: Option<SyncTaskHandle<'static, Result<Option<DataBlock>>>>,
    coalescer: BlockCoalescer,
}

impl ExchangeRecvTransform {
    pub fn create_item(
        worker_id: usize,
        receiver: Arc<dyn InboundChannel>,
        waker: Arc<ExecutorWaker>,
        coalesce: Option<CoalesceThreshold>,
    ) -> PipeItem {
        let input = InputPort::create();
        let output = OutputPort::create();
        let processor = ProcessorPtr::create(Box::new(Self {
            receiver,
            input: input.clone(),
            output: output.clone(),
            tasks: SyncTaskSet::new(worker_id, waker),

            handle: None,
            id: Default::default(),
            coalescer: BlockCoalescer::create(coalesce),
        }));

        PipeItem::create(processor, vec![input], vec![output])
    }
}

impl Processor for ExchangeRecvTransform {
    fn name(&self) -> String {
        String::from("ExchangeRecvTransform")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event_with_cause(&mut self, cause: EventCause) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            self.receiver.close();

            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        loop {
            if let Some(data_block) = self.coalescer.pop() {
                self.output.push_data(Ok(data_block));
                return Ok(Event::NeedConsume);
            }

            if self.handle.is_none() && !self.receiver.is_closed() {
                let receiver = self.receiver.clone();
                let recv_fut = Box::pin(async move { receiver.recv().await });
                self.handle = Some(self.tasks.spawn(self.id, recv_fut));
            }

            if let Some(mut handle) = self.handle.take() {
                match handle.poll(matches!(cause, EventCause::Other)) {
                    Poll::Ready(Ok(None)) => {}
                    Poll::Ready(Ok(Some(data_block))) => {
                        self.coalescer.push(data_block)?;
                        continue;
                    }
                    Poll::Ready(Err(cause)) => {
                        self.receiver.close();
                        return Err(cause);
                    }
                    Poll::Pending => {
                        self.handle = Some(handle);
                    }
                };
            }

            if self.input.has_data() {
                let data_block = self.input.pull_data().unwrap()?;
                self.coalescer.push(data_block)?;
                continue;
            }

            if self.input.is_finished() && self.handle.is_none() {
                if self.coalescer.flush()? {
                    continue;
                }

                self.output.finish();
                self.receiver.close();
                return Ok(Event::Finished);
            }

            self.input.set_need_data();
            return Ok(Event::NeedData);
        }
    }

    fn details_status(&self) -> Option<String> {
        let (coalescing_rows, coalescing_bytes) = self.coalescer.buffered();
        Some(format!(
            "handle_pending={}, coalescing_rows={}, coalescing_bytes={}",
            self.handle.is_some(),
            coalescing_rows,
            coalescing_bytes,
        ))
    }

    fn set_id(&mut self, id: NodeIndex) {
        self.id = id;
    }
}
