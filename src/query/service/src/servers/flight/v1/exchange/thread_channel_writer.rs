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

//! Shuffle-and-send processor that partitions blocks and routes them to
//! OutboundChannels (remote) or the output port (local).
//!
//! Uses FlaggedWaker to poll BoxFutures from OutboundChannel::add_block.
//! Uses BlockPartitionStream to batch rows per partition before sending.
//!
//! `event()` handles I/O (pull input, dispatch partitions, poll sends).
//! `process()` handles CPU work (scatter + partition stream).

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use databend_common_exception::Result;
use databend_common_expression::BlockPartitionStream;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::ExecutorWaker;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use futures_util::future::BoxFuture;
use petgraph::prelude::NodeIndex;

use crate::servers::flight::v1::network::FlaggedWaker;
use crate::servers::flight::v1::network::OutboundChannel;
use crate::servers::flight::v1::scatter::FlightScatter;

pub struct ThreadChannelWriter {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    local_pos: usize,
    /// One channel per partition. `channels[local_pos]` is None.
    channels: Vec<Arc<dyn OutboundChannel>>,

    scatter: Arc<Box<dyn FlightScatter>>,
    partition_stream: BlockPartitionStream,

    waker: Waker,

    /// Block pulled from input, waiting for `process()` to scatter/partition.
    input_block: Option<DataBlock>,
    output_block: Option<DataBlock>,

    /// In-flight send futures, one per remote partition being sent.
    send_futures: Vec<BoxFuture<'static, Result<()>>>,

    /// Whether input is done and we've finalized the partition stream.
    finalized: bool,
}

impl ThreadChannelWriter {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        channels: Vec<Arc<dyn OutboundChannel>>,
        local_pos: usize,
        scatter: Arc<Box<dyn FlightScatter>>,
        rows_threshold: usize,
        bytes_threshold: usize,
    ) -> ProcessorPtr {
        let scatter_size = channels.len();
        ProcessorPtr::create(Box::new(Self {
            input,
            output,
            channels,
            local_pos,
            scatter,
            partition_stream: BlockPartitionStream::create(
                rows_threshold,
                bytes_threshold,
                scatter_size,
            ),
            waker: Waker::noop().clone(),
            input_block: None,
            output_block: None,
            send_futures: Vec::new(),
            finalized: false,
        }))
    }

    pub fn create_item(
        channels: Vec<Arc<dyn OutboundChannel>>,
        local_pos: usize,
        scatter: Arc<Box<dyn FlightScatter>>,
        rows_threshold: usize,
        bytes_threshold: usize,
    ) -> PipeItem {
        let input = InputPort::create();
        let output = OutputPort::create();
        PipeItem::create(
            Self::create(
                input.clone(),
                output.clone(),
                channels,
                local_pos,
                scatter,
                rows_threshold,
                bytes_threshold,
            ),
            vec![input],
            vec![output],
        )
    }

    fn tiny_schedule_futures(&mut self) -> Result<Event> {
        if self.send_futures.is_empty() {
            return Ok(Event::NeedData);
        }

        let mut i = 0;
        let mut cx = Context::from_waker(&self.waker);
        while i < self.send_futures.len() {
            match self.send_futures[i].as_mut().poll(&mut cx) {
                Poll::Ready(Ok(())) => {
                    drop(self.send_futures.swap_remove(i));
                    // Don't increment i — swapped element needs polling too
                }
                Poll::Ready(Err(e)) => {
                    self.send_futures.clear();
                    return Err(e);
                }
                Poll::Pending => {
                    i += 1;
                }
            }
        }

        if self.send_futures.is_empty() {
            return Ok(Event::NeedData);
        }

        Ok(Event::NeedConsume)
    }
}

impl Processor for ThreadChannelWriter {
    fn name(&self) -> String {
        String::from("ThreadChannelWriter")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn on_id_set(&mut self, id: NodeIndex, waker: &Arc<ExecutorWaker>) {
        self.waker = FlaggedWaker::create(waker.to_waker(id, 0));
    }

    fn event(&mut self) -> Result<Event> {
        if !self.output.is_finished() && self.output.can_push() {
            if let Some(output_block) = self.output_block.take() {
                self.output.push_data(Ok(output_block));
                return Ok(Event::NeedConsume);
            }
        }

        if let Event::NeedConsume = self.tiny_schedule_futures()? {
            return Ok(Event::NeedConsume);
        }

        if self.output.is_finished() {
            let all_closed = self.channels.iter().all(|x| x.is_closed());

            if all_closed {
                self.input.finish();
                return Ok(Event::Finished);
            }
        }

        if self.input.is_finished() {
            if !self.finalized {
                self.finalized = true;
                return Ok(Event::Sync);
            }

            if self.output_block.is_none() {
                self.output.finish();
                return Ok(Event::Finished);
            }

            return Ok(Event::NeedConsume);
        }

        if self.input.has_data() {
            self.input_block = Some(self.input.pull_data().unwrap()?);
            return Ok(Event::Sync);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(block) = self.input_block.take() {
            match self.scatter.scatter_indices(&block)? {
                Some(indices) => {
                    let ready_partitions = self.partition_stream.partition(indices, block, true);

                    for (idx, ready_partition) in ready_partitions {
                        self.dispatch_block(idx, ready_partition);
                    }
                }
                None => {
                    // Broadcast mode: clone to all destinations
                    for idx in 0..self.channels.len() {
                        self.dispatch_block(idx, block.clone());
                    }
                }
            }
        } else if self.finalized {
            // Flush remaining buffered data from partition stream
            for idx in self.partition_stream.partition_ids() {
                if let Some(block) = self.partition_stream.finalize_partition(idx) {
                    self.dispatch_block(idx, block);
                }
            }
        }

        Ok(())
    }
}

impl ThreadChannelWriter {
    fn dispatch_block(&mut self, idx: usize, block: DataBlock) {
        // Fast path: local partition goes to output port
        if idx == self.local_pos && self.output_block.is_none() {
            self.output_block = Some(block);
            return;
        }

        // Remote: send via OutboundChannel with tiny future schedule
        let mut future = self.channels[idx].add_block(block);

        let mut cx = Context::from_waker(&self.waker);
        let schedule_future = Pin::new(&mut future);
        if schedule_future.poll(&mut cx).is_pending() {
            let box_future: BoxFuture<'static, Result<()>> = unsafe { std::mem::transmute(future) };
            self.send_futures.push(box_future);
        }
    }
}
