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
use std::task::Poll;

use databend_common_exception::Result;
use databend_common_expression::BlockPartitionStream;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::EventCause;
use databend_common_pipeline::core::ExecutorWaker;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use petgraph::graph::NodeIndex;

use crate::servers::flight::v1::network::DummyOutboundChannel;
use crate::servers::flight::v1::network::OutboundChannel;
use crate::servers::flight::v1::network::SyncTaskHandle;
use crate::servers::flight::v1::network::SyncTaskSet;
use crate::servers::flight::v1::scatter::FlightScatter;

pub struct HashSendSink {
    id: NodeIndex,
    input: Arc<InputPort>,
    scatter: Arc<Box<dyn FlightScatter>>,
    partition_stream: BlockPartitionStream,
    tasks: SyncTaskSet,
    channels: Vec<Arc<dyn OutboundChannel>>,
    handle: Option<SyncTaskHandle<'static, Result<Vec<()>>>>,
}

impl HashSendSink {
    pub fn create_item(
        worker_id: usize,
        scatter: Arc<Box<dyn FlightScatter>>,
        channels: Vec<Arc<dyn OutboundChannel>>,
        waker: Arc<ExecutorWaker>,
        rows_threshold: usize,
        bytes_threshold: usize,
    ) -> PipeItem {
        let input = InputPort::create();
        let scatter_size = channels.len();
        let processor = ProcessorPtr::create(Box::new(Self {
            scatter,
            channels,
            input: input.clone(),
            tasks: SyncTaskSet::new(worker_id, waker),
            partition_stream: BlockPartitionStream::create(
                rows_threshold,
                bytes_threshold,
                scatter_size,
            ),
            handle: None,
            id: NodeIndex::default(),
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
        // Poll existing handle
        if let Some(mut handle) = self.handle.take() {
            match handle.poll(matches!(cause, EventCause::Other)) {
                Poll::Ready(Ok(_)) => {}
                Poll::Ready(Err(cause)) => return Err(cause),
                Poll::Pending => {
                    self.handle = Some(handle);
                    return Ok(Event::NeedConsume);
                }
            };
        }

        if self.input.has_data() {
            let data_block = self.input.pull_data().unwrap()?;

            if let Some(indices) = self.scatter.scatter_indices(&data_block)? {
                let ready_blocks = self.partition_stream.partition(indices, data_block, true);

                let mut futures = Vec::new();

                for (partition_id, block) in ready_blocks {
                    if block.is_empty() {
                        continue;
                    }

                    futures.push({
                        let channel = self.channels[partition_id].clone();
                        async move { channel.add_block(block).await }
                    });
                }

                if !futures.is_empty() {
                    let joined = Box::pin(futures::future::try_join_all(futures));
                    let mut handle = self.tasks.spawn(self.id, joined);

                    if matches!(handle.poll(true), Poll::Pending) {
                        self.handle = Some(handle);
                        return Ok(Event::NeedConsume);
                    }
                }
            }
        }

        if self.input.is_finished() {
            let mut futures = Vec::new();

            for partition_id in 0..self.channels.len() {
                if let Some(block) = self.partition_stream.finalize_partition(partition_id) {
                    if block.is_empty() {
                        continue;
                    }

                    futures.push({
                        let channel = self.channels[partition_id].clone();
                        async move { channel.add_block(block).await }
                    });
                }
            }

            if futures.is_empty() {
                for idx in 0..self.channels.len() {
                    let mut closed = DummyOutboundChannel::create();
                    std::mem::swap(&mut self.channels[idx], &mut closed);
                    closed.close();
                }

                return Ok(Event::Finished);
            }

            let joined = Box::pin(futures::future::try_join_all(futures));
            let mut handle = self.tasks.spawn(self.id, joined);

            if matches!(handle.poll(true), Poll::Pending) {
                self.handle = Some(handle);
                return Ok(Event::NeedConsume);
            }

            for idx in 0..self.channels.len() {
                let mut closed = DummyOutboundChannel::create();
                std::mem::swap(&mut self.channels[idx], &mut closed);
                closed.close();
            }

            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn set_id(&mut self, id: NodeIndex) {
        self.id = id;
    }
}
