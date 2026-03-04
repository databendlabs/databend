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
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::EventCause;
use databend_common_pipeline::core::ExecutorWaker;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use petgraph::prelude::NodeIndex;

use crate::servers::flight::v1::network::DummyOutboundChannel;
use crate::servers::flight::v1::network::OutboundChannel;
use crate::servers::flight::v1::network::SyncTaskHandle;
use crate::servers::flight::v1::network::SyncTaskSet;

pub struct BroadcastSendTransform {
    id: NodeIndex,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    local_pos: usize,
    tasks: SyncTaskSet,
    channels: Vec<Arc<dyn OutboundChannel>>,
    handle: Option<SyncTaskHandle<'static, Result<Vec<()>>>>,
}

impl BroadcastSendTransform {
    pub fn create_item(
        worker_id: usize,
        local_pos: usize,
        channels: Vec<Arc<dyn OutboundChannel>>,
        waker: Arc<ExecutorWaker>,
    ) -> PipeItem {
        let input = InputPort::create();
        let output = OutputPort::create();

        let processor = ProcessorPtr::create(Box::new(Self {
            channels,
            local_pos,
            input: input.clone(),
            output: output.clone(),
            tasks: SyncTaskSet::new(worker_id, waker),

            handle: None,
            id: NodeIndex::default(),
        }));

        PipeItem::create(processor, vec![input], vec![output])
    }
}

impl Processor for BroadcastSendTransform {
    fn name(&self) -> String {
        String::from("BroadcastSendTransform")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event_with_cause(&mut self, cause: EventCause) -> Result<Event> {
        // Poll existing handle
        if let Some(mut handle) = self.handle.take() {
            match handle.poll(matches!(cause, EventCause::Other)) {
                Poll::Ready(Ok(_)) => {}
                Poll::Ready(Err(cause)) => {
                    return Err(cause);
                }
                Poll::Pending => {
                    self.handle = Some(handle);
                    return Ok(Event::NeedConsume);
                }
            };
        }

        if self.output.is_finished() {
            if self.channels.iter().all(|ch| ch.is_closed()) {
                self.input.finish();
                return Ok(Event::Finished);
            }
        }

        if self.input.has_data() {
            let data_block = self.input.pull_data().unwrap()?;

            let mut futures = Vec::new();

            for (idx, output_channel) in self.channels.iter().enumerate() {
                if idx == self.local_pos {
                    if self.output.is_finished() {
                        continue;
                    }

                    if self.output.can_push() {
                        self.output.push_data(Ok(data_block.clone()));
                        continue;
                    }
                }

                futures.push({
                    let data_block = data_block.clone();
                    let output_channel = output_channel.clone();
                    async move { output_channel.add_block(data_block).await }
                });
            }

            if !futures.is_empty() {
                let joined = Box::pin(futures::future::try_join_all(futures));
                let mut handle = self.tasks.spawn(self.id, joined);

                if matches!(
                    handle.poll(matches!(cause, EventCause::Other)),
                    Poll::Pending
                ) {
                    self.handle = Some(handle);
                    return Ok(Event::NeedConsume);
                }
            }

            if !self.output.is_finished() {
                return Ok(Event::NeedConsume);
            }
        }

        // Input finished → close channels
        if self.input.is_finished() {
            self.output.finish();

            for idx in 0..self.channels.len() {
                let mut closed_channel = DummyOutboundChannel::create();
                std::mem::swap(&mut self.channels[idx], &mut closed_channel);
                closed_channel.close();
            }

            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn details_status(&self) -> Option<String> {
        Some(format!(
            "BroadcastSendTransform {} {:?}",
            self.handle.is_some(),
            self.channels
                .iter()
                .map(|x| x.is_closed())
                .collect::<Vec<_>>()
        ))
    }

    fn set_id(&mut self, id: NodeIndex) {
        self.id = id;
    }
}
