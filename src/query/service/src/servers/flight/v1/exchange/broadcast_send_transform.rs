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

use super::outbound_send_channels::OutboundSendChannels;
use super::outbound_send_channels::OutboundSendHandle;
use crate::servers::flight::v1::network::OutboundChannel;
use crate::servers::flight::v1::network::SyncTaskSet;

pub struct BroadcastSendTransform {
    id: NodeIndex,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    local_pos: usize,
    tasks: SyncTaskSet,
    channels: OutboundSendChannels,
    handle: Option<OutboundSendHandle>,
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
            channels: OutboundSendChannels::create(channels),
            local_pos,
            input: input.clone(),
            output: output.clone(),
            tasks: SyncTaskSet::new(worker_id, waker),

            handle: None,
            id: NodeIndex::default(),
        }));

        PipeItem::create(processor, vec![input], vec![output])
    }

    fn no_active_downstream(&self) -> bool {
        self.output.is_finished() && self.channels.all_closed_except(self.local_pos)
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
                Poll::Ready(results) => {
                    self.channels.handle_send_results(results)?;
                    if self.no_active_downstream() {
                        self.input.finish();
                        return Ok(Event::Finished);
                    }
                }
                Poll::Pending => {
                    self.handle = Some(handle);
                    return Ok(Event::NeedConsume);
                }
            };
        }

        if self.no_active_downstream() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if self.input.has_data() {
            let data_block = self.input.pull_data().unwrap()?;

            let mut futures = Vec::new();

            for (idx, output_channel) in self.channels.iter() {
                if output_channel.is_closed() {
                    continue;
                }

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
                    async move { (idx, output_channel.add_block(data_block).await) }
                });
            }

            if !futures.is_empty() {
                let joined = Box::pin(futures::future::join_all(futures));
                let mut handle = self.tasks.spawn(self.id, joined);

                match handle.poll(matches!(cause, EventCause::Other)) {
                    Poll::Ready(results) => {
                        self.channels.handle_send_results(results)?;
                        if self.no_active_downstream() {
                            self.input.finish();
                            return Ok(Event::Finished);
                        }
                    }
                    Poll::Pending => {
                        self.handle = Some(handle);
                        return Ok(Event::NeedConsume);
                    }
                }
            }

            if !self.output.is_finished() {
                return Ok(Event::NeedConsume);
            }
        }

        // Input finished → close channels
        if self.input.is_finished() {
            self.output.finish();

            self.channels.close_all();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn details_status(&self) -> Option<String> {
        Some(format!(
            "handle_pending={}, local_pos={}, closed_channels={}/{}, closed={:?}",
            self.handle.is_some(),
            self.local_pos,
            self.channels.closed_count(),
            self.channels.len(),
            self.channels.closed_status(),
        ))
    }

    fn set_id(&mut self, id: NodeIndex) {
        self.id = id;
    }
}
