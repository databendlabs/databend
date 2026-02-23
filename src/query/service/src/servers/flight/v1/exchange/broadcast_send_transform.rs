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

//! Broadcast send processor. Clones each input block to all OutboundChannels.
//!
//! Pure synchronous processor — no `async_process` needed.
//! Uses [`SyncTaskSet`] / [`SyncTaskHandle`] to poll channel sends from `event()`.

use std::any::Any;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::ExecutorWaker;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use petgraph::prelude::NodeIndex;

use crate::servers::flight::v1::network::OutboundChannel;
use crate::servers::flight::v1::network::SyncTaskHandle;
use crate::servers::flight::v1::network::SyncTaskSet;

pub struct BroadcastSendTransform {
    id: NodeIndex,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    local_idx: usize,
    channels: Vec<Arc<dyn OutboundChannel>>,
    tasks: SyncTaskSet,
    handle: Option<SyncTaskHandle<'static, Result<Vec<()>>>>,
}

impl BroadcastSendTransform {
    pub fn create_item(channels: Vec<Arc<dyn OutboundChannel>>, local_idx: usize) -> PipeItem {
        let input = InputPort::create();
        let output = OutputPort::create();
        let processor = ProcessorPtr::create(Box::new(Self {
            id: NodeIndex::default(),
            input: input.clone(),
            output: output.clone(),
            local_idx,
            channels,
            tasks: SyncTaskSet::new(ExecutorWaker::create()),
            handle: None,
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

    fn on_id_set(&mut self, id: NodeIndex, waker: &Arc<ExecutorWaker>) {
        self.id = id;
        self.tasks = SyncTaskSet::new(waker.clone());
    }

    fn event(&mut self) -> Result<Event> {
        // Poll existing handle
        if let Some(h) = &mut self.handle {
            match h.poll(true) {
                std::task::Poll::Ready(result) => {
                    result?;
                    self.handle = None;
                }
                std::task::Poll::Pending => {
                    return Ok(Event::NeedConsume);
                }
            }
        }

        // Downstream finished and all channels closed → done
        if self.output.is_finished() && self.channels.iter().all(|ch| ch.is_closed()) {
            self.input.finish();
            return Ok(Event::Finished);
        }

        // Input finished → close channels, finish output
        if self.input.is_finished() {
            for ch in &self.channels {
                ch.close();
            }

            self.output.finish();
            return Ok(Event::Finished);
        }

        // Pull data and broadcast
        if self.input.has_data() {
            let block = self.input.pull_data().unwrap()?;

            // Local channel: push to output port if possible
            if self.output.can_push() {
                self.output.push_data(Ok(block.clone()));
            }

            // Remote channels: collect futures for all non-local channels
            let mut futures = Vec::new();
            for (idx, ch) in self.channels.iter().enumerate() {
                if idx == self.local_idx && self.output.can_push() {
                    self.output.push_data(Ok(block.clone()));
                    continue;
                }

                futures.push({
                    let ch = ch.clone();
                    let block = block.clone();
                    async move { ch.add_block(block).await }
                });
            }

            if !futures.is_empty() {
                let joined = Box::pin(futures::future::try_join_all(futures));
                let handle = self.tasks.spawn(self.id, joined);
                if !handle.is_done() {
                    self.handle = Some(handle);
                }
            }

            return Ok(Event::NeedConsume);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }
}
