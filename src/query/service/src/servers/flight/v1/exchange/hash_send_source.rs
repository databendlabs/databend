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
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::EventCause;
use databend_common_pipeline::core::ExecutorWaker;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use petgraph::graph::NodeIndex;

use crate::servers::flight::v1::network::InboundChannel;
use crate::servers::flight::v1::network::SyncTaskHandle;
use crate::servers::flight::v1::network::SyncTaskSet;

pub struct HashSendSource {
    id: NodeIndex,
    output: Arc<OutputPort>,
    tasks: SyncTaskSet,
    receiver: Arc<dyn InboundChannel>,
    handle: Option<SyncTaskHandle<'static, Result<Option<DataBlock>>>>,
}

impl HashSendSource {
    pub fn create_item(
        worker_id: usize,
        receiver: Arc<dyn InboundChannel>,
        waker: Arc<ExecutorWaker>,
    ) -> PipeItem {
        let output = OutputPort::create();
        let processor = ProcessorPtr::create(Box::new(Self {
            receiver,
            output: output.clone(),
            tasks: SyncTaskSet::new(worker_id, waker),
            handle: None,
            id: NodeIndex::default(),
        }));

        PipeItem::create(processor, vec![], vec![output])
    }
}

impl Processor for HashSendSource {
    fn name(&self) -> String {
        String::from("HashSendSource")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event_with_cause(&mut self, cause: EventCause) -> Result<Event> {
        if self.output.is_finished() {
            self.receiver.close();

            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if self.handle.is_none() && !self.receiver.is_closed() {
            let receiver = self.receiver.clone();
            let recv_fut = Box::pin(async move { receiver.recv().await });
            self.handle = Some(self.tasks.spawn(self.id, recv_fut));
        }

        if let Some(mut handle) = self.handle.take() {
            match handle.poll(matches!(cause, EventCause::Other)) {
                Poll::Ready(Ok(None)) => {
                    self.output.finish();
                    return Ok(Event::Finished);
                }
                Poll::Ready(Ok(Some(data_block))) => {
                    self.output.push_data(Ok(data_block));
                    return Ok(Event::NeedConsume);
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

        if self.handle.is_none() {
            self.output.finish();
            self.receiver.close();
            return Ok(Event::Finished);
        }

        Ok(Event::NeedData)
    }

    fn set_id(&mut self, id: NodeIndex) {
        self.id = id;
    }
}
