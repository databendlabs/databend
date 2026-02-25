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

//! Broadcast receive source. Reads data from InboundChannels only (no input port).
//! Uses async_process to await data from ReceiversStream.

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
use futures::StreamExt;
use petgraph::graph::NodeIndex;

use super::receivers_stream::ReceiversStream;
use crate::servers::flight::v1::network::InboundChannel;
use crate::servers::flight::v1::network::SyncTaskHandle;
use crate::servers::flight::v1::network::SyncTaskSet;

pub struct BroadcastRecvTransform {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    id: NodeIndex,
    tasks: SyncTaskSet,
    data_stream: Option<ReceiversStream>,
    handle: Option<SyncTaskHandle<'static, (ReceiversStream, Option<Result<DataBlock>>)>>,
}

impl BroadcastRecvTransform {
    pub fn create_item(
        receivers: Vec<Arc<dyn InboundChannel>>,
        waker: Arc<ExecutorWaker>,
    ) -> PipeItem {
        let input = InputPort::create();
        let output = OutputPort::create();
        let processor = ProcessorPtr::create(Box::new(Self {
            input: input.clone(),
            output: output.clone(),
            tasks: SyncTaskSet::new(waker),
            data_stream: Some(ReceiversStream::new(receivers)),

            handle: None,
            id: Default::default(),
        }));

        PipeItem::create(processor, vec![input], vec![output])
    }
}

impl Processor for BroadcastRecvTransform {
    fn name(&self) -> String {
        String::from("BroadcastRecvTransform")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event_with_cause(&mut self, cause: EventCause) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            drop(self.handle.take());
            drop(self.data_stream.take());

            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if self.input.has_data() {
            let data_block = self.input.pull_data().unwrap()?;
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if let Some(mut stream) = self.data_stream.take() {
            self.handle = Some(self.tasks.spawn(
                self.id,
                Box::pin(async move {
                    let data_block = stream.next().await;
                    (stream, data_block)
                }),
            ));
        }

        if let Some(mut handle) = self.handle.take() {
            return match handle.poll(matches!(cause, EventCause::Other)) {
                Poll::Ready((_, None)) => {
                    if self.input.is_finished() {
                        self.output.finish();
                        return Ok(Event::Finished);
                    }

                    self.input.set_need_data();
                    Ok(Event::NeedData)
                }
                Poll::Ready((stream, Some(data_block))) => {
                    self.data_stream = Some(stream);
                    self.output.push_data(Ok(data_block?));
                    Ok(Event::NeedConsume)
                }
                Poll::Pending => {
                    self.handle = Some(handle);
                    self.input.set_need_data();

                    Ok(Event::NeedData)
                }
            };
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn set_id(&mut self, id: NodeIndex) {
        self.id = id;
    }
}
