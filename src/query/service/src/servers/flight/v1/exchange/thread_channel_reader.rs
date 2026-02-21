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

//! Merge processor that reads from an input port and multiple InboundChannels.
//!
//! Pure synchronous processor - no `async_process` needed.
//! Uses FlaggedWaker to poll a single BoxFuture from an async_event helper.
//! Scans receivers from first to last; finishes when input port and all receivers are done.

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::ExecutorWaker;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use futures::Stream;
use futures::StreamExt;
use futures_util::future::BoxFuture;
use petgraph::prelude::NodeIndex;

use crate::servers::flight::v1::network::FlaggedWaker;
use crate::servers::flight::v1::network::InboundChannel;

pub struct ThreadChannelReader {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    waker: Waker,

    remote_finished: bool,
    receivers_stream: ReceiversStream,

    next_data_future: Option<BoxFuture<'static, Option<Result<DataBlock>>>>,
}

impl ThreadChannelReader {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        receivers: Vec<Arc<dyn InboundChannel>>,
    ) -> ProcessorPtr {
        ProcessorPtr::create(Box::new(Self {
            input,
            output,
            next_data_future: None,
            remote_finished: false,
            waker: Waker::noop().clone(),
            receivers_stream: ReceiversStream::new(receivers),
        }))
    }

    pub fn create_item(receivers: Vec<Arc<dyn InboundChannel>>) -> PipeItem {
        let input = InputPort::create();
        let output = OutputPort::create();
        PipeItem::create(
            Self::create(input.clone(), output.clone(), receivers),
            vec![input],
            vec![output],
        )
    }
}

impl Processor for ThreadChannelReader {
    fn name(&self) -> String {
        String::from("ThreadChannelReader")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn on_id_set(&mut self, id: NodeIndex, waker: &Arc<ExecutorWaker>) {
        self.waker = FlaggedWaker::create(waker.to_waker(id, 0));
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.receivers_stream.close();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if !self.remote_finished && self.next_data_future.is_none() {
            let next_data: BoxFuture<'_, Option<Result<DataBlock>>> =
                Box::pin(self.receivers_stream.next());
            self.next_data_future = Some(unsafe {
                std::mem::transmute::<
                    BoxFuture<'_, Option<Result<DataBlock>>>,
                    BoxFuture<'static, Option<Result<DataBlock>>>,
                >(next_data)
            });
        }

        'remote_data: {
            if let Some(mut event_future) = self.next_data_future.take() {
                let mut cx = Context::from_waker(&self.waker);
                let Poll::Ready(data) = event_future.as_mut().poll(&mut cx) else {
                    self.next_data_future = Some(event_future);
                    break 'remote_data;
                };

                let Some(remote_data) = data else {
                    self.remote_finished = true;
                    break 'remote_data;
                };

                self.output.push_data(Ok(remote_data?));
                return Ok(Event::NeedConsume);
            }
        }

        if self.input.has_data() {
            self.output.push_data(self.input.pull_data().unwrap());
            return Ok(Event::NeedConsume);
        }

        // All receivers done
        if self.input.is_finished() && self.remote_finished {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        return Ok(Event::NeedData);
    }
}

struct ReceiversStream {
    finished: Vec<bool>,
    receivers: Vec<Arc<dyn InboundChannel>>,
    futures: Vec<BoxFuture<'static, std::result::Result<Option<DataBlock>, ErrorCode>>>,
}

impl ReceiversStream {
    fn new(receivers: Vec<Arc<dyn InboundChannel>>) -> Self {
        Self {
            futures: vec![],
            finished: vec![false; receivers.len()],
            receivers,
        }
    }

    pub fn close(&mut self) {
        for (idx, receiver) in self.receivers.iter().enumerate() {
            self.finished[idx] = true;
            receiver.close();
        }
    }
}

impl Stream for ReceiversStream {
    type Item = Result<DataBlock>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if this.futures.is_empty() {
            for receiver in this.receivers.iter() {
                this.futures.push(unsafe {
                    std::mem::transmute::<
                        Pin<Box<dyn Future<Output = Result<Option<DataBlock>>> + Send + '_>>,
                        Pin<Box<dyn Future<Output = Result<Option<DataBlock>>> + Send + 'static>>,
                    >(receiver.recv())
                });
            }
        }

        let mut all_finished = true;
        for idx in 0..this.receivers.len() {
            if this.finished[idx] {
                continue;
            }

            match this.futures[idx].as_mut().poll(cx) {
                Poll::Ready(Ok(Some(block))) => {
                    this.futures[idx] = unsafe {
                        std::mem::transmute::<
                            Pin<Box<dyn Future<Output = Result<Option<DataBlock>>> + Send + '_>>,
                            Pin<
                                Box<
                                    dyn Future<Output = Result<Option<DataBlock>>> + Send + 'static,
                                >,
                            >,
                        >(this.receivers[idx].recv())
                    };
                    return Poll::Ready(Some(Ok(block)));
                }
                Poll::Ready(Ok(None)) => {
                    this.finished[idx] = true;
                    continue;
                }
                Poll::Ready(Err(e)) => {
                    for idx in 0..this.receivers.len() {
                        this.finished[idx] = true;
                        this.receivers[idx].close();
                    }

                    return Poll::Ready(Some(Err(e)));
                }
                Poll::Pending => {
                    all_finished = false;
                    continue;
                }
            }
        }

        match all_finished {
            true => Poll::Ready(None),
            false => Poll::Pending,
        }
    }
}
