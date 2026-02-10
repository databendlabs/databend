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

//! Source processor that reads from a NetworkInboundReceiver.
//!
//! Pure synchronous processor - no `async_process` needed.
//! Uses FlaggedWaker to poll a `RecvFuture` (which encapsulates the
//! try-listen-retry pattern from async_channel).
//!
//! The processor prioritizes consuming data from the network connection
//! with the highest memory usage (via NetworkInboundChannel's priority pop).

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;

use crate::servers::flight::v1::network::NetworkInboundReceiver;
use crate::servers::flight::v1::network::inbound_channel::RecvFuture;

/// Wraps an executor waker to also set an AtomicBool flag when woken.
/// This allows the processor's `event()` to detect that data has become available
/// without needing to poll the channel.
struct FlaggedWaker {
    inner: Waker,
    flag: Arc<AtomicBool>,
}

impl Wake for FlaggedWaker {
    fn wake(self: Arc<Self>) {
        self.flag.store(true, Ordering::Release);
        self.inner.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.flag.store(true, Ordering::Release);
        self.inner.wake_by_ref();
    }
}

/// Source processor that reads DataBlocks from a NetworkInboundReceiver.
///
/// Pure synchronous: uses `event()` + `process()` only, no `async_process()`.
///
/// `process()` polls a `RecvFuture` with a FlaggedWaker. The future
/// encapsulates the try-listen-retry pattern (same as async_channel's Recv).
/// When data arrives, the FlaggedWaker fires, executor re-schedules this
/// processor, and the next `process()` call completes the future.
///
/// Dictionaries are collected until a non-dictionary packet arrives,
/// then all packets are output together as `ExchangeDeserializeMeta`
/// (same pattern as `ExchangeSourceReader`).
pub struct ThreadChannelReader {
    output: Arc<OutputPort>,
    receiver: Arc<NetworkInboundReceiver>,

    waker: Waker,

    /// In-flight recv future, stored across event()/process() cycles.
    recv_future: Option<RecvFuture>,
}

impl ThreadChannelReader {
    pub fn create(
        output: Arc<OutputPort>,
        receiver: Arc<NetworkInboundReceiver>,
        executor_waker: Waker,
    ) -> ProcessorPtr {
        let woken = Arc::new(AtomicBool::new(false));
        let flagged_waker = Waker::from(Arc::new(FlaggedWaker {
            inner: executor_waker,
            flag: woken.clone(),
        }));

        ProcessorPtr::create(Box::new(Self {
            output,
            receiver,
            waker: flagged_waker,
            recv_future: None,
        }))
    }

    pub fn create_item(receiver: Arc<NetworkInboundReceiver>, executor_waker: Waker) -> PipeItem {
        let output = OutputPort::create();
        PipeItem::create(
            Self::create(output.clone(), receiver, executor_waker),
            vec![],
            vec![output],
        )
    }

    fn recv_fut(&mut self) -> RecvFuture {
        self.recv_future
            .take()
            .unwrap_or_else(|| self.receiver.recv())
    }

    fn recv_data(&mut self) -> Poll<Option<Result<DataBlock>>> {
        let mut future = self.recv_fut();

        let mut cx = Context::from_waker(&self.waker);
        match Pin::new(&mut future).poll(&mut cx) {
            Poll::Ready(data) => Poll::Ready(data),
            Poll::Pending => {
                self.recv_future = Some(future);
                Poll::Pending
            }
        }
    }
}

impl Processor for ThreadChannelReader {
    fn name(&self) -> String {
        String::from("ThreadChannelReader")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.receiver.close();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        match self.recv_data() {
            Poll::Pending => Ok(Event::NeedData),
            Poll::Ready(None) => {
                self.output.finish();
                Ok(Event::Finished)
            }
            Poll::Ready(Some(Err(error))) => {
                self.output.finish();
                self.receiver.close();
                Err(error)
            }
            Poll::Ready(Some(Ok(data_block))) => {
                self.output.push_data(Ok(data_block));
                Ok(Event::NeedConsume)
            }
        }
    }
}
