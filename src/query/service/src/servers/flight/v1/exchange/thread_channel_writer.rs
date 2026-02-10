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

//! Sink processor that writes to an OutboundChannel.
//!
//! Pure synchronous processor - no `async_process` needed.
//! Uses FlaggedWaker to poll the BoxFuture from OutboundChannel::add_block.
//! When the send completes (or backpressure clears), the FlaggedWaker fires,
//! the executor re-schedules this processor, and the next `event()` call
//! completes the future.

use std::any::Any;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::task::Wake;
use std::task::Waker;

use databend_common_exception::Result;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use futures_util::future::BoxFuture;

use crate::servers::flight::v1::network::OutboundChannel;

/// Wraps an executor waker to also set an AtomicBool flag when woken.
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

/// Sink processor that sends DataBlocks through an OutboundChannel.
///
/// Pure synchronous: uses `event()` only, no `async_process()`.
///
/// `event()` polls the BoxFuture from `OutboundChannel::add_block` with a
/// FlaggedWaker. When backpressure clears, the FlaggedWaker fires, the executor
/// re-schedules this processor, and the next `event()` call completes the send.
pub struct ThreadChannelWriter {
    input: Arc<InputPort>,
    channel: Arc<dyn OutboundChannel>,

    waker: Waker,

    /// In-flight send future, stored across event() cycles.
    send_future: Option<BoxFuture<'static, Result<()>>>,
}

impl ThreadChannelWriter {
    pub fn create(
        input: Arc<InputPort>,
        channel: Arc<dyn OutboundChannel>,
        executor_waker: Waker,
    ) -> ProcessorPtr {
        let woken = Arc::new(AtomicBool::new(false));
        let flagged_waker = Waker::from(Arc::new(FlaggedWaker {
            inner: executor_waker,
            flag: woken.clone(),
        }));

        ProcessorPtr::create(Box::new(Self {
            input,
            channel,
            waker: flagged_waker,
            send_future: None,
        }))
    }

    pub fn create_item(channel: Arc<dyn OutboundChannel>, executor_waker: Waker) -> PipeItem {
        let input = InputPort::create();
        PipeItem::create(
            Self::create(input.clone(), channel, executor_waker),
            vec![input],
            vec![],
        )
    }

    /// Poll the in-flight send future. Returns Ready(Ok(())) when done,
    /// Ready(Err) on error, or Pending if still waiting.
    fn poll_send(&mut self) -> Poll<Result<()>> {
        let Some(future) = self.send_future.as_mut() else {
            return Poll::Ready(Ok(()));
        };

        let mut cx = Context::from_waker(&self.waker);
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(result) => {
                self.send_future = None;
                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Processor for ThreadChannelWriter {
    fn name(&self) -> String {
        String::from("ThreadChannelWriter")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        // First, try to complete any in-flight send
        if self.send_future.is_some() {
            match self.poll_send() {
                Poll::Ready(Ok(())) => {}
                Poll::Ready(Err(e)) => {
                    self.input.finish();
                    return Err(e);
                }
                Poll::Pending => return Ok(Event::NeedData),
            }
        }

        // No in-flight send. Check input.
        if self.input.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.input.has_data() {
            self.input.set_need_data();
            return Ok(Event::NeedData);
        }

        // Pull data and start sending
        let data = self.input.pull_data().unwrap()?;
        self.send_future = Some(self.channel.add_block(data));

        // Try to complete immediately
        match self.poll_send() {
            Poll::Ready(Ok(())) => {
                self.input.set_need_data();
                Ok(Event::NeedData)
            }
            Poll::Ready(Err(e)) => {
                self.input.finish();
                Err(e)
            }
            Poll::Pending => Ok(Event::NeedData),
        }
    }
}
