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

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::task::Poll;

use databend_common_base::runtime::GlobalIORuntime;
use databend_common_exception::Result;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::ExecutionInfo;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::SyncTaskHandle;
use databend_common_pipeline::core::SyncTaskSet;
use databend_common_pipeline::core::basic_callback;
use futures::future::BoxFuture;
use petgraph::prelude::NodeIndex;

use crate::servers::flight::v1::exchange::outbound_channel::DummyOutboundChannel;
use crate::servers::flight::v1::exchange::outbound_channel::OutboundChannel;
use crate::servers::flight::v1::transport::OutboundStreamRef;
use crate::servers::flight::v1::transport::StreamSendOutcome;

pub(super) type OutboundSendResult = (usize, Result<StreamSendOutcome>);
pub(super) type OutboundSendResults = Vec<OutboundSendResult>;
pub(super) type OutboundSendHandle = SyncTaskHandle<'static, OutboundSendResults>;
type OutboundFinishHandle = SyncTaskHandle<'static, Result<()>>;

pub(super) fn fail_streams_on_pipeline_error(
    streams: &[OutboundStreamRef],
    pipeline: &mut Pipeline,
) {
    let streams = streams.to_vec();
    pipeline.lift_on_finished(basic_callback(move |info: &ExecutionInfo| {
        if let Err(cause) = &info.res {
            let cause = cause.clone();
            let streams = streams.clone();
            GlobalIORuntime::instance().spawn(async move {
                futures::future::join_all(streams.iter().map(|stream| stream.fail(cause.clone())))
                    .await;
            });
        }
        Ok(())
    }));
}

struct ReliableCompletion {
    streams: Vec<OutboundStreamRef>,
    remaining_producers: AtomicUsize,
}

impl ReliableCompletion {
    fn finish(self: &Arc<Self>) -> BoxFuture<'static, Result<()>> {
        let completion = self.clone();
        Box::pin(async move {
            if completion
                .remaining_producers
                .fetch_sub(1, Ordering::AcqRel)
                != 1
            {
                return Ok(());
            }
            for result in
                futures::future::join_all(completion.streams.iter().map(|stream| stream.finish()))
                    .await
            {
                result?;
            }
            Ok(())
        })
    }

    fn install_failure_handler(&self, pipeline: &mut Pipeline) {
        fail_streams_on_pipeline_error(&self.streams, pipeline);
    }
}

#[derive(Clone)]
pub struct SharedOutboundChannels {
    channels: Vec<Arc<dyn OutboundChannel>>,
    completion: Option<Arc<ReliableCompletion>>,
}

impl SharedOutboundChannels {
    pub fn reliable(
        channels: Vec<Arc<dyn OutboundChannel>>,
        streams: Vec<OutboundStreamRef>,
        num_producers: usize,
    ) -> Self {
        Self {
            channels,
            completion: Some(Arc::new(ReliableCompletion {
                streams,
                remaining_producers: AtomicUsize::new(num_producers),
            })),
        }
    }

    /// Channels that need no completion handshake, such as a purely local exchange.
    pub fn immediate(channels: Vec<Arc<dyn OutboundChannel>>) -> Self {
        Self {
            channels,
            completion: None,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.channels.len()
    }

    pub fn install_failure_handler(&self, pipeline: &mut Pipeline) {
        if let Some(completion) = &self.completion {
            completion.install_failure_handler(pipeline);
        }
    }
}

pub(super) struct OutboundSendChannels {
    channels: Vec<Arc<dyn OutboundChannel>>,
    completion: Option<Arc<ReliableCompletion>>,
    finished: bool,
    finish_handle: Option<OutboundFinishHandle>,
}

impl OutboundSendChannels {
    pub(super) fn create(channels: SharedOutboundChannels) -> Self {
        Self {
            channels: channels.channels,
            completion: channels.completion,
            finished: false,
            finish_handle: None,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.channels.len()
    }

    pub(super) fn channel(&self, idx: usize) -> &Arc<dyn OutboundChannel> {
        &self.channels[idx]
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = (usize, &Arc<dyn OutboundChannel>)> {
        self.channels.iter().enumerate()
    }

    pub(super) fn is_closed(&self, idx: usize) -> bool {
        self.channels[idx].is_closed()
    }

    pub(super) fn all_closed(&self) -> bool {
        self.channels.iter().all(|ch| ch.is_closed())
    }

    pub(super) fn all_closed_except(&self, except_idx: usize) -> bool {
        self.channels
            .iter()
            .enumerate()
            .all(|(idx, ch)| idx == except_idx || ch.is_closed())
    }

    /// Renders closed-channel counts for `details_status`, e.g. `2/4, closed=[true, false, ...]`.
    pub(super) fn closed_summary(&self) -> String {
        let closed = self
            .channels
            .iter()
            .map(|ch| ch.is_closed())
            .collect::<Vec<_>>();

        format!(
            "{}/{}, closed={:?}",
            closed.iter().filter(|closed| **closed).count(),
            closed.len(),
            closed
        )
    }

    pub(super) fn close(&mut self, idx: usize) {
        if !self.channels[idx].is_closed() {
            let mut closed = DummyOutboundChannel::create();
            std::mem::swap(&mut self.channels[idx], &mut closed);
            closed.close();
        }
    }

    pub(super) fn poll_complete_event(
        &mut self,
        tasks: &SyncTaskSet,
        id: NodeIndex,
    ) -> Result<Event> {
        if self.finished {
            return Ok(Event::Finished);
        }

        let mut handle = match self.finish_handle.take() {
            Some(handle) => handle,
            None => {
                for idx in 0..self.channels.len() {
                    self.close(idx);
                }

                // Closing the channels is the whole completion for local and legacy groups.
                let Some(completion) = &self.completion else {
                    self.finished = true;
                    return Ok(Event::Finished);
                };

                tasks.spawn(id, completion.finish())
            }
        };

        match handle.poll(false) {
            Poll::Ready(result) => {
                self.finished = true;
                result.map(|_| Event::Finished)
            }
            Poll::Pending => {
                self.finish_handle = Some(handle);
                Ok(Event::NeedConsume)
            }
        }
    }

    pub(super) fn handle_send_results(&mut self, results: OutboundSendResults) -> Result<()> {
        for (idx, result) in results {
            match result {
                Ok(StreamSendOutcome::Accepted) => {}
                Ok(StreamSendOutcome::ConsumerClosed) => self.close(idx),
                Err(cause) => return Err(cause),
            }
        }

        Ok(())
    }
}
