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
use std::task::Poll;

use databend_common_base::runtime::GlobalIORuntime;
use databend_common_exception::Result;
use databend_common_pipeline::core::ExecutionInfo;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::basic_callback;
use petgraph::prelude::NodeIndex;

use crate::servers::flight::v1::network::BlockOutboundSet;
use crate::servers::flight::v1::network::DummyOutboundChannel;
use crate::servers::flight::v1::network::OutboundChannel;
use crate::servers::flight::v1::network::SendOutcome;
use crate::servers::flight::v1::network::SyncTaskHandle;
use crate::servers::flight::v1::network::SyncTaskSet;

pub(super) type OutboundSendResult = (usize, Result<SendOutcome>);
pub(super) type OutboundSendResults = Vec<OutboundSendResult>;
pub(super) type OutboundSendHandle = SyncTaskHandle<'static, OutboundSendResults>;
type OutboundFinishHandle = SyncTaskHandle<'static, Result<()>>;

#[derive(Clone)]
pub(super) struct SharedOutboundChannels {
    channels: Vec<Arc<dyn OutboundChannel>>,
    outbounds: Option<Arc<BlockOutboundSet>>,
}

impl SharedOutboundChannels {
    pub(super) fn legacy(channels: Vec<Arc<dyn OutboundChannel>>) -> Self {
        Self {
            channels,
            outbounds: None,
        }
    }

    pub(super) fn reconnectable(
        channels: Vec<Arc<dyn OutboundChannel>>,
        outbounds: Arc<BlockOutboundSet>,
    ) -> Self {
        Self {
            channels,
            outbounds: Some(outbounds),
        }
    }

    pub(super) fn len(&self) -> usize {
        self.channels.len()
    }

    pub(super) fn install_failure_handler(&self, pipeline: &mut Pipeline) {
        let Some(outbounds) = self.outbounds.clone() else {
            return;
        };
        pipeline.lift_on_finished(basic_callback(move |info: &ExecutionInfo| {
            if let Err(cause) = &info.res {
                let cause = cause.clone();
                // Failure delivery is bounded protocol cleanup; the pipeline keeps its original
                // result instead of waiting for the peer to confirm closure.
                GlobalIORuntime::instance().spawn(async move {
                    outbounds.fail(cause).await;
                });
            }
            Ok(())
        }));
    }
}

pub(super) struct OutboundSendChannels {
    channels: Vec<Arc<dyn OutboundChannel>>,
    outbounds: Option<Arc<BlockOutboundSet>>,
    finished: bool,
    finish_handle: Option<OutboundFinishHandle>,
}

impl OutboundSendChannels {
    pub(super) fn create(channels: SharedOutboundChannels) -> Self {
        Self {
            channels: channels.channels,
            outbounds: channels.outbounds,
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

    pub(super) fn closed_status(&self) -> Vec<bool> {
        self.channels.iter().map(|ch| ch.is_closed()).collect()
    }

    pub(super) fn closed_count(&self) -> usize {
        self.channels.iter().filter(|ch| ch.is_closed()).count()
    }

    pub(super) fn close(&mut self, idx: usize) {
        if !self.channels[idx].is_closed() {
            let mut closed = DummyOutboundChannel::create();
            std::mem::swap(&mut self.channels[idx], &mut closed);
            closed.close();
        }
    }

    pub(super) fn poll_finish(
        &mut self,
        tasks: &SyncTaskSet,
        id: NodeIndex,
        reset: bool,
    ) -> Result<Poll<()>> {
        if self.finished {
            return Ok(Poll::Ready(()));
        }

        let Some(outbounds) = self.outbounds.clone() else {
            for idx in 0..self.channels.len() {
                self.close(idx);
            }
            self.finished = true;
            return Ok(Poll::Ready(()));
        };

        let mut handle = match self.finish_handle.take() {
            Some(handle) => handle,
            None => {
                for idx in 0..self.channels.len() {
                    self.close(idx);
                }
                tasks.spawn(
                    id,
                    Box::pin(async move { outbounds.finish_producer().await }),
                )
            }
        };

        match handle.poll(reset) {
            Poll::Ready(result) => {
                self.finished = true;
                result.map(|_| Poll::Ready(()))
            }
            Poll::Pending => {
                self.finish_handle = Some(handle);
                Ok(Poll::Pending)
            }
        }
    }

    pub(super) fn handle_send_results(&mut self, results: OutboundSendResults) -> Result<()> {
        for (idx, result) in results {
            match result {
                Ok(SendOutcome::Accepted) => {}
                Ok(SendOutcome::ReceiverClosed) => self.close(idx),
                Err(cause) => return Err(cause),
            }
        }

        Ok(())
    }
}
