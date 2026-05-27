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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::servers::flight::v1::network::DummyOutboundChannel;
use crate::servers::flight::v1::network::OutboundChannel;
use crate::servers::flight::v1::network::SyncTaskHandle;

pub(super) type OutboundSendResult = (usize, Result<()>);
pub(super) type OutboundSendResults = Vec<OutboundSendResult>;
pub(super) type OutboundSendHandle = SyncTaskHandle<'static, OutboundSendResults>;

pub(super) struct OutboundSendChannels {
    channels: Vec<Arc<dyn OutboundChannel>>,
}

impl OutboundSendChannels {
    pub(super) fn create(channels: Vec<Arc<dyn OutboundChannel>>) -> Self {
        Self { channels }
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

    pub(super) fn close_all(&mut self) {
        for idx in 0..self.channels.len() {
            self.close(idx);
        }
    }

    pub(super) fn handle_send_results(&mut self, results: OutboundSendResults) -> Result<()> {
        for (idx, result) in results {
            match result {
                Ok(()) => {}
                Err(cause) if cause.code() == ErrorCode::ABORTED_QUERY => {
                    self.close(idx);
                }
                Err(cause) => return Err(cause),
            }
        }

        Ok(())
    }
}
