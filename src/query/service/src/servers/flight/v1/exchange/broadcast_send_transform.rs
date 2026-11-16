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

use databend_common_exception::ErrorCode;
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

use crate::servers::flight::v1::network::SyncTaskHandle;
use crate::servers::flight::v1::network::SyncTaskSet;
use crate::servers::flight::v1::network::outbound::BlockSender;

type BroadcastSendResults = Vec<(usize, Result<()>)>;

pub struct BroadcastSendTransform {
    id: NodeIndex,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    local_pos: usize,
    tasks: SyncTaskSet,
    senders: Vec<Option<Arc<dyn BlockSender>>>,
    handle: Option<SyncTaskHandle<'static, BroadcastSendResults>>,
}

impl BroadcastSendTransform {
    pub fn create_item(
        worker_id: usize,
        local_pos: usize,
        senders: Vec<Arc<dyn BlockSender>>,
        waker: Arc<ExecutorWaker>,
    ) -> PipeItem {
        let input = InputPort::create();
        let output = OutputPort::create();

        let processor = ProcessorPtr::create(Box::new(Self {
            senders: senders.into_iter().map(Some).collect(),
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
        self.output.is_finished()
            && self.senders.iter().enumerate().all(|(idx, sender)| {
                idx == self.local_pos || sender.as_ref().is_none_or(|sender| sender.is_closed())
            })
    }

    fn handle_send_results(&mut self, results: BroadcastSendResults) -> Result<()> {
        for (idx, result) in results {
            match result {
                Ok(()) => {}
                Err(cause) if cause.code() == ErrorCode::ABORTED_QUERY => {
                    // Release this sender's buffer reference so the remaining
                    // destinations can receive EOF before the pipeline is dropped.
                    self.senders[idx].take();
                }
                Err(cause) => return Err(cause),
            }
        }
        Ok(())
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
                    self.handle_send_results(results)?;
                    if self.no_active_downstream() {
                        self.input.finish();
                        self.senders.fill(None);
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
            self.senders.fill(None);
            return Ok(Event::Finished);
        }

        if self.input.has_data() {
            let data_block = self.input.pull_data().unwrap()?;

            let mut futures = Vec::new();

            for (idx, sender) in self.senders.iter().enumerate() {
                let Some(sender) = sender else {
                    continue;
                };
                if sender.is_closed() {
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
                    let sender = sender.clone();
                    async move { (idx, sender.send_block(data_block).await) }
                });
            }

            if !futures.is_empty() {
                let joined = Box::pin(futures::future::join_all(futures));
                let mut handle = self.tasks.spawn(self.id, joined);

                match handle.poll(matches!(cause, EventCause::Other)) {
                    Poll::Ready(results) => {
                        self.handle_send_results(results)?;
                        if self.no_active_downstream() {
                            self.input.finish();
                            self.senders.fill(None);
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

        // Input finished: release this processor's sender references.
        if self.input.is_finished() {
            self.output.finish();

            self.senders.fill(None);
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn details_status(&self) -> Option<String> {
        let closed = self
            .senders
            .iter()
            .map(|sender| sender.as_ref().is_none_or(|sender| sender.is_closed()))
            .collect::<Vec<_>>();
        Some(format!(
            "handle_pending={}, local_pos={}, closed_senders={}/{}, closed={:?}",
            self.handle.is_some(),
            self.local_pos,
            closed.iter().filter(|closed| **closed).count(),
            self.senders.len(),
            closed,
        ))
    }

    fn set_id(&mut self, id: NodeIndex) {
        self.id = id;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::servers::flight::v1::network::inbound::ExchangeReceiveChannels;
    use crate::servers::flight::v1::network::outbound::create_local_block_senders;

    #[test]
    fn test_broadcast_releases_stopped_senders_before_processor_drop() -> Result<()> {
        let receive_channels = ExchangeReceiveChannels::new(2);
        let senders = create_local_block_senders(&receive_channels);
        let receivers = receive_channels.receivers.clone();
        drop(receive_channels);
        let mut transform = BroadcastSendTransform {
            id: NodeIndex::default(),
            input: InputPort::create(),
            output: OutputPort::create(),
            local_pos: 0,
            tasks: SyncTaskSet::new(0, ExecutorWaker::create()),
            senders: senders
                .into_iter()
                .map(|sender| Some(sender as Arc<dyn BlockSender>))
                .collect(),
            handle: None,
        };

        transform
            .handle_send_results(vec![(1, Err(ErrorCode::AbortedQuery("receiver closed")))])?;
        assert!(receivers[1].is_closed());
        assert!(!receivers[0].is_closed());

        transform.input.finish();
        assert!(matches!(
            transform.event_with_cause(EventCause::Other)?,
            Event::Finished
        ));
        assert!(receivers.iter().all(|receiver| receiver.is_closed()));
        Ok(())
    }
}
