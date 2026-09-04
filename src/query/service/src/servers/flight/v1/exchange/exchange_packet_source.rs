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
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::EventCause;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;

use super::serde::ExchangeDeserializeMeta;
use crate::servers::flight::v1::network::NetworkInboundChannel;
use crate::servers::flight::v1::network::inbound_channel::strip_tid;
use crate::servers::flight::v1::network::inbound_quota::QueueItem;
use crate::servers::flight::v1::packets::DataPacket;

pub struct ExchangePacketSource {
    finished: AtomicBool,
    output: Arc<OutputPort>,
    output_data: Vec<DataPacket>,
    channel: Arc<NetworkInboundChannel>,
}

impl ExchangePacketSource {
    fn create(output: Arc<OutputPort>, channel: Arc<NetworkInboundChannel>) -> ProcessorPtr {
        ProcessorPtr::create(Box::new(Self {
            output,
            channel,
            finished: AtomicBool::new(false),
            output_data: vec![],
        }))
    }

    fn close(&self) {
        self.channel.receiver.close();
        while self.channel.receiver.try_recv().is_ok() {}
    }
}

#[async_trait::async_trait]
impl Processor for ExchangePacketSource {
    fn name(&self) -> String {
        String::from("ExchangePacketSource")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.finished.load(Ordering::SeqCst) {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.output.is_finished() {
            if !self.finished.swap(true, Ordering::SeqCst) {
                self.close();
            }
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if !self.output_data.is_empty() {
            let packets = std::mem::take(&mut self.output_data);
            self.output.push_data(Ok(DataBlock::empty_with_meta(
                ExchangeDeserializeMeta::create(packets),
            )));
        }

        Ok(Event::Async)
    }

    fn un_reacted(&self, cause: EventCause, _id: usize) -> Result<()> {
        if matches!(cause, EventCause::Output(_)) && self.output.is_finished() {
            self.close();
        }
        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        if self.output_data.is_empty() {
            let mut dictionaries = Vec::new();
            while let Some(item) = self.channel.recv_raw().await? {
                let QueueItem::RemoteData(item) = item else {
                    return Err(ErrorCode::Internal(
                        "ExchangePacketSource received a local block on a network channel",
                    ));
                };
                let packet = DataPacket::try_from(strip_tid(item.into_data()))?;
                let is_dictionary = matches!(&packet, DataPacket::Dictionary(_));
                dictionaries.push(packet);
                if !is_dictionary {
                    self.output_data = dictionaries;
                    return Ok(());
                }
            }
        }

        if !self.finished.swap(true, Ordering::SeqCst) {
            self.close();
        }
        Ok(())
    }
}

pub fn create_packet_reader_item(channel: Arc<NetworkInboundChannel>) -> PipeItem {
    let output = OutputPort::create();
    PipeItem::create(
        ExchangePacketSource::create(output.clone(), channel),
        vec![],
        vec![output],
    )
}
