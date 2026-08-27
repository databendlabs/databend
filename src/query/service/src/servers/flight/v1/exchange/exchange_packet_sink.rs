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

use arrow_flight::FlightData;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sinks::AsyncSink;
use databend_common_pipeline::sinks::AsyncSinker;

use super::serde::ExchangeSerializeMeta;
use crate::servers::flight::v1::transport::OutboundStreamRef;
use crate::servers::flight::v1::transport::StreamSendOutcome;

pub struct ExchangePacketSink {
    stream: OutboundStreamRef,
    ignore_exchange: bool,
}

impl ExchangePacketSink {
    fn create(
        input: Arc<InputPort>,
        stream: OutboundStreamRef,
        ignore_exchange: bool,
    ) -> ProcessorPtr {
        ProcessorPtr::create(AsyncSinker::create(input, Self {
            stream,
            ignore_exchange,
        }))
    }
}

#[async_trait::async_trait]
impl AsyncSink for ExchangePacketSink {
    const NAME: &'static str = "ExchangePacketSink";

    async fn on_finish(&mut self) -> Result<()> {
        self.stream.finish().await
    }

    async fn consume(&mut self, mut data_block: DataBlock) -> Result<bool> {
        if self.ignore_exchange {
            return Ok(false);
        }

        let serialize_meta = data_block
            .take_meta()
            .and_then(ExchangeSerializeMeta::downcast_from)
            .ok_or_else(|| {
                ErrorCode::Internal("ExchangePacketSink only accepts ExchangeSerializeMeta")
            })?;

        let mut bytes = 0;
        for packet in serialize_meta.packet {
            bytes += packet.bytes_size();
            let flight_data = FlightData::try_from(packet)?;

            if self.stream.send(0, flight_data).await? == StreamSendOutcome::ConsumerClosed {
                return Ok(true);
            }
        }

        Profile::record_usize_profile(ProfileStatisticsName::ExchangeBytes, bytes);
        Ok(false)
    }
}

pub fn create_packet_writer_item(stream: OutboundStreamRef, ignore_exchange: bool) -> PipeItem {
    let input = InputPort::create();
    PipeItem::create(
        ExchangePacketSink::create(input.clone(), stream, ignore_exchange),
        vec![input],
        vec![],
    )
}
