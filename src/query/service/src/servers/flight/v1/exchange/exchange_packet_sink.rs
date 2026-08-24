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
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::ExecutionInfo;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::core::basic_callback;
use databend_common_pipeline::sinks::AsyncSink;
use databend_common_pipeline::sinks::AsyncSinker;

use super::serde::ExchangeSerializeMeta;
use crate::servers::flight::v1::network::BlockOutboundSet;
use crate::servers::flight::v1::network::SendOutcome;

pub struct ExchangePacketSink {
    outbound: Arc<BlockOutboundSet>,
    destination: usize,
    ignore_exchange: bool,
}

impl ExchangePacketSink {
    fn create(
        input: Arc<InputPort>,
        outbound: Arc<BlockOutboundSet>,
        destination: usize,
        ignore_exchange: bool,
    ) -> ProcessorPtr {
        ProcessorPtr::create(AsyncSinker::create(input, Self {
            outbound,
            destination,
            ignore_exchange,
        }))
    }
}

#[async_trait::async_trait]
impl AsyncSink for ExchangePacketSink {
    const NAME: &'static str = "ExchangePacketSink";

    async fn on_finish(&mut self) -> Result<()> {
        self.outbound.finish_producer().await
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
            let mut flight_data = FlightData::try_from(packet)?;
            let mut metadata = 0_u16.to_le_bytes().to_vec();
            metadata.extend_from_slice(&flight_data.app_metadata);
            flight_data.app_metadata = metadata.into();

            if self.outbound.send(0, self.destination, flight_data).await?
                == SendOutcome::ReceiverClosed
            {
                return Ok(true);
            }
        }

        Profile::record_usize_profile(ProfileStatisticsName::ExchangeBytes, bytes);
        Ok(false)
    }
}

pub fn create_packet_writer_item(
    outbound: Arc<BlockOutboundSet>,
    destination: usize,
    ignore_exchange: bool,
) -> PipeItem {
    let input = InputPort::create();
    PipeItem::create(
        ExchangePacketSink::create(input.clone(), outbound, destination, ignore_exchange),
        vec![input],
        vec![],
    )
}

pub fn install_packet_outbound_failure_handler(
    outbound: Arc<BlockOutboundSet>,
    pipeline: &mut Pipeline,
) {
    pipeline.lift_on_finished(basic_callback(move |info: &ExecutionInfo| {
        if let Err(cause) = &info.res {
            let cause = cause.clone();
            let outbound = outbound.clone();
            GlobalIORuntime::instance().spawn(async move {
                outbound.fail(cause).await;
            });
        }
        Ok(())
    }));
}
