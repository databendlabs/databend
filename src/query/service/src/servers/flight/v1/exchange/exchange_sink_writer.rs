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

use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::PipeItem;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sinks::AsyncSink;
use databend_common_pipeline::sinks::AsyncSinker;
use databend_common_pipeline::sinks::Sink;
use databend_common_pipeline::sinks::Sinker;

use crate::servers::flight::v1::exchange::serde::ExchangeSerializeMeta;
use crate::servers::flight::FlightSender;

pub struct ExchangeWriterSink {
    flight_sender: FlightSender,
}

impl ExchangeWriterSink {
    pub fn create(input: Arc<InputPort>, flight_sender: FlightSender) -> Box<dyn Processor> {
        AsyncSinker::create(input, ExchangeWriterSink { flight_sender })
    }
}

#[async_trait::async_trait]
impl AsyncSink for ExchangeWriterSink {
    const NAME: &'static str = "ExchangeWriterSink";

    #[async_backtrace::framed]
    async fn on_finish(&mut self) -> Result<()> {
        self.flight_sender.close();
        Ok(())
    }

    #[async_backtrace::framed]
    async fn consume(&mut self, mut data_block: DataBlock) -> Result<bool> {
        let serialize_meta = match data_block.take_meta() {
            None => Err(ErrorCode::Internal(
                "ExchangeWriterSink only recv ExchangeSerializeMeta, but got none.",
            )),
            Some(block_meta) => ExchangeSerializeMeta::downcast_from(block_meta).ok_or_else(|| {
                ErrorCode::Internal("ExchangeWriterSink only recv ExchangeSerializeMeta")
            }),
        }?;

        let mut bytes = 0;
        for packet in serialize_meta.packet {
            bytes += packet.bytes_size();
            if let Err(error) = self.flight_sender.send(packet).await {
                if error.code() == ErrorCode::ABORTED_QUERY {
                    return Ok(true);
                }

                return Err(error);
            }
        }

        {
            Profile::record_usize_profile(ProfileStatisticsName::ExchangeBytes, bytes);
        }

        Ok(false)
    }
}

pub struct IgnoreExchangeSink {
    flight_sender: FlightSender,
}

impl IgnoreExchangeSink {
    pub fn create(input: Arc<InputPort>, flight_sender: FlightSender) -> Box<dyn Processor> {
        Sinker::create(input, IgnoreExchangeSink { flight_sender })
    }
}

impl Sink for IgnoreExchangeSink {
    const NAME: &'static str = "ExchangeWriterSink";

    fn on_finish(&mut self) -> Result<()> {
        self.flight_sender.close();
        Ok(())
    }

    fn consume(&mut self, _: DataBlock) -> Result<()> {
        Ok(())
    }
}

pub fn create_writer_item(exchange: FlightSender, ignore: bool) -> PipeItem {
    let input = InputPort::create();
    PipeItem::create(
        match ignore {
            true => ProcessorPtr::create(IgnoreExchangeSink::create(input.clone(), exchange)),
            false => ProcessorPtr::create(ExchangeWriterSink::create(input.clone(), exchange)),
        },
        vec![input],
        vec![],
    )
}
