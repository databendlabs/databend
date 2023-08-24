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

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockMetaInfoDowncast;
use common_expression::DataBlock;
use common_pipeline_core::pipe::PipeItem;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_pipeline_sinks::AsyncSink;
use common_pipeline_sinks::AsyncSinker;
use common_pipeline_sinks::Sink;
use common_pipeline_sinks::Sinker;

use crate::api::rpc::exchange::serde::exchange_serializer::ExchangeSerializeMeta;
use crate::api::rpc::flight_client::FlightSender;

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

    #[async_trait::unboxed_simple]
    #[async_backtrace::framed]
    async fn consume(&mut self, mut data_block: DataBlock) -> Result<bool> {
        let serialize_meta = match data_block.take_meta() {
            None => Err(ErrorCode::Internal(
                "ExchangeWriterSink only recv ExchangeSerializeMeta.",
            )),
            Some(block_meta) => ExchangeSerializeMeta::downcast_from(block_meta).ok_or(
                ErrorCode::Internal("ExchangeWriterSink only recv ExchangeSerializeMeta."),
            ),
        }?;

        for packet in serialize_meta.packet {
            if let Err(error) = self.flight_sender.send(packet).await {
                if error.code() == ErrorCode::ABORTED_QUERY {
                    return Ok(true);
                }

                return Err(error);
            }
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

pub fn create_writer_items(exchanges: Vec<FlightSender>, ignore: bool) -> Vec<PipeItem> {
    let mut items = Vec::with_capacity(exchanges.len());

    for exchange in exchanges {
        items.push(create_writer_item(exchange, ignore));
    }

    items
}
