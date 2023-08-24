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

use crate::api::rpc::exchange::serde::exchange_serializer::ExchangeSerializeMeta;
use crate::api::rpc::flight_client::FlightSender;

pub struct ExchangeWriterSink {
    flight_sender: FlightSender,
    ignore_exchange: bool,
}

impl ExchangeWriterSink {
    pub fn create(
        input: Arc<InputPort>,
        flight_sender: FlightSender,
        ignore_exchange: bool,
    ) -> Box<dyn Processor> {
        AsyncSinker::create(input, ExchangeWriterSink {
            flight_sender,
            ignore_exchange,
        })
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

        if self.ignore_exchange {
            return Ok(self.flight_sender.is_closed());
        }

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

pub fn create_writer_item(exchange: FlightSender, ignore: bool) -> PipeItem {
    let input = InputPort::create();
    PipeItem::create(
        ProcessorPtr::create(ExchangeWriterSink::create(input.clone(), exchange, ignore)),
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
