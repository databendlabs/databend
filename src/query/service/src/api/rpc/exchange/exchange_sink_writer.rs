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

use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockMetaInfoDowncast;
use common_expression::DataBlock;
use common_metrics::transform::*;
use common_pipeline_core::processors::InputPort;
use common_pipeline_core::processors::Processor;
use common_pipeline_core::processors::ProcessorPtr;
use common_pipeline_core::PipeItem;
use common_pipeline_sinks::AsyncSink;
use common_pipeline_sinks::AsyncSinker;
use common_pipeline_sinks::Sink;
use common_pipeline_sinks::Sinker;

use crate::api::rpc::flight_client::FlightSender;
use crate::api::ExchangeSerializeMeta;
use crate::sessions::QueryContext;

pub struct ExchangeWriterSink {
    flight_sender: FlightSender,
    source: String,
    destination: String,
    fragment: usize,
}

impl ExchangeWriterSink {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        input: Arc<InputPort>,
        flight_sender: FlightSender,
        source_id: &str,
        destination_id: &str,
        fragment_id: usize,
    ) -> Box<dyn Processor> {
        AsyncSinker::create(input, ctx, ExchangeWriterSink {
            flight_sender,
            source: source_id.to_string(),
            destination: destination_id.to_string(),
            fragment: fragment_id,
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

        let mut bytes = 0;
        let count = serialize_meta.packet.len();
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
            metrics_inc_exchange_write_count(count);
            metrics_inc_exchange_write_bytes(bytes);
        }

        Ok(false)
    }

    fn details_status(&self) -> Option<String> {
        #[derive(Debug)]
        #[allow(dead_code)]
        struct Display {
            source: String,
            destination: String,
            fragment: usize,
        }

        Some(format!("{:?}", Display {
            source: self.source.clone(),
            destination: self.destination.clone(),
            fragment: self.fragment
        }))
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

pub fn create_writer_item(
    ctx: Arc<QueryContext>,
    exchange: FlightSender,
    ignore: bool,
    destination_id: &str,
    fragment_id: usize,
    source_id: &str,
) -> PipeItem {
    let input = InputPort::create();
    PipeItem::create(
        match ignore {
            true => ProcessorPtr::create(IgnoreExchangeSink::create(input.clone(), exchange)),
            false => ProcessorPtr::create(ExchangeWriterSink::create(
                ctx,
                input.clone(),
                exchange,
                source_id,
                destination_id,
                fragment_id,
            )),
        },
        vec![input],
        vec![],
    )
}
