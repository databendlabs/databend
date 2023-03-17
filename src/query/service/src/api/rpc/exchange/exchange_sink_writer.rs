// Copyright 2023 Datafuse Labs.
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
use tracing::info;

use crate::api::rpc::exchange::serde::exchange_serializer::ExchangeSerializeMeta;
use crate::api::rpc::flight_client::FlightExchangeRef;

pub struct ExchangeWriterSink {
    query_id: String,
    fragment: usize,
    exchange: FlightExchangeRef,
}

impl ExchangeWriterSink {
    pub fn create(
        input: Arc<InputPort>,
        flight_exchange: FlightExchangeRef,
        query_id: String,
        fragment: usize,
    ) -> Box<dyn Processor> {
        AsyncSinker::create(input, ExchangeWriterSink {
            query_id,
            fragment,
            exchange: flight_exchange,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSink for ExchangeWriterSink {
    const NAME: &'static str = "ExchangeWriterSink";

    async fn on_start(&mut self) -> Result<()> {
        info!(
            "Start query:{:?}, fragment:{:?} exchange write.",
            self.query_id, self.fragment
        );

        let res = self.exchange.close_input().await;
        info!(
            "Started query:{:?}, fragment:{:?} exchange write. {}",
            self.query_id, self.fragment, res
        );
        Ok(())
    }

    async fn on_finish(&mut self) -> Result<()> {
        info!(
            "Finish query:{:?}, fragment:{:?} exchange write.",
            self.query_id, self.fragment
        );

        let res = self.exchange.close_output().await;
        info!(
            "Finished query:{:?}, fragment:{:?} exchange write. {}",
            self.query_id, self.fragment, res
        );
        Ok(())
    }

    #[async_trait::unboxed_simple]
    async fn consume(&mut self, mut data_block: DataBlock) -> Result<bool> {
        let mut serialize_meta = match data_block.take_meta() {
            None => Err(ErrorCode::Internal(
                "ExchangeWriterSink only recv ExchangeSerializeMeta.",
            )),
            Some(block_meta) => match ExchangeSerializeMeta::downcast_from(block_meta) {
                None => Err(ErrorCode::Internal(
                    "ExchangeWriterSink only recv ExchangeSerializeMeta.",
                )),
                Some(block_meta) => Ok(block_meta),
            },
        }?;

        match serialize_meta.packet.take() {
            None => Ok(false),
            Some(packet) => match self.exchange.send(packet).await {
                Ok(_) => Ok(false),
                Err(error) if error.code() == ErrorCode::ABORTED_QUERY => Ok(true),
                Err(error) => Err(error),
            },
        }
    }
}

pub fn create_writer_item(
    exchange: FlightExchangeRef,
    query_id: String,
    fragment: usize,
) -> PipeItem {
    let input = InputPort::create();
    PipeItem::create(
        ProcessorPtr::create(ExchangeWriterSink::create(
            input.clone(),
            exchange,
            query_id,
            fragment,
        )),
        vec![input],
        vec![],
    )
}

pub fn create_writer_items(
    exchanges: Vec<FlightExchangeRef>,
    query_id: String,
    fragment: usize,
) -> Vec<PipeItem> {
    let mut items = Vec::with_capacity(exchanges.len());

    for exchange in exchanges {
        items.push(create_writer_item(exchange, query_id.clone(), fragment));
    }

    items
}
