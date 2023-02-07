use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_pipeline_core::pipe::PipeItem;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_pipeline_sinks::AsyncSink;
use common_pipeline_sinks::AsyncSinker;

use crate::api::rpc::exchange::serde::exchange_serializer::ExchangeSerializeMeta;
use crate::api::rpc::flight_client::FlightExchange;

pub struct ExchangeWriterSink {
    exchange: FlightExchange,
}

impl ExchangeWriterSink {
    pub fn create(input: Arc<InputPort>, flight_exchange: FlightExchange) -> ProcessorPtr {
        AsyncSinker::create(input, ExchangeWriterSink {
            exchange: flight_exchange,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSink for ExchangeWriterSink {
    const NAME: &'static str = "ExchangeWriterSink";

    async fn on_finish(&mut self) -> Result<()> {
        self.exchange.close_output();
        Ok(())
    }

    #[async_trait::unboxed_simple]
    async fn consume(&mut self, mut data_block: DataBlock) -> Result<()> {
        let packet = match data_block.take_meta() {
            None => Err(ErrorCode::Internal(
                "ExchangeWriterSink only recv ExchangeSerializeMeta.",
            )),
            Some(mut block_meta) => match block_meta
                .as_mut_any()
                .downcast_mut::<ExchangeSerializeMeta>()
            {
                None => Err(ErrorCode::Internal(
                    "ExchangeWriterSink only recv ExchangeSerializeMeta.",
                )),
                Some(block_meta) => Ok(block_meta.packet.take().unwrap()),
            },
        }?;

        self.exchange.send(packet).await?;
        Ok(())
    }
}

pub fn create_writer_item(exchange: FlightExchange) -> PipeItem {
    let input = InputPort::create();
    PipeItem::create(
        ExchangeWriterSink::create(input.clone(), exchange),
        vec![input],
        vec![],
    )
}
