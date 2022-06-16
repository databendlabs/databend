use std::any::Any;
use common_exception::ErrorCode;
use std::sync::Arc;
use async_channel::{Sender, TrySendError};
use common_arrow::arrow::io::flight::serialize_batch;
use common_arrow::arrow_format::flight::data::FlightData;
use common_datablocks::DataBlock;
use crate::api::rpc::exchange::exchange_params::{MergeExchangeParams, SerializeParams};
use crate::pipelines::new::processors::port::{InputPort, OutputPort};
use crate::pipelines::new::processors::Processor;
use crate::pipelines::new::processors::processor::{Event, ProcessorPtr};
use crate::sessions::QueryContext;

use common_exception::Result;
use crate::api::rpc::exchange::exchange_channel::{FragmentReceiver, FragmentSender};
use crate::api::rpc::packet::DataPacket;

pub struct ExchangeMergeSink {
    ctx: Arc<QueryContext>,
    fragment_id: usize,

    input: Arc<InputPort>,
    input_data: Option<DataBlock>,
    output_data: Option<DataPacket>,
    serialize_params: SerializeParams,
    exchange_params: MergeExchangeParams,
    peer_endpoint_publisher: Option<FragmentSender>,
}

impl ExchangeMergeSink {
    pub fn try_create(ctx: Arc<QueryContext>, fragment_id: usize, input: Arc<InputPort>, exchange_params: MergeExchangeParams) -> Result<ProcessorPtr> {
        let serialize_params = exchange_params.create_serialize_params()?;
        Ok(ProcessorPtr::create(Box::new(ExchangeMergeSink {
            ctx,
            input,
            fragment_id,
            exchange_params,
            serialize_params,
            input_data: None,
            output_data: None,
            peer_endpoint_publisher: None,
        })))
    }

    pub fn init(processor: &mut ProcessorPtr) -> Result<()> {
        unsafe {
            if let Some(exchange_merge) = processor.as_any().downcast_mut::<Self>() {
                let id = exchange_merge.fragment_id;
                let query_id = &exchange_merge.exchange_params.query_id;
                let destination_id = &exchange_merge.exchange_params.destination_id;
                let exchange_manager = exchange_merge.ctx.get_exchange_manager();
                exchange_merge.peer_endpoint_publisher = Some(exchange_manager.get_fragment_sink(query_id, id, destination_id)?);
            }

            Ok(())
        }
    }

    fn get_endpoint_publisher(&self) -> Result<&FragmentSender> {
        match &self.peer_endpoint_publisher {
            Some(tx) => Ok(tx),
            None => Err(ErrorCode::LogicalError("Cannot get endpoint_publisher.")),
        }
    }
}

#[async_trait::async_trait]
impl Processor for ExchangeMergeSink {
    fn name(&self) -> &'static str {
        "ExchangeSink"
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if let Some(output) = self.output_data.take() {
            let tx = self.get_endpoint_publisher()?;

            if let Err(try_send_err) = tx.try_send(output) {
                return match try_send_err {
                    TrySendError::Closed(_) => Ok(Event::Finished),
                    TrySendError::Full(value) => {
                        self.output_data = Some(value);
                        Ok(Event::Async)
                    }
                };
            }
        }

        if self.input_data.is_some() {
            return Ok(Event::Sync);
        }

        if self.input.is_finished() {
            if let Some(publisher) = self.peer_endpoint_publisher.take() {
                drop(publisher);
            }

            return Ok(Event::Finished);
        }

        if self.input.has_data() {
            self.input_data = Some(self.input.pull_data().unwrap()?);
            return Ok(Event::Sync);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(data_block) = self.input_data.take() {
            if data_block.is_empty() {
                return Ok(());
            }

            let chunks = data_block.try_into()?;
            let options = &self.serialize_params.options;
            let ipc_fields = &self.serialize_params.ipc_fields;
            let (dicts, values) = serialize_batch(&chunks, ipc_fields, options);

            if !dicts.is_empty() {
                return Err(ErrorCode::UnImplement("DatabendQuery does not implement dicts."));
            }

            // FlightData
            self.output_data = Some(DataPacket::Data(self.fragment_id, values));
        }

        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        if let Some(mut output_data) = self.output_data.take() {
            if let Some(sender) = &self.peer_endpoint_publisher {
                if let Err(_) = sender.send(output_data).await {
                    return Err(ErrorCode::TokioError(
                        "Cannot send flight data to endpoint, because sender is closed."
                    ));
                }
            }
        }

        Ok(())
    }
}
