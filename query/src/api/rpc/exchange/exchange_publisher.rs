use std::sync::Arc;
use common_arrow::arrow::io::flight::{serialize_batch, serialize_schema};
use common_arrow::arrow::io::ipc::IpcField;
use common_arrow::arrow::io::ipc::write::{default_ipc_fields, WriteOptions};
use common_arrow::arrow_format::flight::data::FlightData;
use common_base::base::tokio::sync::mpsc::{Permit};
use common_base::base::tokio::sync::mpsc::error::TrySendError;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use crate::pipelines::new::processors::processor::{Event, ProcessorPtr};
use crate::sessions::QueryContext;
use common_exception::{ErrorCode, Result};
use common_planners::Expression;
use crate::api::{DataExchange, HashDataExchange, MergeExchange};
use crate::api::rpc::exchange::exchange_channel::{Sender, SendError};
use crate::api::rpc::exchange::exchange_params::{HashExchangeParams, MergeExchangeParams, ExchangeParams, SerializeParams};
use crate::api::rpc::flight_scatter::FlightScatter;
use crate::api::rpc::flight_scatter_hash::HashFlightScatter;
use crate::pipelines::new::NewPipeline;
use crate::pipelines::new::processors::port::{InputPort, OutputPort};
use crate::pipelines::new::processors::Processor;

pub struct ExchangePublisher {
    ctx: Arc<QueryContext>,
    local_pos: usize,

    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    publish_sender: Vec<Sender<DataBlock>>,
}

impl ExchangePublisher {
    fn via_merge_exchange(ctx: &Arc<QueryContext>, params: &MergeExchangeParams) -> Result<()> {
        match params.destination_id == ctx.get_cluster().local_id() {
            true => Ok(()), /* do nothing */
            false => Err(ErrorCode::LogicalError("Locally depends on merge exchange, but the localhost is not a coordination node.")),
        }
    }

    pub fn via_exchange(ctx: &Arc<QueryContext>, params: &ExchangeParams, pipeline: &mut NewPipeline) -> Result<()> {
        match params {
            ExchangeParams::MergeExchange(params) => Self::via_merge_exchange(ctx, params),
            ExchangeParams::HashExchange(params) => {
                pipeline.add_transform(|transform_input_port, transform_output_port| {
                    ViaHashExchangePublisher::try_create(
                        ctx.clone(),
                        transform_input_port,
                        transform_output_port,
                        params.clone(),
                    )
                })
            }
        }
    }
}

struct OutputData {
    pub data_block: Option<DataBlock>,
    pub serialized_blocks: Vec<Option<FlightData>>,
}

struct ViaHashExchangePublisher {
    ctx: Arc<QueryContext>,

    exchange_params: HashExchangeParams,
    serialize_params: SerializeParams,

    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    input_data: Option<DataBlock>,
    output_data: Option<OutputData>,
    peer_endpoint_publisher: Vec<Sender<FlightData>>,
}

impl ViaHashExchangePublisher {
    pub fn try_create(ctx: Arc<QueryContext>, input: Arc<InputPort>, output: Arc<OutputPort>, exchange_params: HashExchangeParams) -> Result<ProcessorPtr> {
        let serialize_params = exchange_params.create_serialize_params()?;
        Ok(ProcessorPtr::create(Box::new(ViaHashExchangePublisher {
            ctx,
            input,
            output,
            exchange_params,
            serialize_params,
            input_data: None,
            output_data: None,
            peer_endpoint_publisher: vec![],
        })))
    }

    fn get_peer_endpoint_publisher(&self) -> Result<Vec<Sender<FlightData>>> {
        let destination_ids = &self.exchange_params.destination_ids;
        let mut res = Vec::with_capacity(destination_ids.len());
        let exchange_manager = self.ctx.get_exchange_manager();

        for destination_id in destination_ids {
            let query_id = &self.exchange_params.query_id;
            res.push(exchange_manager.get_fragment_sink(query_id, destination_id)?);
        }

        Ok(res)
    }
}

#[async_trait::async_trait]
impl Processor for ViaHashExchangePublisher {
    fn name(&self) -> &'static str {
        "HashExchangePublisher"
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        // This may cause other cluster nodes to idle.
        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(mut output_data) = self.output_data.take() {
            if self.peer_endpoint_publisher.is_empty() {
                self.peer_endpoint_publisher = self.get_peer_endpoint_publisher()?;
            }

            // If has local data block, the push block to the output port
            let mut pushed_data = false;
            if let Some(data_block) = output_data.data_block.take() {
                pushed_data = true;
                self.output.push_data(Ok(data_block));
            }

            // When the sender is fast enough, we can try to send. If all of them are sent successfully, it will reduce the scheduling of the processor once.
            let mut need_async_send = false;
            for (index, publisher) in self.peer_endpoint_publisher.iter().enumerate() {
                if output_data.serialized_blocks[index].is_some() {
                    let data = output_data.serialized_blocks[index].take().unwrap();
                    match publisher.try_send(data) {
                        Ok(_) => { /* do nothing*/ }
                        Err(SendError::Finished) => { return Ok(Event::Finished); }
                        Err(SendError::QueueIsFull(value)) => {
                            need_async_send = true;
                            output_data.serialized_blocks[index] = Some(value);
                        }
                    }
                }
            }

            if need_async_send {
                self.output_data = Some(output_data);
                return Ok(Event::Async);
            }

            if pushed_data {
                return Ok(Event::NeedConsume);
            }
        }

        if self.input_data.is_some() {
            return Ok(Event::Sync);
        }

        if self.input.is_finished() {
            self.output.finish();
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
            let hash_scatter = &self.exchange_params.hash_scatter;
            let scatted_blocks = hash_scatter.execute(&data_block)?;
            let mut output_data = OutputData { data_block: None, serialized_blocks: vec![] };

            for (index, data_block) in scatted_blocks.into_iter().enumerate() {
                if data_block.is_empty() {
                    output_data.serialized_blocks.push(None);
                    continue;
                }

                if index == self.serialize_params.local_executor_pos {
                    output_data.data_block = Some(data_block);
                    output_data.serialized_blocks.push(None);
                } else {
                    let chunks = data_block.try_into()?;
                    let options = &self.serialize_params.options;
                    let ipc_fields = &self.serialize_params.ipc_fields;
                    let (dicts, values) = serialize_batch(&chunks, ipc_fields, options);

                    if !dicts.is_empty() {
                        return Err(ErrorCode::UnImplement("DatabendQuery does not implement dicts."));
                    }

                    output_data.serialized_blocks.push(Some(values));
                }
            }

            self.output_data = Some(output_data);
        }

        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        if let Some(mut output_data) = self.output_data.take() {
            for (index, publisher) in self.peer_endpoint_publisher.iter().enumerate() {
                if let Some(flight_data) = output_data.serialized_blocks[index].take() {
                    if let Err(_) = publisher.send(flight_data).await {
                        return Err(ErrorCode::TokioError(
                            "Cannot send flight data to endpoint, because sender is closed."
                        ));
                    }
                }
            }
        }

        Ok(())
    }
}
