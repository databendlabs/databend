use std::any::Any;
use std::sync::Arc;
use common_arrow::arrow::io::flight::{default_ipc_fields, deserialize_batch, serialize_batch};
use common_arrow::arrow::io::ipc::IpcSchema;
use common_catalog::table_context::TableContext;
use common_datablocks::DataBlock;
use common_pipeline::processors::port::{InputPort, OutputPort};
use common_pipeline::processors::Processor;
use common_pipeline::processors::processor::{Event, ProcessorPtr};
use crate::api::{DataPacket, FragmentData};
use crate::api::rpc::exchange::exchange_params::{ExchangeParams, SerializeParams, ShuffleExchangeParams};
use crate::sessions::QueryContext;
use common_exception::{ErrorCode, Result};
use common_pipeline::Pipeline;
use crate::api::rpc::flight_client::FlightExchange;
use crate::api::rpc::packets::{PrecommitBlock, ProgressInfo};
use crate::clusters::ClusterHelper;

struct OutputData {
    pub data_block: Option<DataBlock>,
    pub has_serialized_blocks: bool,
    pub serialized_blocks: Vec<Option<DataPacket>>,
}

pub struct ExchangeTransform {
    finished: bool,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    input_data: Option<DataBlock>,
    remote_data: Option<DataPacket>,
    output_data: Option<OutputData>,
    flight_exchanges: Vec<FlightExchange>,
    serialize_params: SerializeParams,
    shuffle_exchange_params: ShuffleExchangeParams,
}

impl ExchangeTransform {
    fn try_create(ctx: Arc<QueryContext>, params: &ShuffleExchangeParams, input: Arc<InputPort>, output: Arc<OutputPort>) -> Result<ProcessorPtr> {
        let exchange_params = ExchangeParams::ShuffleExchange(params.clone());
        let exchange_manager = ctx.get_exchange_manager();
        let flight_exchanges = exchange_manager.get_flight_exchanges(&exchange_params)?;

        Ok(ProcessorPtr::create(Box::new(
            ExchangeTransform {
                input,
                output,
                flight_exchanges,
                finished: false,
                input_data: None,
                remote_data: None,
                output_data: None,
                shuffle_exchange_params: params.clone(),
                serialize_params: params.create_serialize_params()?,
            }
        )))
    }

    pub fn via(ctx: &Arc<QueryContext>, params: &ExchangeParams, pipeline: &mut Pipeline) -> Result<()> {
        match params {
            ExchangeParams::MergeExchange(params) => match params.destination_id == ctx.get_cluster().local_id() {
                true => Ok(()), // do nothing
                false => Err(ErrorCode::LogicalError(format!(
                    "Locally depends on merge exchange, but the localhost is not a coordination node. executor: {}, destination_id: {}, fragment id: {}",
                    ctx.get_cluster().local_id(),
                    params.destination_id,
                    params.fragment_id
                ))),
            },
            ExchangeParams::ShuffleExchange(params) => {
                pipeline.add_transform(|transform_input_port, transform_output_port| {
                    ExchangeTransform::try_create(
                        ctx.clone(),
                        params,
                        transform_input_port,
                        transform_output_port,
                    )
                })
            }
        }
    }
}

#[async_trait::async_trait]
impl Processor for ExchangeTransform {
    fn name(&self) -> &'static str {
        "ExchangeTransform"
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        // This may cause other cluster nodes to idle.
        if !self.output.can_push() {
            // TODO: try send data if other nodes can recv data.
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        // If data needs to be sent to other nodes.
        if let Some(mut output_data) = self.output_data.take() {
            if let Some(data_block) = output_data.data_block.take() {
                self.output.push_data(Ok(data_block));
            }

            self.output_data = Some(output_data);
            return Ok(Event::Async);
        }

        // If the data of other nodes can be received.
        for flight_exchange in &self.flight_exchanges {
            if let Some(data_packet) = flight_exchange.try_recv()? {
                self.remote_data = Some(data_packet);
                return Ok(Event::Sync);
            }
        }

        if self.input_data.is_some() {
            return Ok(Event::Sync);
        }

        if self.input.is_finished() {
            if self.finished {
                self.output.finish();
                return Ok(Event::Finished);
            }

            for flight_exchange in &self.flight_exchanges {
                // No more data will be sent. close the response of endpoint.
                flight_exchange.close_response();
            }

            return Ok(Event::Async);
        }

        if self.input.has_data() {
            self.input_data = Some(self.input.pull_data().unwrap()?);
            return Ok(Event::Sync);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        // Prepare data to be sent to other nodes
        if let Some(data_block) = self.input_data.take() {
            let scatter = &self.shuffle_exchange_params.shuffle_scatter;

            let scatted_blocks = scatter.execute(&data_block, 0)?;

            let mut output_data = OutputData {
                data_block: None,
                serialized_blocks: vec![],
                has_serialized_blocks: false,
            };

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
                    let (dicts, values) = serialize_batch(&chunks, ipc_fields, options)?;

                    if !dicts.is_empty() {
                        return Err(ErrorCode::UnImplement(
                            "DatabendQuery does not implement dicts.",
                        ));
                    }

                    output_data.has_serialized_blocks = true;
                    let data = FragmentData::create(values);
                    output_data.serialized_blocks.push(Some(DataPacket::FragmentData(data)));
                }
            }

            self.output_data = Some(output_data);
        }

        // Processing data received from other nodes
        if let Some(remote_data) = self.remote_data.take() {
            return match remote_data {
                DataPacket::ErrorCode(v) => self.on_recv_error(v),
                DataPacket::Progress(v) => self.on_recv_progress(v),
                DataPacket::FragmentData(v) => self.on_recv_data(v),
                DataPacket::PrecommitBlock(v) => self.on_recv_precommit(v),
                DataPacket::FinishQuery => self.on_finish(),
            };
        }

        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        if let Some(mut output_data) = self.output_data.take() {
            for index in 0..output_data.serialized_blocks.len() {
                if let Some(output_packet) = output_data.serialized_blocks[index].take() {
                    if let Err(_) = self.flight_exchanges[index].send(output_packet).await {
                        return Err(ErrorCode::TokioError(
                            "Cannot send flight data to endpoint, because sender is closed.",
                        ));
                    }
                }
            }
        }

        if !self.finished && self.input.is_finished() {
            // async recv if input is finished.
            // TODO: use future::future::select_all to parallel await
            for flight_exchange in &self.flight_exchanges {
                if let Some(data_packet) = flight_exchange.recv().await? {
                    self.remote_data = Some(data_packet);
                    return Ok(());
                }
            }

            self.finished = true;
        }

        Ok(())
    }
}

impl ExchangeTransform {
    fn on_recv_error(&mut self, cause: ErrorCode) -> Result<()> {
        Err(cause)
    }

    fn on_recv_data(&mut self, fragment_data: FragmentData) -> Result<()> {
        // do nothing if has output data.
        if self.output_data.is_some() {
            return Ok(());
        }

        let schema = &self.shuffle_exchange_params.schema;

        let arrow_schema = Arc::new(schema.to_arrow());
        let ipc_fields = default_ipc_fields(&arrow_schema.fields);
        let ipc_schema = IpcSchema {
            fields: ipc_fields,
            is_little_endian: true,
        };

        let batch = deserialize_batch(
            &fragment_data.data,
            &arrow_schema.fields,
            &ipc_schema,
            &Default::default(),
        )?;

        self.output_data = Some(OutputData {
            serialized_blocks: vec![],
            has_serialized_blocks: false,
            data_block: Some(DataBlock::from_chunk(schema, &batch)?),
        });

        Ok(())
    }

    fn on_recv_progress(&mut self, progress: ProgressInfo) -> Result<()> {
        unimplemented!()
    }

    fn on_recv_precommit(&mut self, fragment_data: PrecommitBlock) -> Result<()> {
        unimplemented!()
    }

    fn on_finish(&mut self) -> Result<()> {
        unimplemented!()
    }
}
