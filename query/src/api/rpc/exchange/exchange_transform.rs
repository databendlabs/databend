use std::any::Any;
use std::sync::Arc;
use jwtk::OneOrMany::Vec;
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
use crate::clusters::ClusterHelper;

struct OutputData {
    pub data_block: Option<DataBlock>,
    pub has_serialized_blocks: bool,
    pub serialized_blocks: Vec<Option<DataPacket>>,
}

struct BothExchangeState {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    input_data: Option<DataBlock>,
    remote_data: Option<DataPacket>,
    output_data: Option<OutputData>,
    flight_exchanges: Vec<FlightExchange>,
    serialize_params: SerializeParams,
    shuffle_exchange_params: ShuffleExchangeParams,
}

struct OnlyReceiveDataState {
    output: Arc<OutputPort>,
    remote_data: Option<DataPacket>,
    output_data: Option<OutputData>,
    flight_exchanges: Vec<FlightExchange>,
    serialize_params: SerializeParams,
    shuffle_exchange_params: ShuffleExchangeParams,
}

pub enum ExchangeTransform {
    Prepare(),
    BothExchange(BothExchangeState),
    OnlyReceiveData()
Finished,
}

impl ExchangeTransform {
    fn try_create() -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(
            Box::new(ExchangeTransform::Prepare())
        ))
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
                        // ctx.clone(),
                        // params.fragment_id,
                        // transform_input_port,
                        // transform_output_port,
                        // params.clone(),
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
        match self {
            ExchangeTransform::Prepare() => unimplemented!(),
            ExchangeTransform::Finished => Ok(Event::Finished),
            ExchangeTransform::BothExchange(_) => self.exchange_event(),
        }
    }

    fn process(&mut self) -> Result<()> {
        if let ExchangeTransform::BothExchange(exchange) = self {
            if exchange.input_data.is_some() {
                // Prepare data to be sent to other nodes
                // return exchange.scatter_data();
            }

            if exchange.remote_data.is_some() {
                // Processing data received from other nodes
                // return exchange.scatter_data();
            }
        }

        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        if let ExchangeTransform::BothExchange(state) = self {
            return state.async_process().await;
        }

        Ok(())
    }
}

impl ExchangeTransform {
    fn exchange_event(&mut self) -> Result<Event> {
        if let ExchangeTransform::BothExchange(state) = self {
            if state.output.is_finished() {
                state.input.finish();
                let mut temp_state = ExchangeTransform::Finished;
                std::mem::swap(self, &mut temp_state);
                return Ok(Event::Finished);
            }

            // This may cause other cluster nodes to idle.
            if !state.output.can_push() {
                state.input.set_not_need_data();
                return Ok(Event::NeedConsume);
            }

            // If data needs to be sent to other nodes.
            if let Some(mut output_data) = state.output_data.take() {
                if let Some(data_block) = output_data.data_block.take() {
                    state.output.push_data(Ok(data_block));
                }

                state.output_data = Some(output_data);
                return Ok(Event::Async);
            }

            // If the data of other nodes can be received.
            for flight_exchange in &state.flight_exchanges {
                if let Some(data_packet) = flight_exchange.try_recv()? {
                    state.remote_data = Some(data_packet);
                    return Ok(Event::Sync);
                }
            }

            if state.input_data.is_some() {
                return Ok(Event::Sync);
            }

            if state.input.is_finished() {
                for flight_exchange in &state.flight_exchanges {
                    // No more data will be sent. close the response of endpoint.
                    flight_exchange.close_response();
                }

                // let mut temp_state = ExchangeTransform::Finished;
                // std::mem::swap(self, &mut temp_state);
                // let mut temp_state = temp_state.to_only_receive();
                // std::mem::swap(self, &mut temp_state);

                state.output.finish();
                return Ok(Event::Async);
            }

            if state.input.has_data() {
                state.input_data = Some(state.input.pull_data().unwrap()?);
                return Ok(Event::Sync);
            }

            state.input.set_need_data();
            return Ok(Event::NeedData);
        }

        Err(ErrorCode::LogicalError("It's a bug"))
    }
}
//
// impl BothExchangeState {
//     pub fn process_remote_data(&mut self) -> Result<()> {
//         if let Some(data_packet) = self.remote_data.take() {
//             match data_packet {
//                 DataPacket::ErrorCode(cause) => Err(cause),
//                 DataPacket::Progress(_) => {}
//                 DataPacket::FragmentData(_) => {}
//                 DataPacket::PrecommitBlock(_) => {}
//                 DataPacket::FinishQuery => {}
//             }
//
//             let schema = &self.shuffle_exchange_params.schema;
//
//             let arrow_schema = Arc::new(schema.to_arrow());
//             let ipc_fields = default_ipc_fields(&arrow_schema.fields);
//             let ipc_schema = IpcSchema {
//                 fields: ipc_fields,
//                 is_little_endian: true,
//             };
//
//             let batch = deserialize_batch(
//                 &data_packet,
//                 &arrow_schema.fields,
//                 &ipc_schema,
//                 &Default::default(),
//             )?;
//
//             self.remote_data_block = Some(DataBlock::from_chunk(&self.schema, &batch)?);
//         }
//
//         Ok(())
//     }
//
//     pub fn scatter_data(&mut self) -> Result<()> {
//         if let Some(data_block) = self.input_data.take() {
//             let scatter = &self.shuffle_exchange_params.shuffle_scatter;
//
//             let scatted_blocks = scatter.execute(&data_block, 0)?;
//             let mut output_data = OutputData {
//                 data_block: None,
//                 serialized_blocks: vec![],
//                 has_serialized_blocks: false,
//             };
//
//             for (index, data_block) in scatted_blocks.into_iter().enumerate() {
//                 if data_block.is_empty() {
//                     output_data.serialized_blocks.push(None);
//                     continue;
//                 }
//
//                 if index == self.serialize_params.local_executor_pos {
//                     output_data.data_block = Some(data_block);
//                     output_data.serialized_blocks.push(None);
//                 } else {
//                     let chunks = data_block.try_into()?;
//                     let options = &self.serialize_params.options;
//                     let ipc_fields = &self.serialize_params.ipc_fields;
//                     let (dicts, values) = serialize_batch(&chunks, ipc_fields, options)?;
//
//                     if !dicts.is_empty() {
//                         return Err(ErrorCode::UnImplement(
//                             "DatabendQuery does not implement dicts.",
//                         ));
//                     }
//
//                     output_data.has_serialized_blocks = true;
//                     let data = FragmentData::Data(0, values);
//                     output_data.serialized_blocks.push(Some(DataPacket::FragmentData(data)));
//                 }
//             }
//
//             self.output_data = Some(output_data);
//         }
//
//         Ok(())
//     }
//
//     pub async fn async_process(&mut self) -> Result<()> {
//         if let Some(mut output_data) = self.output_data.take() {
//             for index in 0..output_data.serialized_blocks.len() {
//                 if let Some(output_packet) = output_data.serialized_blocks[index].take() {
//                     if let Err(_) = self.flight_exchanges[index].send(output_packet).await {
//                         return Err(ErrorCode::TokioError(
//                             "Cannot send flight data to endpoint, because sender is closed.",
//                         ));
//                     }
//                 }
//             }
//         }
//
//         Ok(())
//     }
// }
