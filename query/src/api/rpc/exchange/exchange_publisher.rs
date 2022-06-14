use std::sync::Arc;
use common_arrow::arrow::io::flight::{serialize_batch, serialize_schema};
use common_arrow::arrow::io::ipc::IpcField;
use common_arrow::arrow::io::ipc::write::{default_ipc_fields, WriteOptions};
use common_arrow::arrow_format::flight::data::FlightData;
use common_base::base::tokio::sync::mpsc::Permit;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use crate::pipelines::new::processors::processor::{Event, ProcessorPtr};
use crate::sessions::QueryContext;
use common_exception::{ErrorCode, Result};
use common_planners::Expression;
use crate::api::{DataExchange, HashDataExchange, MergeExchange};
// use crate::api::rpc::exchange::exchange_channel::{Sender, SendError};
use async_channel::{Sender, TrySendError};
use crate::api::rpc::exchange::exchange_params::{ExchangeParams, MergeExchangeParams, SerializeParams, ShuffleExchangeParams};
use crate::api::rpc::exchange::exchange_sink_merge::ExchangeMergeSink;
use crate::api::rpc::exchange::exchange_sink_shuffle::ExchangePublisherSink;
use crate::api::rpc::flight_scatter::FlightScatter;
use crate::api::rpc::flight_scatter_hash::HashFlightScatter;
use crate::pipelines::new::{NewPipe, NewPipeline, SinkPipeBuilder};
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

    pub fn publisher_sink(ctx: &Arc<QueryContext>, params: &ExchangeParams, pipeline: &mut NewPipeline) -> Result<()> {
        match params {
            ExchangeParams::MergeExchange(params) => {
                let mut sink_builder = SinkPipeBuilder::create();

                for _index in 0..pipeline.output_len() {
                    let input = InputPort::create();
                    sink_builder.add_sink(input.clone(), ExchangeMergeSink::try_create(
                        ctx.clone(),
                        params.fragment_id,
                        input.clone(),
                        params.clone(),
                    )?);
                }

                pipeline.add_pipe(sink_builder.finalize());
                Ok(())
            }
            ExchangeParams::ShuffleExchange(params) => {
                pipeline.add_transform(|transform_input_port, transform_output_port| {
                    ExchangePublisherSink::<false>::try_create(
                        ctx.clone(),
                        params.fragment_id,
                        transform_input_port,
                        transform_output_port,
                        params.clone(),
                    )
                })
            }
        }
    }

    pub fn via_exchange(ctx: &Arc<QueryContext>, params: &ExchangeParams, pipeline: &mut NewPipeline) -> Result<()> {
        match params {
            ExchangeParams::MergeExchange(params) => Self::via_merge_exchange(ctx, params),
            ExchangeParams::ShuffleExchange(params) => {
                pipeline.add_transform(|transform_input_port, transform_output_port| {
                    ExchangePublisherSink::<true>::try_create(
                        ctx.clone(),
                        params.fragment_id,
                        transform_input_port,
                        transform_output_port,
                        params.clone(),
                    )
                })
            }
        }
    }
}
