use async_channel::Receiver;
use common_arrow::arrow_format::flight::data::FlightData;
use common_datavalues::DataSchemaRef;
use common_exception::Result;

use crate::api::rpc::exchange::exchange_params::ExchangeParams;
use crate::api::rpc::exchange::exchange_source_merge::ExchangeMergeSource;
use crate::api::rpc::exchange::exchange_source_shuffle::ExchangeShuffleSource;
use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::NewPipe;
use crate::pipelines::new::NewPipeline;

pub struct ExchangeSource {}

impl ExchangeSource {
    pub fn via_exchange(
        rx: Receiver<Result<FlightData>>,
        params: &ExchangeParams,
        pipeline: &mut NewPipeline,
    ) -> Result<()> {
        pipeline.add_transform(|transform_input_port, transform_output_port| {
            ExchangeShuffleSource::try_create(
                transform_input_port,
                transform_output_port,
                rx.clone(),
                params.get_schema(),
            )
        })
    }

    pub fn create_source(
        rx: Receiver<Result<FlightData>>,
        schema: DataSchemaRef,
        pipeline: &mut NewPipeline,
    ) -> Result<()> {
        let output = OutputPort::create();
        pipeline.add_pipe(NewPipe::SimplePipe {
            inputs_port: vec![],
            outputs_port: vec![output.clone()],
            processors: vec![ExchangeMergeSource::try_create(output, rx, schema)?],
        });

        Ok(())
    }
}
