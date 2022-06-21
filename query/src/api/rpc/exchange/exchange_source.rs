// Copyright 2022 Datafuse Labs.
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
