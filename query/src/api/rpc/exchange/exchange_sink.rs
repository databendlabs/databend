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

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;

use crate::api::rpc::exchange::exchange_params::ExchangeParams;
use crate::api::rpc::exchange::exchange_params::MergeExchangeParams;
use crate::api::rpc::exchange::exchange_sink_merge::ExchangeMergeSink;
use crate::api::rpc::exchange::exchange_sink_shuffle::ExchangePublisherSink;
use crate::clusters::ClusterHelper;
use crate::pipelines::processors::port::InputPort;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::processor::ProcessorPtr;
use crate::pipelines::Pipeline;
use crate::pipelines::SinkPipeBuilder;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct ExchangeSink;

impl ExchangeSink {
    pub fn via(ctx: &Arc<QueryContext>, params: &ExchangeParams, pipeline: &mut Pipeline) -> Result<()> {
        match params {
            ExchangeParams::MergeExchange(params) => {
                if params.destination_id == ctx.get_cluster().local_id() {
                    return Err(ErrorCode::LogicalError(format!(
                        "Locally depends on merge exchange, but the localhost is not a coordination node. executor: {}, destination_id: {}, fragment id: {}",
                        ctx.get_cluster().local_id(),
                        params.destination_id,
                        params.fragment_id
                    )));
                }

                let mut sink_builder = SinkPipeBuilder::create();

                for _index in 0..pipeline.output_len() {
                    let input = InputPort::create();
                    sink_builder.add_sink(
                        input.clone(),
                        ExchangeMergeSink::try_create(ctx.clone(), input.clone(), params)?,
                    );
                }

                pipeline.add_pipe(sink_builder.finalize());
                Ok(())
            }
            ExchangeParams::ShuffleExchange(params) => {
                let mut sink_builder = SinkPipeBuilder::create();

                for _index in 0..pipeline.output_len() {
                    let input = InputPort::create();
                    sink_builder.add_sink(
                        input.clone(),
                        ExchangePublisherSink::try_create(
                            ctx.clone(),
                            input,
                            params,
                        )?,
                    );
                }

                pipeline.add_pipe(sink_builder.finalize());
                Ok(())
            }
        }
    }
}
