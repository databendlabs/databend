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
use crate::pipelines::new::processors::port::InputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::NewPipeline;
use crate::pipelines::new::SinkPipeBuilder;
use crate::sessions::QueryContext;

pub struct ExchangeSink;

impl ExchangeSink {
    fn via_merge_exchange(ctx: &Arc<QueryContext>, params: &MergeExchangeParams) -> Result<()> {
        match params.destination_id == ctx.get_cluster().local_id() {
            true => Ok(()), /* do nothing */
            false => Err(ErrorCode::LogicalError(
                format!(
                    "Locally depends on merge exchange, but the localhost is not a coordination node. executor: {}, destination_id: {}, fragment id: {}",
                    ctx.get_cluster().local_id(),
                    params.destination_id,
                    params.fragment_id
                ),
            )),
        }
    }

    pub fn init(processor: &mut ProcessorPtr) -> Result<()> {
        ExchangeMergeSink::init(processor)?;
        ExchangePublisherSink::<true>::init(processor)?;
        ExchangePublisherSink::<false>::init(processor)
    }

    pub fn publisher_sink(
        ctx: &Arc<QueryContext>,
        params: &ExchangeParams,
        pipeline: &mut NewPipeline,
    ) -> Result<()> {
        match params {
            ExchangeParams::MergeExchange(params) => {
                let mut sink_builder = SinkPipeBuilder::create();

                for _index in 0..pipeline.output_len() {
                    let input = InputPort::create();
                    sink_builder.add_sink(
                        input.clone(),
                        ExchangeMergeSink::try_create(
                            ctx.clone(),
                            params.fragment_id,
                            input.clone(),
                            params.clone(),
                        )?,
                    );
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

    pub fn via_exchange(
        ctx: &Arc<QueryContext>,
        params: &ExchangeParams,
        pipeline: &mut NewPipeline,
    ) -> Result<()> {
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
