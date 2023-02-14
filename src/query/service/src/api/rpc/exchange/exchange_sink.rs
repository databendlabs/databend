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
use common_pipeline_core::pipe::Pipe;
use common_pipeline_core::processors::processor::ProcessorPtr;

use crate::api::rpc::exchange::exchange_params::ExchangeParams;
use crate::api::rpc::exchange::exchange_sink_writer::create_writer_items;
use crate::api::rpc::exchange::exchange_sink_writer::ExchangeWriterSink;
use crate::api::rpc::exchange::exchange_transform_shuffle::exchange_shuffle;
use crate::api::rpc::exchange::serde::exchange_serializer::create_serializer_items;
use crate::api::rpc::exchange::serde::exchange_serializer::TransformExchangeSerializer;
use crate::clusters::ClusterHelper;
use crate::pipelines::Pipeline;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct ExchangeSink;

impl ExchangeSink {
    pub fn via(
        ctx: &Arc<QueryContext>,
        params: &ExchangeParams,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let exchange_manager = ctx.get_exchange_manager();
        let flight_exchange = exchange_manager.get_flight_exchanges(params)?;

        match params {
            ExchangeParams::MergeExchange(params) => {
                if params.destination_id == ctx.get_cluster().local_id() {
                    return Err(ErrorCode::Internal(format!(
                        "Locally depends on merge exchange, but the localhost is not a coordination node. executor: {}, destination_id: {}, fragment id: {}",
                        ctx.get_cluster().local_id(),
                        params.destination_id,
                        params.fragment_id
                    )));
                }

                pipeline.add_transform(|input, output| {
                    Ok(TransformExchangeSerializer::create(
                        input,
                        output,
                        &params.schema,
                    ))
                })?;

                assert_eq!(flight_exchange.len(), 1);
                pipeline.add_sink(|input| {
                    Ok(ProcessorPtr::create(ExchangeWriterSink::create(
                        input,
                        flight_exchange[0].clone(),
                    )))
                })
            }
            ExchangeParams::ShuffleExchange(params) => {
                exchange_shuffle(params, pipeline)?;

                // exchange serialize transform
                let len = flight_exchange.len();
                let items = create_serializer_items(len, &params.schema);
                pipeline.add_pipe(Pipe::create(len, len, items));

                // exchange writer sink
                let items = create_writer_items(flight_exchange);
                pipeline.add_pipe(Pipe::create(len, 0, items));
                Ok(())
            }
        }
    }
}
